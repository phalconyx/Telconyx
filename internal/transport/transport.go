// Package transport implements the raw HTTP transport for the Telegram Bot API.
// It is intentionally minimal and depends only on the standard library.
package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultAPIBase is the hosted Telegram Bot API endpoint.
const DefaultAPIBase = "https://api.telegram.org"

// Response is a buffered HTTP response.
type Response struct {
	StatusCode int
	Body       []byte
	Header     http.Header
}

// StreamResponse is a streaming HTTP response (downloads).
// The caller MUST close Body when done.
type StreamResponse struct {
	StatusCode int
	Header     http.Header
	Body       io.ReadCloser
}

// Client is a thin wrapper around net/http for the Telegram Bot API.
type Client struct {
	http    *http.Client
	base    string
	apiBase string
	token   string
}

// New returns a new transport client. base is the API server root
// (DefaultAPIBase when empty; a self-hosted telegram-bot-api server otherwise).
//
// timeout bounds the connection phases (dial, TLS) and the wait for response
// headers — it deliberately does NOT bound body transfer, so large uploads
// and downloads on slow links are never cut off mid-stream. Callers bound
// whole operations with a context deadline when they need one.
func New(token string, timeout time.Duration, base string) *Client {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	base = strings.TrimRight(base, "/")
	if base == "" {
		base = DefaultAPIBase
	}
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second,
		// Counted from the end of the request write, so an upload that takes
		// minutes to send is not affected — this only bounds how long the API
		// may think before answering.
		ResponseHeaderTimeout: timeout,
		ExpectContinueTimeout: 1 * time.Second,
		// Keep at least as many idle conns as concurrent chunk transfers so
		// parallel downloads don't churn TLS handshakes.
		MaxIdleConnsPerHost: 8,
		IdleConnTimeout:     90 * time.Second,
	}
	return &Client{
		http:    &http.Client{Transport: tr},
		base:    base,
		apiBase: base + "/bot" + token,
		token:   token,
	}
}

// FileURL builds the temporary download URL for a Telegram file path.
func (c *Client) FileURL(filePath string) string {
	return c.base + "/file/bot" + c.token + "/" + filePath
}

// Close releases any idle HTTP connections.
func (c *Client) Close() {
	c.http.CloseIdleConnections()
}

// redactErr strips the bot token from an error message so it never leaks into
// API responses or logs. The token is part of every request URL, and net/http
// embeds that URL verbatim in transport errors (e.g. timeouts:
// `Post "https://api.telegram.org/bot<TOKEN>/sendDocument": context deadline exceeded`).
func (c *Client) redactErr(err error) error {
	if err == nil || c.token == "" {
		return err
	}
	if msg := err.Error(); strings.Contains(msg, c.token) {
		return errors.New(strings.ReplaceAll(msg, c.token, "<redacted>"))
	}
	return err
}

// PostForm sends an application/x-www-form-urlencoded POST to a Bot API method.
func (c *Client) PostForm(ctx context.Context, method string, params url.Values) (*Response, error) {
	u := c.apiBase + "/" + method
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(params.Encode()))
	if err != nil {
		return nil, c.redactErr(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return c.do(req)
}

// PostMultipart sends a multipart/form-data POST with a single file field.
// The file is streamed through an io.Pipe; the caller is responsible for
// providing a reader that can be consumed (or re-created for retries).
func (c *Client) PostMultipart(
	ctx context.Context,
	method string,
	fields map[string]string,
	fileField, fileName string,
	file io.Reader,
) (*Response, error) {
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)

	go func() {
		// Always close pw; this signals EOF to the HTTP client.
		defer pw.Close()
		if err := writeMultipart(mw, fields, fileField, fileName, file); err != nil {
			_ = pw.CloseWithError(err)
		}
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.apiBase+"/"+method, pr)
	if err != nil {
		return nil, c.redactErr(err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return c.do(req)
}

func writeMultipart(
	mw *multipart.Writer,
	fields map[string]string,
	fileField, fileName string,
	file io.Reader,
) error {
	for k, v := range fields {
		if err := mw.WriteField(k, v); err != nil {
			return err
		}
	}
	fw, err := mw.CreateFormFile(fileField, fileName)
	if err != nil {
		return err
	}
	if _, err := io.Copy(fw, file); err != nil {
		return err
	}
	return mw.Close()
}

// GetStream performs a streaming GET (for downloading file content).
// The caller MUST close the returned Body.
func (c *Client) GetStream(ctx context.Context, fullURL string) (*StreamResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, c.redactErr(err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, c.redactErr(err)
	}
	return &StreamResponse{
		StatusCode: resp.StatusCode,
		Header:     resp.Header,
		Body:       resp.Body,
	}, nil
}

func (c *Client) do(req *http.Request) (*Response, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, c.redactErr(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, c.redactErr(err)
	}
	return &Response{
		StatusCode: resp.StatusCode,
		Body:       body,
		Header:     resp.Header,
	}, nil
}

// ErrorBodyf is a small helper for producing human-readable error messages
// from buffered responses.
func ErrorBodyf(prefix string, status int, body []byte) error {
	return fmt.Errorf("%s: status=%d body=%s", prefix, status, truncate(string(body), 512))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "...(truncated)"
}
