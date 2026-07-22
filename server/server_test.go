package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/phalconyx/telconyx"
)

func newTestHandler(t *testing.T, apiKey, chatID string) http.Handler {
	t.Helper()
	c, err := telconyx.NewClient(telconyx.Config{Token: "test-token", ChatID: chatID})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return New(c, Config{APIKey: apiKey})
}

func newTestPoolHandler(t *testing.T) http.Handler {
	t.Helper()
	p, err := telconyx.NewPool(telconyx.PoolConfig{Routes: []telconyx.Route{
		{Alias: "b1", Token: "t1", ChatID: "-100"},
		{Alias: "b2", Token: "t2", ChatID: "-200"},
	}})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	return New(p, Config{})
}

func do(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestDelete_RequiresAPIKey(t *testing.T) {
	h := newTestHandler(t, "secret", "-100")
	// No X-API-Key header -> 401, regardless of body.
	if w := do(h, "POST", "/delete", `{"url":"telconyx://file/x"}`); w.Code != http.StatusUnauthorized {
		t.Errorf("missing api key: got %d, want 401 (body=%s)", w.Code, w.Body.String())
	}
}

func TestDelete_BadRequests(t *testing.T) {
	h := newTestHandler(t, "", "-100") // auth disabled
	cases := []struct {
		name, body string
		want       int
	}{
		{"invalid json", `{not json`, http.StatusBadRequest},
		{"missing url", `{}`, http.StatusBadRequest},
		{"blank url", `{"url":"   "}`, http.StatusBadRequest},
		{"not a telconyx url", `{"url":"https://example.com/x"}`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if w := do(h, "POST", "/delete", tc.body); w.Code != tc.want {
				t.Errorf("got %d, want %d (body=%s)", w.Code, tc.want, w.Body.String())
			}
		})
	}
}

func TestDelete_NonNumericChatID(t *testing.T) {
	// A valid link, but the server is configured with a non-numeric chat id.
	// DeleteChunks fails before any Telegram call, so this is hermetic.
	h := newTestHandler(t, "", "@mygroup")
	link := (&telconyx.FileLink{FileID: "fid", MessageID: 5, ChatID: -100}).URL()
	w := do(h, "POST", "/delete", `{"url":"`+link+`"}`)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("got %d, want 502 (body=%s)", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "numeric") {
		t.Errorf("expected numeric-ChatID error, got %s", w.Body.String())
	}
}

// TestUnknownRoute verifies a link whose route alias is not configured on
// this server gets a 400 with the dedicated unknown_route code — on both
// endpoints that resolve links, and before any Telegram call is made.
func TestUnknownRoute(t *testing.T) {
	h := newTestPoolHandler(t)
	link := (&telconyx.FileLink{FileID: "fid", MessageID: 5, Route: "ghost"}).URL()
	for _, path := range []string{"/download", "/delete"} {
		t.Run(path, func(t *testing.T) {
			w := do(h, "POST", path, `{"url":"`+link+`"}`)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("got %d, want 400 (body=%s)", w.Code, w.Body.String())
			}
			if !strings.Contains(w.Body.String(), "unknown_route") {
				t.Errorf("expected unknown_route error code, got %s", w.Body.String())
			}
		})
	}
}

// TestDownload_PreStreamErrorReturnsJSON verifies that a download failing
// before the first body byte (e.g. expired file_id) produces a proper JSON
// error envelope instead of an empty 200 with stale file headers.
func TestDownload_PreStreamErrorReturnsJSON(t *testing.T) {
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/getFile") {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"ok":false,"error_code":400,"description":"Bad Request: file is too big"}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer fake.Close()

	c, err := telconyx.NewClient(telconyx.Config{Token: "t", ChatID: "-100", APIBase: fake.URL})
	if err != nil {
		t.Fatal(err)
	}
	h := New(c, Config{})

	link := (&telconyx.FileLink{FileID: "fid", Size: 10, Name: "x.bin", MimeType: "text/html"}).URL()
	w := do(h, "POST", "/download", `{"url":"`+link+`"}`)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("got %d, want 502 (body=%s)", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "download_failed") {
		t.Errorf("expected download_failed code, got %s", w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json (stale file headers must be cleared)", ct)
	}
	if cd := w.Header().Get("Content-Disposition"); cd != "" {
		t.Errorf("stale Content-Disposition survived: %q", cd)
	}
	if cl := w.Header().Get("Content-Length"); cl == "10" {
		t.Errorf("stale Content-Length survived: %q", cl)
	}
}

// TestHealth_SuccessEnvelope verifies the standard success envelope shape.
func TestHealth_SuccessEnvelope(t *testing.T) {
	h := newTestHandler(t, "", "-100")
	w := do(h, "GET", "/health", "")
	if w.Code != http.StatusOK {
		t.Fatalf("got %d, want 200", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	if w.Header().Get("X-Request-Id") == "" {
		t.Error("missing X-Request-Id header")
	}
	var resp struct {
		Data struct {
			Status string `json:"status"`
		} `json:"data"`
		Meta struct {
			RequestID string `json:"request_id"`
		} `json:"meta"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, w.Body.String())
	}
	if resp.Data.Status != "ok" {
		t.Errorf("data.status = %q, want ok", resp.Data.Status)
	}
	if resp.Meta.RequestID == "" {
		t.Error("meta.request_id is empty")
	}
}

// TestError_Envelope verifies the standard error envelope shape and that the
// HTTP status is authoritative (no redundant body status, no success:false).
func TestError_Envelope(t *testing.T) {
	h := newTestHandler(t, "", "-100")
	w := do(h, "POST", "/delete", `{bad json`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400", w.Code)
	}
	var resp struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
		Meta struct {
			RequestID string `json:"request_id"`
		} `json:"meta"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, w.Body.String())
	}
	if resp.Error.Code != "invalid_json" {
		t.Errorf("error.code = %q, want invalid_json", resp.Error.Code)
	}
	if resp.Error.Message == "" {
		t.Error("error.message is empty")
	}
	if resp.Meta.RequestID == "" {
		t.Error("meta.request_id is empty")
	}
	// X-Request-Id header should match meta.request_id.
	if got := w.Header().Get("X-Request-Id"); got != resp.Meta.RequestID {
		t.Errorf("X-Request-Id header %q != meta.request_id %q", got, resp.Meta.RequestID)
	}
}

// TestRequestID_PropagatesClientHeader verifies a sanitized incoming
// X-Request-Id is echoed back for tracing.
func TestRequestID_PropagatesClientHeader(t *testing.T) {
	h := newTestHandler(t, "", "-100")
	req := httptest.NewRequest("GET", "/health", nil)
	req.Header.Set("X-Request-Id", "trace-abc-123")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if got := w.Header().Get("X-Request-Id"); got != "trace-abc-123" {
		t.Errorf("X-Request-Id = %q, want trace-abc-123 (should propagate)", got)
	}
}
