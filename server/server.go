// Package server exposes Telconyx as an HTTP service.
//
// All JSON responses share a consistent envelope:
//
//	success: {"data": <payload>, "meta": {"request_id": "..."}}
//	error:   {"error": {"code": "...", "message": "...", "details": {...}?}, "meta": {"request_id": "..."}}
//
// The HTTP status code is authoritative; the body never contradicts it.
// /download streams raw file bytes on success (no envelope); only its
// pre-stream errors use the JSON envelope. Every response carries an
// X-Request-Id header echoing meta.request_id.
//
// Endpoints:
//
//	GET  /health     - liveness check
//	POST /upload     - multipart upload (field "file"), returns the file's metadata + telconyx:// url
//	POST /download   - JSON body {"url": "telconyx://..."}, streams file bytes
//	POST /delete     - JSON body {"url": "telconyx://..."}, deletes the file's Telegram message(s)
package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/phalconyx/telconyx"
)

// Config is the server configuration.
type Config struct {
	// APIKey, if non-empty, requires the X-API-Key header on all endpoints
	// except /health.
	APIKey string
	// MaxUploadBytes caps the multipart upload body. Default: same as the
	// store's MaxUploadSize (which itself defaults to 2 GiB).
	MaxUploadBytes int64
}

// Storage is the subset of telconyx operations the server needs. Both
// *telconyx.Client (single bot) and *telconyx.Pool (multiple routed bots)
// implement it.
type Storage interface {
	UploadFileHandle(ctx context.Context, f *os.File, size int64, opts telconyx.UploadOpts) (*telconyx.UploadResult, error)
	DownloadTo(ctx context.Context, link *telconyx.FileLink, w io.Writer) (int64, error)
	DeleteChunks(ctx context.Context, link *telconyx.FileLink) error
	Resolve(link *telconyx.FileLink) error
	MaxUploadSize() int64
}

// New returns a configured http.Handler backed by store.
func New(store Storage, cfg Config) http.Handler {
	if cfg.MaxUploadBytes <= 0 {
		cfg.MaxUploadBytes = store.MaxUploadSize()
	}
	h := &handler{store: store, cfg: cfg}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("POST /upload", h.upload)
	mux.HandleFunc("POST /download", h.download)
	mux.HandleFunc("POST /delete", h.delete)
	return mux
}

type handler struct {
	store Storage
	cfg   Config
}

func (h *handler) auth(w http.ResponseWriter, r *http.Request) bool {
	if h.cfg.APIKey == "" {
		return true
	}
	got := r.Header.Get("X-API-Key")
	if got == "" || got != h.cfg.APIKey {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "missing or invalid X-API-Key")
		return false
	}
	return true
}

func (h *handler) health(w http.ResponseWriter, r *http.Request) {
	writeData(w, r, http.StatusOK, map[string]any{
		"status": "ok",
		"time":   time.Now().UTC().Format(time.RFC3339),
	})
}

func (h *handler) upload(w http.ResponseWriter, r *http.Request) {
	if !h.auth(w, r) {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, h.cfg.MaxUploadBytes)
	if err := r.ParseMultipartForm(h.cfg.MaxUploadBytes); err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_multipart", fmt.Sprintf("invalid multipart form: %v", err))
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "missing_file", fmt.Sprintf("missing 'file' field: %v", err))
		return
	}
	defer file.Close()

	// Save the multipart body to a temp file. This is required to support
	// chunked uploads for files larger than the Bot API 50 MB limit.
	ext := filepath.Ext(header.Filename)
	tmp, err := os.CreateTemp("", "telconyx-upload-*"+ext)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "internal", fmt.Sprintf("create temp file: %v", err))
		return
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	written, err := io.Copy(tmp, file)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "internal", fmt.Sprintf("write temp file: %v", err))
		return
	}

	// Re-open for the chunked upload path; UploadFileHandle seeks into the file.
	src, err := os.Open(tmpPath)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "internal", fmt.Sprintf("reopen temp file: %v", err))
		return
	}
	defer src.Close()

	opts := telconyx.UploadOpts{
		Name:     header.Filename,
		Caption:  r.FormValue("caption"),
		MimeType: header.Header.Get("Content-Type"),
	}

	result, err := h.store.UploadFileHandle(r.Context(), src, written, opts)
	if err != nil {
		writeError(w, r, http.StatusBadGateway, "upload_failed", err.Error())
		return
	}

	writeData(w, r, http.StatusCreated, uploadResponse{
		UploadResult: result,
		URL:          result.Link(),
	})
}

type uploadResponse struct {
	*telconyx.UploadResult
	URL string `json:"url"`
}

func (h *handler) download(w http.ResponseWriter, r *http.Request) {
	if !h.auth(w, r) {
		return
	}
	var req struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_json", fmt.Sprintf("invalid json body: %v", err))
		return
	}
	if strings.TrimSpace(req.URL) == "" {
		writeError(w, r, http.StatusBadRequest, "missing_url", "missing 'url' field")
		return
	}
	link, err := telconyx.ParseURL(req.URL)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_link", err.Error())
		return
	}
	if err := h.store.Resolve(link); err != nil {
		writeUnresolvable(w, r, err)
		return
	}
	if link.MimeType != "" {
		w.Header().Set("Content-Type", link.MimeType)
	}
	if link.Name != "" {
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, sanitizeFilename(link.Name)))
	}
	if link.Size > 0 {
		w.Header().Set("Content-Length", strconv.Itoa(link.Size))
	}
	if link.IsChunked() {
		w.Header().Set("X-Telconyx-Chunks", strconv.Itoa(len(link.AllChunks())))
	}
	// On success the body is the raw file stream (not enveloped). If DownloadTo
	// fails after headers/bytes are already written, we can only abort; the
	// client sees a truncated response.
	if _, err := h.store.DownloadTo(r.Context(), link, w); err != nil {
		return
	}
}

// writeUnresolvable maps a Resolve failure to an error response: a link whose
// route alias is not configured on this server gets a dedicated code so the
// operator knows it is a configuration mismatch, not a bad link.
func writeUnresolvable(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, telconyx.ErrUnknownRoute) {
		writeError(w, r, http.StatusBadRequest, "unknown_route", err.Error())
		return
	}
	writeError(w, r, http.StatusBadRequest, "invalid_link", err.Error())
}

// delete removes the Telegram message(s) backing a telconyx:// file. For chunked
// files every part is deleted. Requires a numeric TELCONYX_CHAT_ID.
func (h *handler) delete(w http.ResponseWriter, r *http.Request) {
	if !h.auth(w, r) {
		return
	}
	var req struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_json", fmt.Sprintf("invalid json body: %v", err))
		return
	}
	if strings.TrimSpace(req.URL) == "" {
		writeError(w, r, http.StatusBadRequest, "missing_url", "missing 'url' field")
		return
	}
	link, err := telconyx.ParseURL(req.URL)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_link", err.Error())
		return
	}
	if err := h.store.Resolve(link); err != nil {
		writeUnresolvable(w, r, err)
		return
	}

	// Count parts whose message id is known (legacy links only carry the first).
	chunks := link.AllChunks()
	deletable := 0
	for _, ch := range chunks {
		if ch.MessageID != 0 {
			deletable++
		}
	}

	if err := h.store.DeleteChunks(r.Context(), link); err != nil {
		// Some messages may already have been deleted before the failure.
		writeError(w, r, http.StatusBadGateway, "delete_failed", err.Error())
		return
	}

	writeData(w, r, http.StatusOK, map[string]any{
		"deleted_messages": deletable,
		"total_chunks":     len(chunks),
		"skipped":          len(chunks) - deletable,
	})
}

// writeData writes a success envelope: {"data": <data>, "meta": {"request_id": ...}}.
func writeData(w http.ResponseWriter, r *http.Request, code int, data any) {
	rid := requestID(r)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Request-Id", rid)
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"data": data,
		"meta": map[string]any{"request_id": rid},
	})
}

// writeError writes an error envelope with a machine-readable code and a
// human-readable message. The HTTP status code carries the error category.
func writeError(w http.ResponseWriter, r *http.Request, code int, errCode, message string) {
	writeErrorDetails(w, r, code, errCode, message, nil)
}

// writeErrorDetails is writeError with an optional structured details payload.
func writeErrorDetails(w http.ResponseWriter, r *http.Request, code int, errCode, message string, details any) {
	rid := requestID(r)
	errObj := map[string]any{"code": errCode, "message": message}
	if details != nil {
		errObj["details"] = details
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Request-Id", rid)
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": errObj,
		"meta":  map[string]any{"request_id": rid},
	})
}

// requestID returns a correlation id for the request: the client-supplied
// X-Request-Id (sanitized) if present, otherwise a freshly generated one.
func requestID(r *http.Request) string {
	if id := sanitizeRequestID(r.Header.Get("X-Request-Id")); id != "" {
		return id
	}
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "req_unknown"
	}
	return "req_" + hex.EncodeToString(b[:])
}

// sanitizeRequestID keeps only header-safe characters and caps the length, so a
// client-supplied id can be echoed back without enabling header injection.
func sanitizeRequestID(s string) string {
	if len(s) > 128 {
		s = s[:128]
	}
	return strings.Map(func(rn rune) rune {
		switch {
		case rn >= 'a' && rn <= 'z', rn >= 'A' && rn <= 'Z', rn >= '0' && rn <= '9':
			return rn
		case rn == '-', rn == '_', rn == '.':
			return rn
		default:
			return -1
		}
	}, s)
}

func sanitizeFilename(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "\"", "")
	if s == "" {
		return "download"
	}
	return s
}
