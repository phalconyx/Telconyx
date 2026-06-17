// Package server exposes Telconyx as an HTTP service.
//
// Endpoints:
//
//	GET  /health     - liveness check
//	POST /upload     - multipart upload, returns FileLink JSON
//	POST /download   - JSON body {"url": "telconyx://..."}, streams file bytes
//	POST /delete     - JSON body {"url": "telconyx://..."}, deletes the file's Telegram message(s)
package server

import (
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
	// client's MaxUploadSize (which itself defaults to 2 GiB).
	MaxUploadBytes int64
}

// New returns a configured http.Handler.
func New(c *telconyx.Client, cfg Config) http.Handler {
	if cfg.MaxUploadBytes <= 0 {
		cfg.MaxUploadBytes = c.Config().MaxUploadSize
	}
	h := &handler{client: c, cfg: cfg}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("POST /upload", h.upload)
	mux.HandleFunc("POST /download", h.download)
	mux.HandleFunc("POST /delete", h.delete)
	return mux
}

type handler struct {
	client *telconyx.Client
	cfg    Config
}

func (h *handler) auth(w http.ResponseWriter, r *http.Request) bool {
	if h.cfg.APIKey == "" {
		return true
	}
	got := r.Header.Get("X-API-Key")
	if got == "" || got != h.cfg.APIKey {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return false
	}
	return true
}

func (h *handler) health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
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
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid multipart: %v", err))
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("missing 'file' field: %v", err))
		return
	}
	defer file.Close()

	// Save the multipart body to a temp file. This is required to support
	// chunked uploads for files larger than the Bot API 50 MB limit.
	ext := filepath.Ext(header.Filename)
	tmp, err := os.CreateTemp("", "telconyx-upload-*"+ext)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("create temp: %v", err))
		return
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	written, err := io.Copy(tmp, file)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("write temp: %v", err))
		return
	}

	// Re-open for the chunked upload path; UploadFileHandle seeks into the file.
	src, err := os.Open(tmpPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("reopen temp: %v", err))
		return
	}
	defer src.Close()

	opts := telconyx.UploadOpts{
		Name:     header.Filename,
		Caption:  r.FormValue("caption"),
		MimeType: header.Header.Get("Content-Type"),
	}

	result, err := h.client.UploadFileHandle(r.Context(), src, written, opts)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}

	resp := uploadResponse{
		UploadResult: result,
		URL:          result.Link(),
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
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
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid json body: %v", err))
		return
	}
	if strings.TrimSpace(req.URL) == "" {
		writeError(w, http.StatusBadRequest, "missing 'url' field")
		return
	}
	link, err := telconyx.ParseURL(req.URL)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
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
	if _, err := h.client.DownloadTo(r.Context(), link, w); err != nil {
		// Headers may already be written; just abort.
		// The client will see a truncated response.
		return
	}
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
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid json body: %v", err))
		return
	}
	if strings.TrimSpace(req.URL) == "" {
		writeError(w, http.StatusBadRequest, "missing 'url' field")
		return
	}
	link, err := telconyx.ParseURL(req.URL)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
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

	if err := h.client.DeleteChunks(r.Context(), link); err != nil {
		// Some messages may already have been deleted before the failure.
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success":          true,
		"deleted_messages": deletable,
		"total_chunks":     len(chunks),
		"skipped":          len(chunks) - deletable,
	})
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error":   msg,
		"status":  code,
		"success": false,
	})
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

// Ensure the package is referenced even on minor build paths.
var _ = errors.New
