package telconyx

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrFileTooBig indicates a single chunk exceeds the Bot API limit (50 MB).
	// This is internal; user-facing errors should be ErrUploadTooLarge or ErrDownloadTooLarge.
	ErrFileTooBig = errors.New("telconyx: chunk exceeds Bot API limit (50 MB)")
	// ErrUploadTooLarge indicates the file exceeds the configured MaxUploadSize.
	ErrUploadTooLarge = errors.New("telconyx: file exceeds MaxUploadSize")
	// ErrDownloadTooLarge indicates the file exceeds the configured MaxDownloadSize.
	ErrDownloadTooLarge = errors.New("telconyx: file exceeds MaxDownloadSize")
	ErrInvalidConfig    = errors.New("telconyx: invalid config")
	ErrInvalidLink      = errors.New("telconyx: invalid telconyx:// link")
	ErrUnauthorized     = errors.New("telconyx: unauthorized")
	// ErrUnknownRoute indicates a link references a route alias that is not
	// configured in the Pool serving the request.
	ErrUnknownRoute = errors.New("telconyx: link references an unknown route")
)

// APIError represents a non-success response from the Telegram Bot API.
type APIError struct {
	Code        int
	Description string
	Method      string
}

func (e *APIError) Error() string {
	if e.Method != "" {
		return fmt.Sprintf("telconyx: %s: api error %d: %s", e.Method, e.Code, e.Description)
	}
	return fmt.Sprintf("telconyx: api error %d: %s", e.Code, e.Description)
}

// NonRetryableError signals that a request will keep failing no matter how
// many times it is retried (e.g. the server returned a malformed response,
// the chat rejected the file, or the bot lacks permission). Returning it
// from an upload/download step short-circuits withRetry to avoid producing
// duplicate uploads in the target chat.
type NonRetryableError struct {
	Method string
	Reason string
	// Detail is an optional snippet of the server response for debugging.
	Detail string
	// Err is the optional underlying cause, preserved for errors.Is/As.
	Err error
}

func (e *NonRetryableError) Error() string {
	msg := ""
	switch {
	case e.Method != "" && e.Detail != "":
		msg = fmt.Sprintf("telconyx: %s: %s (non-retryable; server said: %s)", e.Method, e.Reason, e.Detail)
	case e.Method != "":
		msg = fmt.Sprintf("telconyx: %s: %s (non-retryable)", e.Method, e.Reason)
	default:
		msg = fmt.Sprintf("telconyx: %s (non-retryable)", e.Reason)
	}
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

func (e *NonRetryableError) Unwrap() error { return e.Err }

// PartialUploadError reports a chunked upload that failed after at least one
// chunk was already sent to the chat. Link references the sent chunks so they
// can be removed with DeleteChunks before retrying; retrying the upload
// without cleanup duplicates those chunks in the chat.
type PartialUploadError struct {
	// Uploaded is the number of chunks successfully sent before the failure.
	Uploaded int
	// Total is the number of chunks the complete file would have.
	Total int
	// Link references only the sent chunks, for cleanup via DeleteChunks.
	Link *FileLink
	// Err is the underlying chunk failure.
	Err error
}

func (e *PartialUploadError) Error() string {
	return fmt.Sprintf("telconyx: chunked upload failed after %d/%d chunks: %v", e.Uploaded, e.Total, e.Err)
}

func (e *PartialUploadError) Unwrap() error { return e.Err }

// FloodWaitError indicates a rate-limit response (HTTP 429) with retry_after.
type FloodWaitError struct {
	Seconds int
}

func (e *FloodWaitError) Error() string {
	return fmt.Sprintf("telconyx: flood wait %ds", e.Seconds)
}

// Duration returns the suggested wait time as a time.Duration.
func (e *FloodWaitError) Duration() time.Duration {
	if e.Seconds <= 0 {
		return 0
	}
	return time.Duration(e.Seconds) * time.Second
}
