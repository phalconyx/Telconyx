package telconyx

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/phalconyx/telconyx/internal/transport"
)

// Bot API limits and library defaults.
const (
	// MaxFileSize is the Bot API upload limit per single message (50 MB).
	// A chunk must not exceed this size.
	MaxFileSize = 50 * 1024 * 1024

	// MaxBotDownloadSize is the hosted Bot API getFile download limit (20 MB).
	// Bots can SEND files up to 50 MB but can only DOWNLOAD files up to 20 MB
	// through api.telegram.org — anything stored in larger pieces cannot be
	// retrieved. A self-hosted telegram-bot-api server (Config.APIBase) lifts
	// this limit.
	MaxBotDownloadSize = 20 * 1024 * 1024

	// DefaultChunkSize is the default chunk size for split uploads.
	// 19 MB keeps every chunk under the 20 MB getFile download limit
	// (MaxBotDownloadSize) with headroom, so files remain downloadable
	// through the hosted Bot API. With a self-hosted API server, ChunkSize
	// can be raised up to MaxFileSize.
	DefaultChunkSize int64 = 19 * 1024 * 1024

	// DefaultMaxFileSize is the default maximum total file size for upload/download.
	// 2 GB.
	DefaultMaxFileSize int64 = 2 * 1024 * 1024 * 1024

	// DefaultChunkConcurrency is the default number of concurrent chunk downloads.
	DefaultChunkConcurrency = 3
)

// Config is the configuration for a Client.
type Config struct {
	// Token is the Telegram bot token (e.g. "123456:ABC-DEF...").
	Token string
	// ChatID is the target chat (numeric ID like "-1001234567890" or "@groupusername").
	ChatID string
	// APIBase is the Bot API server root. Default: "https://api.telegram.org".
	// Point it at a self-hosted telegram-bot-api server to lift the hosted
	// API's 20 MB getFile download limit (see MaxBotDownloadSize).
	APIBase string
	// Timeout bounds the connection phases of each HTTP request (dial, TLS,
	// wait for response headers). It does NOT bound body transfer, so slow
	// large uploads/downloads are never cut off mid-stream; use a context
	// deadline to bound a whole operation. Default: 60s.
	Timeout time.Duration
	// Retries is the maximum number of attempts for retryable errors. Default: 3.
	Retries int
	// BackoffBase is the base delay for exponential backoff. Default: 500ms.
	BackoffBase time.Duration
	// BackoffMax is the maximum delay between retries, and also the longest
	// flood-wait (429 retry_after) that is honoured by sleeping in place — a
	// longer flood-wait is returned as an error immediately so the caller
	// (or a Pool, which then reroutes) decides what to do. Default: 30s.
	BackoffMax time.Duration

	// MaxUploadSize is the maximum total file size in bytes for uploads.
	// Files larger than this are rejected with ErrUploadTooLarge.
	// Default: DefaultMaxFileSize (2 GB).
	MaxUploadSize int64
	// MaxDownloadSize is the maximum total file size in bytes for downloads.
	// Default: DefaultMaxFileSize (2 GB).
	MaxDownloadSize int64
	// ChunkSize is the size of each chunk when splitting large files.
	// Capped at MaxFileSize. Default: DefaultChunkSize (49 MB).
	ChunkSize int64
	// ChunkConcurrency is the number of concurrent chunk downloads (1+).
	// Default: DefaultChunkConcurrency (3).
	ChunkConcurrency int
}

// Client is a Telconyx client bound to a single bot + chat.
type Client struct {
	cfg Config
	tp  *transport.Client
}

// NewClient creates a new Client.
func NewClient(cfg Config) (*Client, error) {
	if cfg.Token == "" {
		return nil, errors.New("telconyx: Token is required")
	}
	if cfg.ChatID == "" {
		return nil, errors.New("telconyx: ChatID is required")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 60 * time.Second
	}
	if cfg.Retries <= 0 {
		cfg.Retries = 3
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = 500 * time.Millisecond
	}
	if cfg.BackoffMax <= 0 {
		cfg.BackoffMax = 30 * time.Second
	}
	if cfg.MaxUploadSize <= 0 {
		cfg.MaxUploadSize = DefaultMaxFileSize
	}
	if cfg.MaxDownloadSize <= 0 {
		cfg.MaxDownloadSize = DefaultMaxFileSize
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = DefaultChunkSize
	}
	if cfg.ChunkSize > MaxFileSize {
		cfg.ChunkSize = MaxFileSize
	}
	if cfg.ChunkConcurrency <= 0 {
		cfg.ChunkConcurrency = DefaultChunkConcurrency
	}
	return &Client{
		cfg: cfg,
		tp:  transport.New(cfg.Token, cfg.Timeout, cfg.APIBase),
	}, nil
}

// Config returns a copy of the active configuration.
func (c *Client) Config() Config { return c.cfg }

// MaxUploadSize returns the configured maximum total upload size in bytes.
func (c *Client) MaxUploadSize() int64 { return c.cfg.MaxUploadSize }

// Resolve verifies the link can be served. A single-bot Client serves every
// link (it cannot know which route alias it corresponds to); the route-aware
// check lives on Pool.
func (c *Client) Resolve(link *FileLink) error {
	if link == nil {
		return ErrInvalidLink
	}
	return nil
}

// Close releases any idle HTTP connections held by the underlying transport.
func (c *Client) Close() {
	c.tp.Close()
}

// withRetry runs fn with retry logic for transient errors.
// It returns the last error if all attempts fail.
func (c *Client) withRetry(ctx context.Context, fn func(ctx context.Context) error) error {
	var lastErr error
	for attempt := 0; attempt < c.cfg.Retries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := fn(ctx)
		if err == nil {
			return nil
		}
		lastErr = err

		// Context cancellation: stop immediately.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		// Non-retryable API error: stop.
		var apiErr *APIError
		if errors.As(err, &apiErr) {
			if apiErr.Code >= 400 && apiErr.Code < 500 && apiErr.Code != 429 {
				return err
			}
		}

		// Non-retryable error (e.g. malformed response, file rejected by chat):
		// stop immediately to avoid producing duplicate uploads in the group.
		var nr *NonRetryableError
		if errors.As(err, &nr) {
			return err
		}

		// Compute delay: flood-wait or exponential backoff.
		var delay time.Duration
		var fw *FloodWaitError
		if errors.As(err, &fw) {
			delay = fw.Duration()
			// A flood-wait longer than BackoffMax would starve the caller
			// (and, in a Pool, block failover to a colder route). Surface it
			// instead of sleeping; the pool marks the route cooling down.
			if delay > c.cfg.BackoffMax {
				return err
			}
		} else {
			delay = backoff(attempt, c.cfg.BackoffBase, c.cfg.BackoffMax)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return lastErr
}

// backoff returns a delay with exponential growth and ±50% jitter.
func backoff(attempt int, base, max time.Duration) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	shift := attempt
	if shift > 12 {
		shift = 12
	}
	d := base << shift
	if d <= 0 || d > max {
		d = max
	}
	half := int64(d) / 2
	if half <= 0 {
		return 0
	}
	return time.Duration(half + rand.Int64N(half+1))
}
