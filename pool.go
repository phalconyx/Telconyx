package telconyx

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// Route is one upload destination: a bot token plus a target chat, identified
// by a stable short alias. The alias is embedded in every telconyx:// link
// created through it, so downloads and deletes can be routed back to the same
// bot — Telegram file_ids are only valid for the bot that uploaded them.
//
// Aliases may only contain [A-Za-z0-9_-] and must be unique within a Pool.
// Renaming or removing an alias breaks existing links that reference it, so
// treat aliases as permanent identifiers.
type Route struct {
	Alias  string
	Token  string
	ChatID string
}

// PoolConfig configures a Pool.
type PoolConfig struct {
	// Routes lists the available destinations. At least one is required.
	// A single route may have an empty alias — its links then carry no route
	// marker, exactly like a plain Client. With multiple routes every alias
	// must be non-empty.
	Routes []Route

	// DefaultRoute is the alias assumed for links that carry no route marker
	// (links created by a single-bot setup). When migrating from a single bot
	// to multiple routes, point this at the route holding the original bot so
	// old links keep resolving. Default: the first route.
	DefaultRoute string

	// Picker selects the route for each new upload. Default: least-inflight —
	// the route with the fewest uploads currently in progress, with ties
	// broken round-robin so an idle pool degrades to pure rotation.
	Picker Picker

	// Base carries the shared per-client tuning (timeouts, retries, size
	// limits, chunking). Its Token and ChatID fields are ignored; those come
	// from each Route.
	Base Config
}

// Picker selects the route for the next upload. Pick receives the candidate
// aliases — routes currently in flood-wait cooldown are already filtered
// out — and returns one of them. The slice is never empty. Implementations
// must be safe for concurrent use.
type Picker interface {
	Pick(aliases []string) string
}

// NewRoundRobin returns a Picker that cycles through the candidates in order,
// ignoring load. It is available as an explicit alternative to the default
// least-inflight picker.
func NewRoundRobin() Picker { return &roundRobin{} }

type roundRobin struct{ n atomic.Uint64 }

func (r *roundRobin) Pick(aliases []string) string {
	i := r.n.Add(1) - 1
	return aliases[i%uint64(len(aliases))]
}

// leastInflight is the default Picker: it selects the candidate with the
// fewest uploads currently in flight. Uploads here are long-lived and wildly
// heterogeneous (a 1 KB note vs a 2 GB chunked file), so counting active
// uploads is a good proxy for how much message-rate pressure a route is about
// to generate. Ties — including the all-idle case — are broken round-robin,
// so a quiet pool behaves exactly like plain rotation.
type leastInflight struct {
	p *Pool
	n atomic.Uint64 // tie-break rotation
}

func (l *leastInflight) Pick(aliases []string) string {
	l.p.mu.Lock()
	ties := make([]string, 0, len(aliases))
	min := 0
	for i, a := range aliases {
		n := l.p.inflight[a]
		if i == 0 || n < min {
			min = n
			ties = ties[:0]
		}
		if n == min {
			ties = append(ties, a)
		}
	}
	l.p.mu.Unlock()
	i := l.n.Add(1) - 1
	return ties[i%uint64(len(ties))]
}

// Pool distributes uploads over multiple routes (bot + chat pairs) and routes
// downloads and deletes back to the route that stored the file. It exposes the
// same operations as Client, so the HTTP server and CLI work with either.
type Pool struct {
	clients map[string]*Client
	order   []string // aliases in config order
	def     string   // alias resolved for links without a route marker
	picker  Picker

	mu        sync.Mutex
	coolUntil map[string]time.Time // flood-wait cooldown per alias
	inflight  map[string]int       // uploads in progress per alias
}

// NewPool creates a Pool from cfg.
func NewPool(cfg PoolConfig) (*Pool, error) {
	if len(cfg.Routes) == 0 {
		return nil, fmt.Errorf("%w: at least one route is required", ErrInvalidConfig)
	}
	p := &Pool{
		clients:   make(map[string]*Client, len(cfg.Routes)),
		coolUntil: make(map[string]time.Time),
		inflight:  make(map[string]int),
		picker:    cfg.Picker,
	}
	if p.picker == nil {
		p.picker = &leastInflight{p: p}
	}
	for _, rt := range cfg.Routes {
		if rt.Alias == "" && len(cfg.Routes) > 1 {
			return nil, fmt.Errorf("%w: route aliases are required when more than one route is configured", ErrInvalidConfig)
		}
		if !validAlias(rt.Alias) {
			return nil, fmt.Errorf("%w: invalid route alias %q (allowed characters: A-Z a-z 0-9 - _)", ErrInvalidConfig, rt.Alias)
		}
		if _, dup := p.clients[rt.Alias]; dup {
			return nil, fmt.Errorf("%w: duplicate route alias %q", ErrInvalidConfig, rt.Alias)
		}
		cc := cfg.Base
		cc.Token = rt.Token
		cc.ChatID = rt.ChatID
		client, err := NewClient(cc)
		if err != nil {
			return nil, fmt.Errorf("telconyx: route %q: %w", rt.Alias, err)
		}
		p.clients[rt.Alias] = client
		p.order = append(p.order, rt.Alias)
	}
	p.def = cfg.DefaultRoute
	if p.def == "" {
		p.def = cfg.Routes[0].Alias
	}
	if _, ok := p.clients[p.def]; !ok {
		return nil, fmt.Errorf("%w: DefaultRoute %q is not a configured route", ErrInvalidConfig, cfg.DefaultRoute)
	}
	return p, nil
}

func validAlias(s string) bool {
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
		default:
			return false
		}
	}
	return true
}

// Routes returns the configured route aliases in configuration order.
func (p *Pool) Routes() []string {
	out := make([]string, len(p.order))
	copy(out, p.order)
	return out
}

// Client returns the underlying client for a route alias. An empty alias
// resolves to the default route. It returns ErrUnknownRoute for aliases not
// configured in this pool.
func (p *Pool) Client(alias string) (*Client, error) {
	if alias == "" {
		alias = p.def
	}
	c, ok := p.clients[alias]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownRoute, alias)
	}
	return c, nil
}

// Resolve verifies the link's route is served by this pool.
func (p *Pool) Resolve(link *FileLink) error {
	_, err := p.clientFor(link)
	return err
}

// MaxUploadSize returns the maximum total upload size shared by all routes.
func (p *Pool) MaxUploadSize() int64 {
	return p.clients[p.def].cfg.MaxUploadSize
}

// Close releases idle HTTP connections of every route's client.
func (p *Pool) Close() {
	for _, c := range p.clients {
		c.Close()
	}
}

func (p *Pool) clientFor(link *FileLink) (*Client, error) {
	if link == nil {
		return nil, ErrInvalidLink
	}
	return p.Client(link.Route)
}

// UploadFile uploads a local file via a route chosen by the Picker.
// See Client.UploadFile for chunking behaviour.
func (p *Pool) UploadFile(ctx context.Context, path string) (*UploadResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	opts := UploadOpts{Name: filepath.Base(path)}
	if mt := mime.TypeByExtension(filepath.Ext(path)); mt != "" {
		opts.MimeType = mt
	}
	return p.UploadFileHandle(ctx, f, info.Size(), opts)
}

// UploadFileHandle uploads from an open *os.File via a route chosen by the
// Picker. See Client.UploadFileHandle. The caller is responsible for closing f.
func (p *Pool) UploadFileHandle(ctx context.Context, f *os.File, size int64, opts UploadOpts) (*UploadResult, error) {
	return p.uploadFailover(ctx, func(ctx context.Context, c *Client) (*UploadResult, error) {
		return c.UploadFileHandle(ctx, f, size, opts)
	})
}

// UploadReader reads the entire source into memory then uploads it via a
// route chosen by the Picker. See Client.UploadReader for size limits.
func (p *Pool) UploadReader(ctx context.Context, r io.Reader, opts UploadOpts) (*UploadResult, error) {
	// Buffer once so a failover attempt on another route can re-read.
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("telconyx: read source: %w", err)
	}
	return p.uploadFailover(ctx, func(ctx context.Context, c *Client) (*UploadResult, error) {
		return c.UploadReader(ctx, bytes.NewReader(data), opts)
	})
}

// Download routes the download to the link's origin route.
// See Client.Download.
func (p *Pool) Download(ctx context.Context, link *FileLink, dest string) (int64, error) {
	c, err := p.clientFor(link)
	if err != nil {
		return 0, err
	}
	return c.Download(ctx, link, dest)
}

// DownloadTo routes the download to the link's origin route.
// See Client.DownloadTo.
func (p *Pool) DownloadTo(ctx context.Context, link *FileLink, w io.Writer) (int64, error) {
	c, err := p.clientFor(link)
	if err != nil {
		return 0, err
	}
	return c.DownloadTo(ctx, link, w)
}

// DeleteChunks routes the deletion to the link's origin route — only the bot
// that sent a message can delete it. See Client.DeleteChunks.
func (p *Pool) DeleteChunks(ctx context.Context, link *FileLink) error {
	c, err := p.clientFor(link)
	if err != nil {
		return err
	}
	return c.DeleteChunks(ctx, link)
}

// uploadFailover runs up against a picked route and, when the failure is safe
// to move (nothing persisted, not a permanent rejection), retries on the next
// route. Each route is tried at most once.
func (p *Pool) uploadFailover(ctx context.Context, up func(context.Context, *Client) (*UploadResult, error)) (*UploadResult, error) {
	tried := make(map[string]bool, len(p.order))
	var lastErr error
	for range p.order {
		alias := p.pick(tried)
		tried[alias] = true

		p.addInflight(alias, 1)
		res, err := up(ctx, p.clients[alias])
		p.addInflight(alias, -1)
		if err == nil {
			res.Route = alias
			return res, nil
		}
		if alias != "" {
			lastErr = fmt.Errorf("telconyx: route %q: %w", alias, err)
		} else {
			lastErr = err
		}

		var fw *FloodWaitError
		if errors.As(err, &fw) {
			p.markCooldown(alias, fw.Duration())
		}
		// Stamp the route on a partial-upload link so cleanup via
		// Pool.DeleteChunks reaches the bot that holds the chunks.
		var pe *PartialUploadError
		if errors.As(err, &pe) && pe.Link != nil && pe.Link.Route == "" {
			pe.Link.Route = alias
		}
		if !failoverEligible(err) {
			return nil, lastErr
		}
	}
	return nil, lastErr
}

// pick chooses the next upload route, skipping routes in flood-wait cooldown
// and already-tried routes when possible. It degrades gracefully: if every
// route is cooling down, cooldowns are ignored rather than failing outright.
func (p *Pool) pick(tried map[string]bool) string {
	now := time.Now()
	p.mu.Lock()
	avail := make([]string, 0, len(p.order))
	for _, a := range p.order {
		if tried[a] {
			continue
		}
		if until, ok := p.coolUntil[a]; ok && now.Before(until) {
			continue
		}
		avail = append(avail, a)
	}
	p.mu.Unlock()
	if len(avail) == 0 {
		for _, a := range p.order {
			if !tried[a] {
				avail = append(avail, a)
			}
		}
	}
	if len(avail) == 0 {
		avail = p.order
	}
	return p.picker.Pick(avail)
}

// addInflight adjusts the number of uploads in progress on a route; the
// default least-inflight picker reads these counters.
func (p *Pool) addInflight(alias string, delta int) {
	p.mu.Lock()
	p.inflight[alias] += delta
	p.mu.Unlock()
}

// markCooldown excludes a route from picking for d, extending (never
// shortening) any cooldown already in place.
func (p *Pool) markCooldown(alias string, d time.Duration) {
	if d <= 0 {
		return
	}
	p.mu.Lock()
	if t := time.Now().Add(d); t.After(p.coolUntil[alias]) {
		p.coolUntil[alias] = t
	}
	p.mu.Unlock()
}

// failoverEligible reports whether an upload error is safe and useful to retry
// on a different route. Not eligible: cancellations, permanent rejections,
// size-limit errors (limits are shared by all routes), and partial chunked
// uploads — those already left messages in a chat, and retrying elsewhere
// would duplicate them.
func failoverEligible(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, ErrUploadTooLarge) || errors.Is(err, ErrFileTooBig) {
		return false
	}
	var pe *PartialUploadError
	if errors.As(err, &pe) {
		return false
	}
	var nre *NonRetryableError
	if errors.As(err, &nre) {
		return false
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		if apiErr.Code >= 400 && apiErr.Code < 500 && apiErr.Code != 429 {
			return false
		}
	}
	return true
}
