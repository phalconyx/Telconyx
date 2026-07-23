package telconyx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func newTestPool(t *testing.T, aliases ...string) *Pool {
	t.Helper()
	routes := make([]Route, 0, len(aliases))
	for i, a := range aliases {
		routes = append(routes, Route{Alias: a, Token: fmt.Sprintf("tok%d", i), ChatID: fmt.Sprintf("-10%d", i)})
	}
	p, err := NewPool(PoolConfig{Routes: routes})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	return p
}

func TestNewPool_Validation(t *testing.T) {
	cases := []struct {
		name string
		cfg  PoolConfig
	}{
		{"no routes", PoolConfig{}},
		{"duplicate alias", PoolConfig{Routes: []Route{
			{Alias: "b1", Token: "t", ChatID: "-1"},
			{Alias: "b1", Token: "t2", ChatID: "-2"},
		}}},
		{"empty alias with multiple routes", PoolConfig{Routes: []Route{
			{Alias: "", Token: "t", ChatID: "-1"},
			{Alias: "b2", Token: "t2", ChatID: "-2"},
		}}},
		{"invalid alias characters", PoolConfig{Routes: []Route{
			{Alias: "b 1", Token: "t", ChatID: "-1"},
		}}},
		{"unknown default route", PoolConfig{
			Routes:       []Route{{Alias: "b1", Token: "t", ChatID: "-1"}},
			DefaultRoute: "ghost",
		}},
		{"missing token", PoolConfig{Routes: []Route{{Alias: "b1", ChatID: "-1"}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewPool(tc.cfg); err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

func TestNewPool_SingleEmptyAlias(t *testing.T) {
	// Legacy mode: one route without an alias behaves like a plain Client.
	p, err := NewPool(PoolConfig{Routes: []Route{{Token: "t", ChatID: "-1"}}})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	if _, err := p.Client(""); err != nil {
		t.Errorf("Client(\"\") should resolve the default route: %v", err)
	}
}

func TestPool_ClientAndResolve(t *testing.T) {
	p := newTestPool(t, "b1", "b2")

	if _, err := p.Client("b2"); err != nil {
		t.Errorf("Client(b2): %v", err)
	}
	// Empty alias resolves to the default (first) route.
	def, err := p.Client("")
	if err != nil {
		t.Fatalf("Client(\"\"): %v", err)
	}
	b1, _ := p.Client("b1")
	if def != b1 {
		t.Error("empty alias should resolve to the first route")
	}

	if _, err := p.Client("ghost"); !errors.Is(err, ErrUnknownRoute) {
		t.Errorf("Client(ghost): got %v, want ErrUnknownRoute", err)
	}
	if err := p.Resolve(&FileLink{FileID: "f", Route: "ghost"}); !errors.Is(err, ErrUnknownRoute) {
		t.Errorf("Resolve(ghost): got %v, want ErrUnknownRoute", err)
	}
	if err := p.Resolve(&FileLink{FileID: "f"}); err != nil {
		t.Errorf("Resolve(no route): %v", err)
	}
	if err := p.Resolve(nil); !errors.Is(err, ErrInvalidLink) {
		t.Errorf("Resolve(nil): got %v, want ErrInvalidLink", err)
	}
}

func TestPool_DefaultRouteOverride(t *testing.T) {
	p, err := NewPool(PoolConfig{
		Routes: []Route{
			{Alias: "b1", Token: "t1", ChatID: "-1"},
			{Alias: "b2", Token: "t2", ChatID: "-2"},
		},
		DefaultRoute: "b2",
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	def, _ := p.Client("")
	b2, _ := p.Client("b2")
	if def != b2 {
		t.Error("DefaultRoute=b2 should make empty alias resolve to b2")
	}
}

func TestRoundRobin_Cycles(t *testing.T) {
	rr := NewRoundRobin()
	aliases := []string{"a", "b", "c"}
	counts := map[string]int{}
	for i := 0; i < 9; i++ {
		counts[rr.Pick(aliases)]++
	}
	for _, a := range aliases {
		if counts[a] != 3 {
			t.Errorf("alias %q picked %d times, want 3 (counts=%v)", a, counts[a], counts)
		}
	}
}

func TestPool_PickSkipsCooldown(t *testing.T) {
	p := newTestPool(t, "b1", "b2")
	p.markCooldown("b1", time.Minute)
	for i := 0; i < 5; i++ {
		if got := p.pick(nil); got != "b2" {
			t.Fatalf("pick #%d: got %q, want b2 (b1 is cooling down)", i, got)
		}
	}
}

func TestPool_PickCooldownExpires(t *testing.T) {
	p := newTestPool(t, "b1", "b2")
	p.markCooldown("b1", 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	seen := map[string]bool{}
	for i := 0; i < 4; i++ {
		seen[p.pick(nil)] = true
	}
	if !seen["b1"] {
		t.Error("b1 should be pickable again after its cooldown expired")
	}
}

func TestPool_PickAllCoolingFallsBack(t *testing.T) {
	p := newTestPool(t, "b1", "b2")
	p.markCooldown("b1", time.Minute)
	p.markCooldown("b2", time.Minute)
	if got := p.pick(nil); got == "" {
		t.Error("pick must still return a route when everything is cooling down")
	}
}

func TestPool_MarkCooldownNeverShortens(t *testing.T) {
	p := newTestPool(t, "b1", "b2")
	p.markCooldown("b1", time.Minute)
	p.markCooldown("b1", time.Millisecond) // must not shorten the earlier cooldown
	if got := p.pick(nil); got != "b2" {
		t.Errorf("got %q, want b2 (b1 cooldown must not shrink)", got)
	}
}

func TestPool_UploadFailover_MovesToNextRoute(t *testing.T) {
	p := newTestPool(t, "b1", "b2")
	b1, _ := p.Client("b1")

	var attempts []string
	res, err := p.uploadFailover(context.Background(), func(_ context.Context, c *Client) (*UploadResult, error) {
		alias := poolAliasOf(p, c)
		attempts = append(attempts, alias)
		if c == b1 {
			return nil, &FloodWaitError{Seconds: 60}
		}
		return &UploadResult{FileID: "fid"}, nil
	})
	if err != nil {
		t.Fatalf("uploadFailover: %v", err)
	}
	if res.Route != "b2" {
		t.Errorf("Route: got %q, want b2", res.Route)
	}
	if len(attempts) != 2 || attempts[0] != "b1" || attempts[1] != "b2" {
		t.Errorf("attempts: got %v, want [b1 b2]", attempts)
	}
	// The flood-waited route must now be in cooldown.
	if got := p.pick(nil); got != "b2" {
		t.Errorf("post-failover pick: got %q, want b2 (b1 cooling down)", got)
	}
}

func TestPool_UploadFailover_StopsOnNonRetryable(t *testing.T) {
	p := newTestPool(t, "b1", "b2")
	var calls int
	_, err := p.uploadFailover(context.Background(), func(context.Context, *Client) (*UploadResult, error) {
		calls++
		return nil, &NonRetryableError{Method: "sendDocument", Reason: "rejected"}
	})
	if calls != 1 {
		t.Errorf("calls: got %d, want 1 (non-retryable must not fail over)", calls)
	}
	var nre *NonRetryableError
	if !errors.As(err, &nre) {
		t.Errorf("error chain lost NonRetryableError: %v", err)
	}
	if !strings.Contains(err.Error(), `route "b1"`) {
		t.Errorf("error should name the failing route, got: %v", err)
	}
}

func TestPool_UploadFailover_StopsOnPartialAndStampsRoute(t *testing.T) {
	p := newTestPool(t, "b1", "b2")
	partial := &PartialUploadError{
		Uploaded: 2,
		Total:    5,
		Link:     &FileLink{FileID: "fid0", MessageID: 10},
		Err:      errors.New("boom"),
	}
	var calls int
	_, err := p.uploadFailover(context.Background(), func(context.Context, *Client) (*UploadResult, error) {
		calls++
		return nil, partial
	})
	if calls != 1 {
		t.Errorf("calls: got %d, want 1 (partial upload must not fail over)", calls)
	}
	var pe *PartialUploadError
	if !errors.As(err, &pe) {
		t.Fatalf("error chain lost PartialUploadError: %v", err)
	}
	if pe.Link.Route != "b1" {
		t.Errorf("partial link route: got %q, want b1 (needed for cleanup)", pe.Link.Route)
	}
}

func TestPool_UploadFailover_ExhaustsAllRoutes(t *testing.T) {
	p := newTestPool(t, "b1", "b2", "b3")
	var calls int
	_, err := p.uploadFailover(context.Background(), func(context.Context, *Client) (*UploadResult, error) {
		calls++
		return nil, &APIError{Code: 502, Description: "bad gateway"}
	})
	if calls != 3 {
		t.Errorf("calls: got %d, want 3 (each route tried once)", calls)
	}
	if err == nil {
		t.Error("expected error after exhausting all routes")
	}
}

// poolAliasOf maps a client back to its alias (test helper).
func poolAliasOf(p *Pool, c *Client) string {
	for a, cl := range p.clients {
		if cl == c {
			return a
		}
	}
	return ""
}

func TestFailoverEligible(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"context canceled", context.Canceled, false},
		{"deadline exceeded", fmt.Errorf("wrap: %w", context.DeadlineExceeded), false},
		{"upload too large", fmt.Errorf("x: %w", ErrUploadTooLarge), false},
		{"non-retryable", &NonRetryableError{Reason: "x"}, false},
		{"partial upload", &PartialUploadError{Err: errors.New("x")}, false},
		{"api 400", &APIError{Code: 400}, false},
		{"api 403", &APIError{Code: 403}, false},
		{"api 429", &APIError{Code: 429}, true},
		{"api 500", &APIError{Code: 500}, true},
		{"flood wait", &FloodWaitError{Seconds: 5}, true},
		{"plain network error", errors.New("connection reset"), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := failoverEligible(tc.err); got != tc.want {
				t.Errorf("failoverEligible(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestFileLink_RouteRoundTrip(t *testing.T) {
	l := &FileLink{FileID: "abc", Route: "b2"}
	parsed, err := ParseURL(l.URL())
	if err != nil {
		t.Fatalf("ParseURL: %v", err)
	}
	if parsed.Route != "b2" {
		t.Errorf("Route: got %q, want b2", parsed.Route)
	}
}

func TestFileLink_EmptyRouteKeepsLegacyEncoding(t *testing.T) {
	// A link without a route must be byte-identical to pre-routing links.
	with := (&FileLink{FileID: "abc", Route: ""}).URL()
	without := (&FileLink{FileID: "abc"}).URL()
	if with != without {
		t.Errorf("empty route changed the encoding: %q vs %q", with, without)
	}
}

func TestUploadResult_LinkCarriesRoute(t *testing.T) {
	r := &UploadResult{FileID: "fid", MessageID: 1, ChatID: -1, Route: "b3"}
	parsed, err := ParseURL(r.Link())
	if err != nil {
		t.Fatalf("ParseURL: %v", err)
	}
	if parsed.Route != "b3" {
		t.Errorf("Route: got %q, want b3", parsed.Route)
	}
}

func TestPartialUploadError_Unwrap(t *testing.T) {
	inner := &FloodWaitError{Seconds: 7}
	err := error(&PartialUploadError{Uploaded: 1, Total: 3, Err: fmt.Errorf("chunk 2/3: %w", inner)})
	var fw *FloodWaitError
	if !errors.As(err, &fw) || fw.Seconds != 7 {
		t.Errorf("PartialUploadError must unwrap to the underlying cause, got %v", err)
	}
	if !strings.Contains(err.Error(), "1/3") {
		t.Errorf("Error() should mention progress, got %q", err.Error())
	}
}

func TestPartialChunkLink(t *testing.T) {
	first := &UploadResult{FileID: "fid0", FileUniqueID: "u0", MessageID: 100, ChatID: -1001}
	chunks := []ChunkRef{
		{Index: 0, FileID: "fid0", MessageID: 100, Size: 10},
		{Index: 1, FileID: "fid1", MessageID: 101, Size: 10},
	}
	l := partialChunkLink(first, chunks, "big.bin")
	all := l.AllChunks()
	if len(all) != 2 {
		t.Fatalf("AllChunks: got %d, want 2", len(all))
	}
	if all[1].MessageID != 101 {
		t.Errorf("chunk 1 message id: got %d, want 101", all[1].MessageID)
	}

	// Single uploaded chunk: falls back to top-level fields.
	l1 := partialChunkLink(first, chunks[:1], "big.bin")
	all1 := l1.AllChunks()
	if len(all1) != 1 || all1[0].MessageID != 100 {
		t.Errorf("single-chunk partial link wrong: %+v", all1)
	}
}
