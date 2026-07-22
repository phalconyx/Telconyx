package telconyx

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"
)

func rawLink(payload string) string {
	return linkPrefix + base64.RawURLEncoding.EncodeToString([]byte(payload))
}

func TestParseURL_CorruptChunkMetadata(t *testing.T) {
	cases := map[string]string{
		"ck shorter than cc": `{"f":"x","cc":3,"ck":["a","b"]}`,
		"ck longer than cc":  `{"f":"x","cc":2,"ck":["a","b","c"]}`,
		"cm length mismatch": `{"f":"x","cc":2,"ck":["a","b"],"cm":[1]}`,
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseURL(rawLink(payload)); !errors.Is(err, ErrInvalidLink) {
				t.Errorf("expected ErrInvalidLink, got %v", err)
			}
		})
	}
}

func TestParseURL_LegacyChunkedWithoutMessageIDs(t *testing.T) {
	// Old links carry no CM array; the first chunk falls back to M.
	l, err := ParseURL(rawLink(`{"f":"a","m":9,"cc":2,"ck":["a","b"],"cs":5,"s":10}`))
	if err != nil {
		t.Fatalf("ParseURL: %v", err)
	}
	if !l.IsChunked() || l.Chunks[0].MessageID != 9 {
		t.Errorf("legacy chunked link parsed wrong: %+v", l.Chunks)
	}
}

func TestNonRetryableError_UnwrapsCause(t *testing.T) {
	cause := &APIError{Code: 502, Description: "bad gateway"}
	err := error(&NonRetryableError{Method: "download", Reason: "cannot resume", Err: cause})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Code != 502 {
		t.Errorf("NonRetryableError must unwrap to its cause, got %v", err)
	}
	if got := err.Error(); got == "" || !errors.Is(err, err) {
		t.Errorf("Error() broken: %q", got)
	}
}

func TestWithRetry_LongFloodWaitSurfaces(t *testing.T) {
	c, err := NewClient(Config{Token: "x", ChatID: "y", Retries: 5, BackoffMax: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	var calls int
	start := time.Now()
	rerr := c.withRetry(context.Background(), func(ctx context.Context) error {
		calls++
		return &FloodWaitError{Seconds: 3600}
	})
	if calls != 1 {
		t.Errorf("calls: got %d, want 1 (long flood-wait must not sleep in place)", calls)
	}
	var fw *FloodWaitError
	if !errors.As(rerr, &fw) || fw.Seconds != 3600 {
		t.Errorf("expected the FloodWaitError to surface, got %v", rerr)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("withRetry slept %v for a 3600s flood-wait", elapsed)
	}
}

func TestWithRetry_ShortFloodWaitStillRetries(t *testing.T) {
	c, err := NewClient(Config{Token: "x", ChatID: "y", Retries: 2, BackoffMax: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	var calls int
	_ = c.withRetry(context.Background(), func(ctx context.Context) error {
		calls++
		return &FloodWaitError{Seconds: 0} // zero wait: retry immediately
	})
	if calls != 2 {
		t.Errorf("calls: got %d, want 2 (short flood-wait is honoured in place)", calls)
	}
}

func TestCheckDownloadable(t *testing.T) {
	c, err := NewClient(Config{Token: "x", ChatID: "y", MaxDownloadSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.checkDownloadable(nil); !errors.Is(err, ErrInvalidLink) {
		t.Errorf("nil link: got %v", err)
	}
	if err := c.checkDownloadable(&FileLink{}); !errors.Is(err, ErrInvalidLink) {
		t.Errorf("missing file_id: got %v", err)
	}
	if err := c.checkDownloadable(&FileLink{FileID: "f", Size: 200}); !errors.Is(err, ErrDownloadTooLarge) {
		t.Errorf("oversize claim: got %v", err)
	}
	chunkedNoSize := &FileLink{
		FileID: "f", Size: 50, ChunkCount: 2,
		Chunks: []ChunkRef{{Index: 0, FileID: "f"}, {Index: 1, FileID: "g"}},
	}
	if err := c.checkDownloadable(chunkedNoSize); !errors.Is(err, ErrInvalidLink) {
		t.Errorf("chunked without chunk_size: got %v", err)
	}
	ok := &FileLink{FileID: "f", Size: 50}
	if err := c.checkDownloadable(ok); err != nil {
		t.Errorf("valid link rejected: %v", err)
	}
}

func TestDeleteChunks_RefusesForeignChat(t *testing.T) {
	c, err := NewClient(Config{Token: "x", ChatID: "-100200"})
	if err != nil {
		t.Fatal(err)
	}
	link := &FileLink{FileID: "f", MessageID: 5, ChatID: -100999}
	derr := c.DeleteChunks(context.Background(), link)
	if derr == nil {
		t.Fatal("expected refusal for a link from another chat (message ids are chat-specific)")
	}
	if want := "refusing to delete"; !strings.Contains(derr.Error(), want) {
		t.Errorf("error %q should contain %q", derr, want)
	}
}
