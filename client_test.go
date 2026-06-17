package telconyx

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFileLink_EncodeDecode(t *testing.T) {
	original := &FileLink{
		FileID:       "AgACAgIAAxkBAAM",
		FileUniqueID: "AgADBAAM",
		MessageID:    12345,
		ChatID:       -1001234567890,
		Size:         1024 * 1024,
		MimeType:     "application/pdf",
		Name:         "test.pdf",
	}

	s := original.URL()
	if s == "" {
		t.Fatal("URL() returned empty string")
	}
	if !strings.HasPrefix(s, linkPrefix) {
		t.Fatalf("URL %q does not start with %q", s, linkPrefix)
	}

	parsed, err := ParseURL(s)
	if err != nil {
		t.Fatalf("ParseURL: %v", err)
	}
	if parsed.FileID != original.FileID {
		t.Errorf("FileID: got %q, want %q", parsed.FileID, original.FileID)
	}
	if parsed.FileUniqueID != original.FileUniqueID {
		t.Errorf("FileUniqueID: got %q, want %q", parsed.FileUniqueID, original.FileUniqueID)
	}
	if parsed.MessageID != original.MessageID {
		t.Errorf("MessageID: got %d, want %d", parsed.MessageID, original.MessageID)
	}
	if parsed.ChatID != original.ChatID {
		t.Errorf("ChatID: got %d, want %d", parsed.ChatID, original.ChatID)
	}
	if parsed.Size != original.Size {
		t.Errorf("Size: got %d, want %d", parsed.Size, original.Size)
	}
	if parsed.MimeType != original.MimeType {
		t.Errorf("MimeType: got %q, want %q", parsed.MimeType, original.MimeType)
	}
	if parsed.Name != original.Name {
		t.Errorf("Name: got %q, want %q", parsed.Name, original.Name)
	}
}

func TestFileLink_EncodeMinimal(t *testing.T) {
	minimal := &FileLink{FileID: "abc"}
	s := minimal.URL()
	if s == "" {
		t.Fatal("URL empty")
	}
	parsed, err := ParseURL(s)
	if err != nil {
		t.Fatalf("ParseURL: %v", err)
	}
	if parsed.FileID != "abc" {
		t.Errorf("FileID: got %q", parsed.FileID)
	}
}

func TestParseURL_Invalid(t *testing.T) {
	cases := []string{
		"",
		"not-a-link",
		"telconyx://file/",
		"telconyx://file/!!!not-valid-base64!!!",
		"telconyx://file/" + "AAAA", // valid base64 but invalid JSON
	}
	for _, c := range cases {
		if _, err := ParseURL(c); err == nil {
			t.Errorf("ParseURL(%q) expected error, got nil", c)
		}
	}
}

func TestFileLink_String(t *testing.T) {
	l := &FileLink{FileID: "x"}
	if got := l.String(); !strings.HasPrefix(got, linkPrefix) {
		t.Errorf("String() = %q, want prefix %q", got, linkPrefix)
	}
	var nilLink *FileLink
	if got := nilLink.String(); got != "" {
		t.Errorf("nil.String() = %q, want empty", got)
	}
}

func TestFileLink_ChunkedEncodeDecode(t *testing.T) {
	original := &FileLink{
		FileID:       "fid0",
		FileUniqueID: "unique0",
		MessageID:    100,
		ChatID:       -1001,
		Size:         100 * 1024 * 1024,
		MimeType:     "application/zip",
		Name:         "big.zip",
		ChunkSize:    49 * 1024 * 1024,
		ChunkCount:   3,
		Chunks: []ChunkRef{
			{Index: 0, FileID: "fid0", MessageID: 100, Size: 49 * 1024 * 1024},
			{Index: 1, FileID: "fid1", MessageID: 101, Size: 49 * 1024 * 1024},
			{Index: 2, FileID: "fid2", MessageID: 102, Size: 2 * 1024 * 1024},
		},
	}

	s := original.URL()
	parsed, err := ParseURL(s)
	if err != nil {
		t.Fatalf("ParseURL: %v", err)
	}
	if !parsed.IsChunked() {
		t.Errorf("expected IsChunked()=true")
	}
	if len(parsed.Chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(parsed.Chunks))
	}
	if parsed.ChunkSize != original.ChunkSize {
		t.Errorf("ChunkSize: got %d, want %d", parsed.ChunkSize, original.ChunkSize)
	}
	if parsed.ChunkCount != original.ChunkCount {
		t.Errorf("ChunkCount: got %d, want %d", parsed.ChunkCount, original.ChunkCount)
	}
	// file_ids must round-trip
	for i, ch := range original.Chunks {
		if parsed.Chunks[i].FileID != ch.FileID {
			t.Errorf("chunk %d FileID: got %q, want %q", i, parsed.Chunks[i].FileID, ch.FileID)
		}
	}
	// message_ids must round-trip too (required to delete every chunk)
	for i, ch := range original.Chunks {
		if parsed.Chunks[i].MessageID != ch.MessageID {
			t.Errorf("chunk %d MessageID: got %d, want %d", i, parsed.Chunks[i].MessageID, ch.MessageID)
		}
	}
}

func TestFileLink_AllChunks_Legacy(t *testing.T) {
	// Legacy FileLink without Chunks should still report 1 chunk via AllChunks().
	l := &FileLink{FileID: "f", MessageID: 7, Size: 100}
	cs := l.AllChunks()
	if len(cs) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(cs))
	}
	if cs[0].FileID != "f" || cs[0].MessageID != 7 || cs[0].Size != 100 {
		t.Errorf("synthesised chunk wrong: %+v", cs[0])
	}
	if l.IsChunked() {
		t.Error("legacy link should not report IsChunked=true")
	}
}

func TestUploadResult_Link(t *testing.T) {
	r := &UploadResult{
		FileID:    "fid",
		MessageID: 7,
		ChatID:    1,
	}
	got := r.Link()
	if !strings.HasPrefix(got, linkPrefix) {
		t.Errorf("Link() = %q, want prefix %q", got, linkPrefix)
	}
}

func TestUploadResult_ChunkedLink(t *testing.T) {
	r := &UploadResult{
		FileID:      "fid0",
		MessageID:   1,
		ChatID:      -100,
		Size:        100 * 1024 * 1024,
		Name:        "big.bin",
		ChunkSize:   49 * 1024 * 1024,
		ChunkCount:  3,
		Chunks:      []ChunkRef{{Index: 0, FileID: "fid0"}, {Index: 1, FileID: "fid1"}, {Index: 2, FileID: "fid2"}},
	}
	parsed, _ := ParseURL(r.Link())
	if !parsed.IsChunked() {
		t.Error("expected IsChunked=true")
	}
}

func TestBackoff(t *testing.T) {
	base := 100 * time.Millisecond
	max := 5 * time.Second
	for attempt := 0; attempt < 10; attempt++ {
		d := backoff(attempt, base, max)
		if d < base/2 {
			t.Errorf("attempt %d: delay %v < base/2", attempt, d)
		}
		if d > max {
			t.Errorf("attempt %d: delay %v > max", attempt, d)
		}
	}
}

func TestBackoff_CapsAtMax(t *testing.T) {
	base := 100 * time.Millisecond
	max := 1 * time.Second
	for attempt := 0; attempt < 30; attempt++ {
		d := backoff(attempt, base, max)
		if d > max {
			t.Errorf("attempt %d: delay %v > max %v", attempt, d, max)
		}
	}
}

func TestNewClient_RequiresFields(t *testing.T) {
	if _, err := NewClient(Config{}); err == nil {
		t.Error("expected error for empty config")
	}
	if _, err := NewClient(Config{Token: "x"}); err == nil {
		t.Error("expected error for missing ChatID")
	}
	if _, err := NewClient(Config{ChatID: "x"}); err == nil {
		t.Error("expected error for missing Token")
	}
	c, err := NewClient(Config{Token: "x", ChatID: "y"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c == nil {
		t.Fatal("client is nil")
	}
	// Defaults applied
	if c.Config().MaxUploadSize != DefaultMaxFileSize {
		t.Errorf("MaxUploadSize default: got %d, want %d", c.Config().MaxUploadSize, DefaultMaxFileSize)
	}
	if c.Config().MaxDownloadSize != DefaultMaxFileSize {
		t.Errorf("MaxDownloadSize default: got %d, want %d", c.Config().MaxDownloadSize, DefaultMaxFileSize)
	}
	if c.Config().ChunkSize != DefaultChunkSize {
		t.Errorf("ChunkSize default: got %d, want %d", c.Config().ChunkSize, DefaultChunkSize)
	}
	if c.Config().ChunkConcurrency != DefaultChunkConcurrency {
		t.Errorf("ChunkConcurrency default: got %d, want %d", c.Config().ChunkConcurrency, DefaultChunkConcurrency)
	}
}

func TestParseSize(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"100", 100},
		{"1KB", 1024},
		{"1MB", 1024 * 1024},
		{"49MB", 49 * 1024 * 1024},
		{"2GB", 2 * 1024 * 1024 * 1024},
		{"1MiB", 1024 * 1024},
		{"1.5MB", int64(1.5 * float64(1024*1024))},
		{"1B", 1},
		{"1K", 1024},
		{"  49MB  ", 49 * 1024 * 1024},
	}
	for _, c := range cases {
		got, err := ParseSize(c.in)
		if err != nil {
			t.Errorf("ParseSize(%q) error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseSize(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestParseSize_Invalid(t *testing.T) {
	cases := []string{"", "MB", "abc", "1XB", "MB1"}
	for _, c := range cases {
		if _, err := ParseSize(c); err == nil {
			t.Errorf("ParseSize(%q) expected error", c)
		}
	}
}

func TestNewClient_ClampsChunkSize(t *testing.T) {
	c, err := NewClient(Config{Token: "x", ChatID: "y", ChunkSize: 200 * 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	if c.Config().ChunkSize != MaxFileSize {
		t.Errorf("ChunkSize: got %d, want %d (clamped to MaxFileSize)", c.Config().ChunkSize, MaxFileSize)
	}
}

func TestFloodWaitError_Duration(t *testing.T) {
	cases := []struct {
		s        int
		expected time.Duration
	}{
		{0, 0},
		{5, 5 * time.Second},
		{120, 120 * time.Second},
	}
	for _, c := range cases {
		fw := &FloodWaitError{Seconds: c.s}
		if got := fw.Duration(); got != c.expected {
			t.Errorf("Seconds=%d: Duration()=%v, want %v", c.s, got, c.expected)
		}
	}
}

func TestParseGetFileResponse_Success(t *testing.T) {
	body := []byte(`{"ok":true,"result":{"file_id":"abc","file_path":"documents/file_12.pdf"}}`)
	path, err := parseGetFileResponse(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if path != "documents/file_12.pdf" {
		t.Errorf("got %q", path)
	}
}

func TestParseGetFileResponse_FloodWait(t *testing.T) {
	body := []byte(`{"ok":false,"error_code":429,"description":"Too Many Requests","parameters":{"retry_after":42}}`)
	_, err := parseGetFileResponse(body)
	fw, ok := err.(*FloodWaitError)
	if !ok {
		t.Fatalf("expected *FloodWaitError, got %T (%v)", err, err)
	}
	if fw.Seconds != 42 {
		t.Errorf("Seconds=%d, want 42", fw.Seconds)
	}
}

func TestParseGetFileResponse_NotOK(t *testing.T) {
	body := []byte(`{"ok":false,"error_code":400,"description":"Bad Request"}`)
	_, err := parseGetFileResponse(body)
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("expected *APIError, got %T (%v)", err, err)
	}
	if apiErr.Code != 400 {
		t.Errorf("Code=%d", apiErr.Code)
	}
}

// TestChunkRoundTrip simulates a chunked upload/download by writing a known
// pattern into a file, splitting into chunks, then reassembling and verifying
// the SHA-256 matches. It does not call Telegram.
func TestChunkRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	src := filepath.Join(tmpDir, "source.bin")

	// 5 MB of pseudo-random data, deterministic seed.
	const totalSize = 5 * 1024 * 1024
	rng := rand.New(rand.NewPCG(42, 99))
	original := make([]byte, totalSize)
	for i := range original {
		original[i] = byte(rng.Uint32() & 0xFF)
	}
	wantHash := sha256.Sum256(original)

	if err := os.WriteFile(src, original, 0o644); err != nil {
		t.Fatal(err)
	}

	// Split into 1 MB chunks (file is 5 MB → 5 chunks).
	const chunkSize int64 = 1 * 1024 * 1024
	chunkCount := int((totalSize + int(chunkSize) - 1) / int(chunkSize))

	// Write each chunk to its own temp file (simulating Telegram storing it).
	chunkFiles := make([]string, chunkCount)
	chunkFileIDs := make([]string, chunkCount)
	chunks := make([]ChunkRef, chunkCount)
	for i := 0; i < chunkCount; i++ {
		off := int64(i) * chunkSize
		size := chunkSize
		if off+size > int64(totalSize) {
			size = int64(totalSize) - off
		}
		cf := filepath.Join(tmpDir, "chunk_"+strconvI(i))
		data := original[off : off+size]
		if err := os.WriteFile(cf, data, 0o644); err != nil {
			t.Fatal(err)
		}
		chunkFiles[i] = cf
		chunkFileIDs[i] = "fake_file_id_" + strconvI(i)
		chunks[i] = ChunkRef{Index: i, FileID: chunkFileIDs[i], MessageID: 100 + i, Size: int(size)}
	}

	// Build FileLink as if from a real upload.
	link := &FileLink{
		FileID:       chunks[0].FileID,
		FileUniqueID: "unique",
		MessageID:    chunks[0].MessageID,
		ChatID:       -1001,
		Size:         totalSize,
		Name:         "source.bin",
		ChunkSize:    int(chunkSize),
		ChunkCount:   chunkCount,
		Chunks:       chunks,
	}
	if !link.IsChunked() {
		t.Fatal("expected IsChunked=true")
	}

	// Reassemble: read each chunk in order and write to dest.
	dest := filepath.Join(tmpDir, "restored.bin")
	out, err := os.Create(dest)
	if err != nil {
		t.Fatal(err)
	}
	for _, cf := range chunkFiles {
		b, err := os.ReadFile(cf)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := out.Write(b); err != nil {
			t.Fatal(err)
		}
	}
	out.Close()

	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	gotHash := sha256.Sum256(got)
	if !bytes.Equal(gotHash[:], wantHash[:]) {
		t.Errorf("reassembled hash mismatch: got %s, want %s", hex.EncodeToString(gotHash[:]), hex.EncodeToString(wantHash[:]))
	}
}

func strconvI(n int) string {
	// tiny int->string helper to avoid extra imports in this test file
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func TestNonRetryableError_StopsRetry(t *testing.T) {
	c, err := NewClient(Config{Token: "x", ChatID: "y", Retries: 5})
	if err != nil {
		t.Fatal(err)
	}
	var calls int
	err = c.withRetry(context.Background(), func(ctx context.Context) error {
		calls++
		return &NonRetryableError{Method: "sendDocument", Reason: "test"}
	})
	if calls != 1 {
		t.Errorf("expected 1 call (non-retryable), got %d", calls)
	}
	var nre *NonRetryableError
	if !errors.As(err, &nre) {
		t.Errorf("expected *NonRetryableError, got %T (%v)", err, err)
	}
}

func TestNonRetryableError_StopsRetryViaAs(t *testing.T) {
	// Same test but with a wrapped error, to confirm errors.As works.
	c, _ := NewClient(Config{Token: "x", ChatID: "y", Retries: 5})
	var calls int
	err := c.withRetry(context.Background(), func(ctx context.Context) error {
		calls++
		return fmt.Errorf("wrapped: %w", &NonRetryableError{Reason: "x"})
	})
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
	var nre *NonRetryableError
	if !errors.As(err, &nre) {
		t.Error("expected NonRetryableError to be unwrappable")
	}
}

func TestParseSendDocumentResponse_NoDocument(t *testing.T) {
	// Telegram returns a Message but without a document field (e.g. a warning
	// message about file size). This must be classified as non-retryable.
	body := []byte(`{
		"ok": true,
		"result": {
			"message_id": 99,
			"chat": {"id": -100, "type": "supergroup"},
			"text": "Sorry, the file is too big."
		}
	}`)
	var out *UploadResult
	err := parseSendDocumentResponse(body, UploadOpts{Name: "x.pdf"}, &out)
	var nre *NonRetryableError
	if !errors.As(err, &nre) {
		t.Fatalf("expected *NonRetryableError, got %T (%v)", err, err)
	}
	if nre.Method != "sendDocument" {
		t.Errorf("Method: got %q", nre.Method)
	}
	if !strings.Contains(nre.Detail, "file is too big") {
		t.Errorf("Detail should include server response, got %q", nre.Detail)
	}
}

func TestParseSendDocumentResponse_NonDocumentMedia(t *testing.T) {
	// We always call sendDocument, but Telegram reclassifies some files: a .gif
	// comes back as "animation", media files as "video"/"audio", etc. The file
	// IS stored and has a usable file_id, so this must succeed — not error.
	cases := []struct {
		name  string
		field string
	}{
		{"animation (gif)", "animation"},
		{"video", "video"},
		{"audio", "audio"},
		{"voice", "voice"},
		{"sticker", "sticker"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{
				"ok": true,
				"result": {
					"message_id": 42,
					"chat": {"id": -1001, "type": "supergroup"},
					"` + tc.field + `": {
						"file_id": "FID_123",
						"file_unique_id": "UNIQ_123",
						"mime_type": "application/octet-stream",
						"file_size": 2048
					}
				}
			}`)
			var out *UploadResult
			if err := parseSendDocumentResponse(body, UploadOpts{Name: "x.gif"}, &out); err != nil {
				t.Fatalf("expected success, got error: %v", err)
			}
			if out == nil {
				t.Fatal("expected a result, got nil")
			}
			if out.FileID != "FID_123" {
				t.Errorf("FileID: got %q, want FID_123", out.FileID)
			}
			if out.MessageID != 42 {
				t.Errorf("MessageID: got %d, want 42", out.MessageID)
			}
			if out.Size != 2048 {
				t.Errorf("Size: got %d, want 2048", out.Size)
			}
		})
	}
}

func TestParseSendDocumentResponse_Photo(t *testing.T) {
	// A photo field is an array of sizes; we should pick the largest (last).
	body := []byte(`{
		"ok": true,
		"result": {
			"message_id": 7,
			"chat": {"id": -1001, "type": "supergroup"},
			"photo": [
				{"file_id": "small", "file_unique_id": "us", "file_size": 100},
				{"file_id": "large", "file_unique_id": "ul", "file_size": 9000}
			]
		}
	}`)
	var out *UploadResult
	if err := parseSendDocumentResponse(body, UploadOpts{Name: "p.jpg"}, &out); err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if out.FileID != "large" {
		t.Errorf("FileID: got %q, want largest size 'large'", out.FileID)
	}
}

func TestParseGetFileResponse_EmptyPath(t *testing.T) {
	body := []byte(`{"ok":true,"result":{"file_id":"abc","file_path":""}}`)
	_, err := parseGetFileResponse(body)
	var nre *NonRetryableError
	if !errors.As(err, &nre) {
		t.Fatalf("expected *NonRetryableError, got %T (%v)", err, err)
	}
}

func TestTransport_PostForm_NotActuallyUsed(t *testing.T) {
	// sanity: ensure url.Values is importable from this package
	_ = url.Values{}
	_ = rand.Int64N
}
