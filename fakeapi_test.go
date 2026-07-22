package telconyx

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fakeTelegram is a minimal in-memory Bot API server for hermetic tests:
// it implements sendDocument, getFile, and file downloads, plus per-test
// fault injection.
type fakeTelegram struct {
	t   *testing.T
	srv *httptest.Server

	mu      sync.Mutex
	files   map[string][]byte // file_id -> stored bytes
	nextID  int
	nextMsg int

	// getHits counts file-content GETs per file_id.
	getHits map[string]int

	// cutFirstGets: for that many first content GETs (across all files),
	// serve only half the bytes with a full Content-Length, producing a
	// mid-stream failure on the client.
	cutFirstGets int
	totalGets    int

	// appendGarbage: serve the stored bytes plus this many extra bytes.
	appendGarbage int

	// floodTokens: sendDocument for these tokens answers 429 with a huge
	// retry_after.
	floodTokens map[string]bool
}

func newFakeTelegram(t *testing.T) *fakeTelegram {
	f := &fakeTelegram{
		t:           t,
		files:       map[string][]byte{},
		getHits:     map[string]int{},
		floodTokens: map[string]bool{},
	}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeTelegram) URL() string { return f.srv.URL }

func (f *fakeTelegram) handle(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch {
	case strings.HasPrefix(path, "/file/bot"):
		// /file/bot<token>/files/<id>
		id := path[strings.LastIndex(path, "/")+1:]
		f.serveFile(w, id)
	case strings.HasPrefix(path, "/bot"):
		rest := strings.TrimPrefix(path, "/bot")
		slash := strings.Index(rest, "/")
		if slash < 0 {
			http.NotFound(w, r)
			return
		}
		token, method := rest[:slash], rest[slash+1:]
		switch method {
		case "sendDocument":
			f.sendDocument(w, r, token)
		case "getFile":
			f.getFile(w, r)
		case "deleteMessage":
			fmt.Fprint(w, `{"ok":true,"result":true}`)
		default:
			http.NotFound(w, r)
		}
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeTelegram) sendDocument(w http.ResponseWriter, r *http.Request, token string) {
	f.mu.Lock()
	flooded := f.floodTokens[token]
	f.mu.Unlock()
	if flooded {
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprint(w, `{"ok":false,"error_code":429,"description":"Too Many Requests","parameters":{"retry_after":3600}}`)
		return
	}

	if err := r.ParseMultipartForm(64 << 20); err != nil {
		f.t.Errorf("fake sendDocument: parse multipart: %v", err)
		w.WriteHeader(400)
		return
	}
	file, hdr, err := r.FormFile("document")
	if err != nil {
		f.t.Errorf("fake sendDocument: missing document field: %v", err)
		w.WriteHeader(400)
		return
	}
	defer file.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(file); err != nil {
		f.t.Errorf("fake sendDocument: read: %v", err)
		w.WriteHeader(500)
		return
	}

	f.mu.Lock()
	f.nextID++
	f.nextMsg++
	id := "fid" + strconv.Itoa(f.nextID)
	msg := f.nextMsg
	f.files[id] = buf.Bytes()
	f.mu.Unlock()

	resp := fmt.Sprintf(`{"ok":true,"result":{"message_id":%d,"chat":{"id":-100500},"document":{"file_id":%q,"file_unique_id":"u-%s","file_name":%q,"file_size":%d}}}`,
		msg, id, id, hdr.Filename, buf.Len())
	fmt.Fprint(w, resp)
}

func (f *fakeTelegram) getFile(w http.ResponseWriter, r *http.Request) {
	id := r.FormValue("file_id")
	f.mu.Lock()
	_, ok := f.files[id]
	f.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"ok":false,"error_code":400,"description":"Bad Request: file not found"}`)
		return
	}
	resp := map[string]any{"ok": true, "result": map[string]any{"file_id": id, "file_path": "files/" + id}}
	_ = json.NewEncoder(w).Encode(resp)
}

func (f *fakeTelegram) serveFile(w http.ResponseWriter, id string) {
	f.mu.Lock()
	data, ok := f.files[id]
	f.getHits[id]++
	f.totalGets++
	cut := f.totalGets <= f.cutFirstGets
	garbage := f.appendGarbage
	f.mu.Unlock()
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if cut {
		// Announce the full length but send only half: the client observes
		// an unexpected EOF mid-body.
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(200)
		_, _ = w.Write(data[:len(data)/2])
		return
	}
	if garbage > 0 {
		data = append(append([]byte{}, data...), bytes.Repeat([]byte{0xEE}, garbage)...)
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(200)
	_, _ = w.Write(data)
}

func fakeClient(t *testing.T, f *fakeTelegram, mut func(*Config)) *Client {
	t.Helper()
	cfg := Config{Token: "tok", ChatID: "-100500", APIBase: f.URL()}
	if mut != nil {
		mut(&cfg)
	}
	c, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// TestFakeAPI_ChunkedRoundTrip uploads a file split into many chunks and
// downloads it back through both the file path (parallel) and the writer
// path (sequential), verifying byte-for-byte integrity.
func TestFakeAPI_ChunkedRoundTrip(t *testing.T) {
	fake := newFakeTelegram(t)
	c := fakeClient(t, fake, func(cfg *Config) { cfg.ChunkSize = 1024 })

	const totalSize = 5*1024 + 137 // 6 chunks, last one partial
	rng := rand.New(rand.NewPCG(7, 7))
	original := make([]byte, totalSize)
	for i := range original {
		original[i] = byte(rng.Uint32())
	}
	wantHash := sha256.Sum256(original)

	dir := t.TempDir()
	src := filepath.Join(dir, "src.bin")
	if err := os.WriteFile(src, original, 0o644); err != nil {
		t.Fatal(err)
	}

	res, err := c.UploadFile(context.Background(), src)
	if err != nil {
		t.Fatalf("UploadFile: %v", err)
	}
	if res.ChunkCount != 6 {
		t.Errorf("ChunkCount: got %d, want 6", res.ChunkCount)
	}

	link, err := ParseURL(res.Link())
	if err != nil {
		t.Fatalf("ParseURL: %v", err)
	}

	// File path (parallel reassembly via WriteAt).
	dest := filepath.Join(dir, "dest.bin")
	n, err := c.Download(context.Background(), link, dest)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if n != totalSize {
		t.Errorf("Download bytes: got %d, want %d", n, totalSize)
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(got) != wantHash {
		t.Error("file-path download corrupted the data")
	}

	// Writer path (sequential streaming).
	var buf bytes.Buffer
	n, err = c.DownloadTo(context.Background(), link, &buf)
	if err != nil {
		t.Fatalf("DownloadTo: %v", err)
	}
	if n != totalSize || sha256.Sum256(buf.Bytes()) != wantHash {
		t.Errorf("writer-path download corrupted the data (n=%d)", n)
	}
}

// TestFakeAPI_FileDownloadRetriesAfterMidStreamCut proves the K2 fix for the
// file path: a mid-stream failure is retried from scratch and the final file
// is intact (previously the partial bytes stayed in front of the retry).
func TestFakeAPI_FileDownloadRetriesAfterMidStreamCut(t *testing.T) {
	fake := newFakeTelegram(t)
	c := fakeClient(t, fake, nil)

	original := bytes.Repeat([]byte("telconyx!"), 1000)
	dir := t.TempDir()
	src := filepath.Join(dir, "one.bin")
	if err := os.WriteFile(src, original, 0o644); err != nil {
		t.Fatal(err)
	}
	res, err := c.UploadFile(context.Background(), src)
	if err != nil {
		t.Fatal(err)
	}
	link, _ := ParseURL(res.Link())

	fake.mu.Lock()
	fake.cutFirstGets = 1 // first content GET dies halfway
	fake.mu.Unlock()

	dest := filepath.Join(dir, "restored.bin")
	n, err := c.Download(context.Background(), link, dest)
	if err != nil {
		t.Fatalf("Download should succeed on retry, got: %v", err)
	}
	got, _ := os.ReadFile(dest)
	if n != int64(len(original)) || !bytes.Equal(got, original) {
		t.Errorf("retry left a corrupt file: n=%d len=%d", n, len(got))
	}
	fake.mu.Lock()
	hits := fake.totalGets
	fake.mu.Unlock()
	if hits != 2 {
		t.Errorf("content GETs: got %d, want 2 (one failed + one retry)", hits)
	}
}

// TestFakeAPI_WriterDownloadNeverRetriesAfterPartialWrite proves the K2 fix
// for the writer path: once bytes reached the writer, a failure must NOT be
// retried (that would duplicate data), and the error must surface.
func TestFakeAPI_WriterDownloadNeverRetriesAfterPartialWrite(t *testing.T) {
	fake := newFakeTelegram(t)
	c := fakeClient(t, fake, nil)

	original := bytes.Repeat([]byte("stream"), 2000)
	dir := t.TempDir()
	src := filepath.Join(dir, "w.bin")
	if err := os.WriteFile(src, original, 0o644); err != nil {
		t.Fatal(err)
	}
	res, err := c.UploadFile(context.Background(), src)
	if err != nil {
		t.Fatal(err)
	}
	link, _ := ParseURL(res.Link())

	fake.mu.Lock()
	fake.cutFirstGets = 1000 // every content GET dies halfway
	fake.mu.Unlock()

	var buf bytes.Buffer
	_, err = c.DownloadTo(context.Background(), link, &buf)
	if err == nil {
		t.Fatal("expected an error from the cut stream")
	}
	if buf.Len() == 0 || buf.Len() >= len(original) {
		t.Errorf("partial bytes: got %d, want >0 and < %d", buf.Len(), len(original))
	}
	fake.mu.Lock()
	hits := fake.totalGets
	fake.mu.Unlock()
	if hits != 1 {
		t.Errorf("content GETs: got %d, want exactly 1 (no retry after partial write)", hits)
	}
}

// TestFakeAPI_OversizeBodyRejected proves the S4 fix: a body larger than the
// hard limit is rejected instead of ballooning memory or the output.
func TestFakeAPI_OversizeBodyRejected(t *testing.T) {
	fake := newFakeTelegram(t)
	c := fakeClient(t, fake, func(cfg *Config) { cfg.MaxDownloadSize = 4096 })

	original := bytes.Repeat([]byte("x"), 1000)
	dir := t.TempDir()
	src := filepath.Join(dir, "o.bin")
	if err := os.WriteFile(src, original, 0o644); err != nil {
		t.Fatal(err)
	}
	res, err := c.UploadFile(context.Background(), src)
	if err != nil {
		t.Fatal(err)
	}
	link, _ := ParseURL(res.Link())

	fake.mu.Lock()
	fake.appendGarbage = 8192 // server suddenly serves way more than stored
	fake.mu.Unlock()

	var buf bytes.Buffer
	_, err = c.DownloadTo(context.Background(), link, &buf)
	if err == nil || !strings.Contains(err.Error(), "maximum expected size") {
		t.Fatalf("expected oversize rejection, got: %v", err)
	}
	if int64(buf.Len()) > 4096 {
		t.Errorf("wrote %d bytes despite the 4096 limit", buf.Len())
	}
}

// TestFakeAPI_IdlePoolRotatesRoutes proves the least-inflight default picker
// degrades to round robin when idle: sequential uploads alternate routes
// instead of sticking to the first one.
func TestFakeAPI_IdlePoolRotatesRoutes(t *testing.T) {
	fake := newFakeTelegram(t)
	p, err := NewPool(PoolConfig{
		Routes: []Route{
			{Alias: "b1", Token: "t1", ChatID: "-100500"},
			{Alias: "b2", Token: "t2", ChatID: "-100500"},
		},
		Base: Config{APIBase: fake.URL()},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)

	var routes []string
	for i := 0; i < 4; i++ {
		res, err := p.UploadReader(context.Background(), strings.NewReader("rotate"), UploadOpts{Name: "r.txt"})
		if err != nil {
			t.Fatalf("upload %d: %v", i, err)
		}
		routes = append(routes, res.Route)
	}
	want := []string{"b1", "b2", "b1", "b2"}
	for i := range want {
		if routes[i] != want[i] {
			t.Fatalf("upload routes: got %v, want %v", routes, want)
		}
	}
}

// TestFakeAPI_PoolFailsOverOnLongFloodWait proves S5 end to end: a route
// answering 429 with a huge retry_after is abandoned immediately, the pool
// reroutes the upload, and the hot route is left cooling down.
func TestFakeAPI_PoolFailsOverOnLongFloodWait(t *testing.T) {
	fake := newFakeTelegram(t)
	fake.mu.Lock()
	fake.floodTokens["hot"] = true
	fake.mu.Unlock()

	p, err := NewPool(PoolConfig{
		Routes: []Route{
			{Alias: "b1", Token: "hot", ChatID: "-100500"},
			{Alias: "b2", Token: "cold", ChatID: "-100500"},
		},
		Base: Config{APIBase: fake.URL()},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)

	res, err := p.UploadReader(context.Background(), strings.NewReader("failover me"), UploadOpts{Name: "x.txt"})
	if err != nil {
		t.Fatalf("UploadReader: %v", err)
	}
	if res.Route != "b2" {
		t.Errorf("Route: got %q, want b2 (b1 is flooded)", res.Route)
	}
	if got := p.pick(nil); got != "b2" {
		t.Errorf("pick after failover: got %q, want b2 (b1 cooling down)", got)
	}
}
