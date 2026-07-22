package telconyx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"sync"
)

// Download downloads a file from Telegram and saves it to dest.
// It overwrites dest if it already exists. Chunked files are reassembled
// in parallel using up to ChunkConcurrency workers. Retries are safe: the
// destination is rewritten from scratch (single files) or per-chunk at fixed
// offsets (chunked files), so a mid-stream failure never corrupts the result.
func (c *Client) Download(ctx context.Context, link *FileLink, dest string) (int64, error) {
	if err := c.checkDownloadable(link); err != nil {
		return 0, err
	}
	chunks := link.AllChunks()
	if len(chunks) > 1 {
		return c.downloadChunkedToFile(ctx, link, chunks, dest)
	}
	return c.downloadSingleToFile(ctx, link, dest)
}

// DownloadTo streams the file content to w. It returns the number of bytes written.
// For chunked files, chunks are downloaded sequentially (since random access on
// a plain io.Writer is not generally possible).
//
// Because bytes already written to w cannot be taken back, a failure after the
// first byte of a chunk is NOT retried — retrying would append a second copy
// of the data. Callers that can rewind should prefer Download (file variant),
// which retries fully.
func (c *Client) DownloadTo(ctx context.Context, link *FileLink, w io.Writer) (int64, error) {
	if err := c.checkDownloadable(link); err != nil {
		return 0, err
	}
	chunks := link.AllChunks()
	if len(chunks) > 1 {
		return c.downloadChunkedToWriter(ctx, link, chunks, w)
	}
	return c.downloadSingleToWriter(ctx, link, w)
}

// checkDownloadable validates a link before any network call.
func (c *Client) checkDownloadable(link *FileLink) error {
	if link == nil {
		return ErrInvalidLink
	}
	if link.FileID == "" {
		return fmt.Errorf("%w: missing file_id", ErrInvalidLink)
	}
	if int64(link.Size) > c.cfg.MaxDownloadSize {
		return fmt.Errorf("telconyx: file size %d exceeds MaxDownloadSize %d: %w",
			link.Size, c.cfg.MaxDownloadSize, ErrDownloadTooLarge)
	}
	// Chunk offsets and per-chunk bounds derive from ChunkSize; a chunked
	// link without it (e.g. a partial-upload cleanup link) cannot be
	// reassembled.
	if link.IsChunked() && link.ChunkSize <= 0 {
		return fmt.Errorf("%w: chunked link without chunk_size", ErrInvalidLink)
	}
	return nil
}

func (c *Client) downloadSingleToFile(ctx context.Context, link *FileLink, dest string) (int64, error) {
	f, err := os.Create(dest)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var total int64
	err = c.withRetry(ctx, func(ctx context.Context) error {
		// Reset between attempts so a mid-stream failure never leaves partial
		// bytes in front of the retried copy.
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if err := f.Truncate(0); err != nil {
			return err
		}
		n, err := c.downloadChunkBytes(ctx, link.FileID, c.cfg.MaxDownloadSize, f)
		total = n
		return err
	})
	if err != nil {
		return 0, err
	}
	if link.Size > 0 && total != int64(link.Size) {
		return total, fmt.Errorf("telconyx: downloaded %d bytes but link declares %d", total, link.Size)
	}
	return total, nil
}

func (c *Client) downloadSingleToWriter(ctx context.Context, link *FileLink, w io.Writer) (int64, error) {
	var total int64
	err := c.withRetry(ctx, func(ctx context.Context) error {
		n, err := c.downloadChunkBytes(ctx, link.FileID, c.cfg.MaxDownloadSize, w)
		total = n
		if err != nil && n > 0 {
			// Bytes already reached w and cannot be unwritten; a retry would
			// append a second copy behind the partial data.
			return &NonRetryableError{
				Method: "download",
				Reason: fmt.Sprintf("stream failed after %d bytes; cannot resume on a plain writer", n),
				Err:    err,
			}
		}
		return err
	})
	if err != nil {
		return total, err
	}
	if link.Size > 0 && total != int64(link.Size) {
		return total, fmt.Errorf("telconyx: downloaded %d bytes but link declares %d", total, link.Size)
	}
	return total, nil
}

// downloadChunkedToFile downloads all chunks in parallel, each streamed
// directly to its fixed offset in dest (no chunk-sized buffers). A failing
// chunk cancels the remaining downloads.
func (c *Client) downloadChunkedToFile(ctx context.Context, link *FileLink, chunks []ChunkRef, dest string) (int64, error) {
	f, err := os.Create(dest)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	// Pre-allocate to avoid sparse-file issues and disk fragmentation.
	if link.Size > 0 {
		if err := f.Truncate(int64(link.Size)); err != nil {
			return 0, err
		}
	}

	concurrency := c.cfg.ChunkConcurrency
	if concurrency <= 0 {
		concurrency = DefaultChunkConcurrency
	}
	if concurrency > len(chunks) {
		concurrency = len(chunks)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan ChunkRef, len(chunks))
	for _, ch := range chunks {
		jobs <- ch
	}
	close(jobs)

	type chunkResult struct {
		idx int
		n   int64
		err error
	}
	// Exactly one result per chunk, so the buffer can never fill up and
	// block a worker (which would deadlock wg.Wait below).
	results := make(chan chunkResult, len(chunks))
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ch := range jobs {
				n, err := c.downloadChunkAt(ctx, ch, f, int64(link.ChunkSize))
				results <- chunkResult{idx: ch.Index, n: n, err: err}
				if err != nil {
					cancel() // stop the other workers; their result is moot
				}
			}
		}()
	}

	wg.Wait()
	close(results)

	var firstErr error
	var total int64
	for r := range results {
		total += r.n
		if r.err == nil {
			continue
		}
		wrapped := fmt.Errorf("chunk %d: %w", r.idx, r.err)
		// Prefer the root-cause error over follow-on cancellations.
		if firstErr == nil || (errors.Is(firstErr, context.Canceled) && !errors.Is(r.err, context.Canceled)) {
			firstErr = wrapped
		}
	}
	if firstErr != nil {
		return 0, firstErr
	}
	if link.Size > 0 && total != int64(link.Size) {
		return total, fmt.Errorf("telconyx: reassembled %d bytes but link declares %d", total, link.Size)
	}
	return total, nil
}

// downloadChunkAt streams one chunk to its offset in f, with retries.
// WriteAt-based writes make retries safe: every attempt rewrites the same
// fixed region from its start.
func (c *Client) downloadChunkAt(ctx context.Context, ch ChunkRef, f *os.File, chunkSize int64) (int64, error) {
	offset := int64(ch.Index) * chunkSize
	var n int64
	err := c.withRetry(ctx, func(ctx context.Context) error {
		m, err := c.downloadChunkBytes(ctx, ch.FileID, chunkSize, io.NewOffsetWriter(f, offset))
		n = m
		return err
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// downloadChunkedToWriter streams chunks sequentially to w.
// This is slower than the parallel file variant but works with any io.Writer.
func (c *Client) downloadChunkedToWriter(ctx context.Context, link *FileLink, chunks []ChunkRef, w io.Writer) (int64, error) {
	var total int64
	for _, ch := range chunks {
		var chunkN int64
		err := c.withRetry(ctx, func(ctx context.Context) error {
			n, err := c.downloadChunkBytes(ctx, ch.FileID, int64(link.ChunkSize), w)
			chunkN = n
			if err != nil && n > 0 {
				// See DownloadTo: bytes already reached w, resuming is unsafe.
				return &NonRetryableError{
					Method: "download",
					Reason: fmt.Sprintf("stream failed inside chunk %d after %d bytes; cannot resume on a plain writer", ch.Index, n),
					Err:    err,
				}
			}
			return err
		})
		total += chunkN
		if err != nil {
			return total, fmt.Errorf("chunk %d: %w", ch.Index, err)
		}
	}
	if link.Size > 0 && total != int64(link.Size) {
		return total, fmt.Errorf("telconyx: streamed %d bytes but link declares %d", total, link.Size)
	}
	return total, nil
}

// downloadChunkBytes downloads a single chunk and writes its content to w.
// limit is a hard upper bound on the chunk size (links are untrusted input,
// so the claimed sizes are never used as the bound): receiving more than
// limit bytes is a non-retryable error. Returns the number of bytes written.
// Retry policy is decided by the caller.
func (c *Client) downloadChunkBytes(ctx context.Context, fileID string, limit int64, w io.Writer) (int64, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("telconyx: internal: non-positive download limit %d", limit)
	}
	params := url.Values{}
	params.Set("file_id", fileID)

	resp, err := c.tp.PostForm(ctx, "getFile", params)
	if err != nil {
		return 0, err
	}
	filePath, err := parseGetFileResponse(resp.Body)
	if err != nil {
		return 0, err
	}

	sr, err := c.tp.GetStream(ctx, c.tp.FileURL(filePath))
	if err != nil {
		return 0, err
	}
	if sr.StatusCode != 200 {
		b, _ := io.ReadAll(io.LimitReader(sr.Body, 4096))
		_ = sr.Body.Close()
		return 0, &APIError{
			Code:        sr.StatusCode,
			Description: fmt.Sprintf("download failed: %s", truncate(string(b), 256)),
			Method:      "getFile",
		}
	}
	defer sr.Body.Close()

	n, err := io.CopyN(w, sr.Body, limit)
	if err == io.EOF {
		return n, nil // body ended within the limit — the normal case
	}
	if err != nil {
		return n, err
	}
	// Exactly limit bytes were copied; anything further means the server is
	// sending more than this chunk may hold.
	var probe [1]byte
	if _, perr := io.ReadFull(sr.Body, probe[:]); perr != io.EOF {
		if perr != nil {
			return n, perr
		}
		return n, &NonRetryableError{
			Method: "download",
			Reason: fmt.Sprintf("chunk exceeds the maximum expected size %d", limit),
		}
	}
	return n, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
