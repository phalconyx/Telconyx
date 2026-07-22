package telconyx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
)

// UploadOpts configures a single upload.
type UploadOpts struct {
	// Name is the filename to display in Telegram.
	Name string
	// Caption is an optional caption that becomes the message caption.
	Caption string
	// MimeType is an optional MIME type override. If empty, it's inferred from Name.
	MimeType string
}

// UploadResult is the result of a successful upload.
// For files larger than ChunkSize, the file is split into multiple chunks
// and Chunks/ChunkSize/ChunkCount are populated.
type UploadResult struct {
	FileID       string `json:"file_id"`
	FileUniqueID string `json:"file_unique_id"`
	MessageID    int    `json:"message_id"`
	ChatID       int64  `json:"chat_id"`
	Size         int    `json:"size"`
	MimeType     string `json:"mime_type,omitempty"`
	Name         string `json:"name,omitempty"`

	// Route is the alias of the Pool route that performed the upload.
	// Empty for uploads through a plain single-bot Client.
	Route string `json:"route,omitempty"`

	// Chunking (only present if the file was split into multiple chunks).
	ChunkSize  int        `json:"chunk_size,omitempty"`
	ChunkCount int        `json:"chunk_count,omitempty"`
	Chunks     []ChunkRef `json:"chunks,omitempty"`
}

// Link returns the portable telconyx:// URL for this upload.
func (r *UploadResult) Link() string {
	if r == nil {
		return ""
	}
	fl := &FileLink{
		FileID:       r.FileID,
		FileUniqueID: r.FileUniqueID,
		MessageID:    r.MessageID,
		ChatID:       r.ChatID,
		Size:         r.Size,
		MimeType:     r.MimeType,
		Name:         r.Name,
		Route:        r.Route,
		ChunkSize:    r.ChunkSize,
		ChunkCount:   r.ChunkCount,
		Chunks:       r.Chunks,
	}
	return fl.URL()
}

// UploadFile uploads a local file. If the file is larger than the configured
// ChunkSize, it is split into multiple chunks and reassembled transparently
// on download. The file is never fully buffered in memory; chunks are read
// sequentially from disk.
func (c *Client) UploadFile(ctx context.Context, path string) (*UploadResult, error) {
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
	return c.UploadFileHandle(ctx, f, info.Size(), opts)
}

// UploadFileHandle uploads from an open *os.File with a known size.
// Supports chunked uploads for files larger than ChunkSize.
// The caller is responsible for closing f.
func (c *Client) UploadFileHandle(ctx context.Context, f *os.File, size int64, opts UploadOpts) (*UploadResult, error) {
	if size > c.cfg.MaxUploadSize {
		return nil, fmt.Errorf("telconyx: file size %d exceeds MaxUploadSize %d: %w",
			size, c.cfg.MaxUploadSize, ErrUploadTooLarge)
	}
	if size > c.cfg.ChunkSize {
		return c.uploadChunked(ctx, f, size, c.cfg.ChunkSize, opts)
	}
	return c.uploadSingle(ctx, f, size, opts)
}

// uploadSingle sends the file as a single Telegram message.
func (c *Client) uploadSingle(ctx context.Context, f *os.File, size int64, opts UploadOpts) (*UploadResult, error) {
	if size > MaxFileSize {
		return nil, fmt.Errorf("%w: %d > %d", ErrFileTooBig, size, MaxFileSize)
	}
	var result *UploadResult
	err := c.withRetry(ctx, func(ctx context.Context) error {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return err
		}
		return c.uploadOnce(ctx, f, opts, &result)
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// uploadChunked splits a file into chunks of chunkSize bytes and uploads
// each chunk as a separate Telegram message. The first chunk's metadata is
// duplicated at the top level of the returned UploadResult.
func (c *Client) uploadChunked(ctx context.Context, f *os.File, totalSize, chunkSize int64, opts UploadOpts) (*UploadResult, error) {
	if chunkSize <= 0 || chunkSize > MaxFileSize {
		return nil, fmt.Errorf("%w: chunk size %d invalid", ErrFileTooBig, chunkSize)
	}
	chunkCount := int((totalSize + chunkSize - 1) / chunkSize)
	if chunkCount < 2 {
		return c.uploadSingle(ctx, f, totalSize, opts)
	}

	chunks := make([]ChunkRef, 0, chunkCount)
	var firstResult *UploadResult

	for i := 0; i < chunkCount; i++ {
		offset := int64(i) * chunkSize
		size := chunkSize
		if offset+size > totalSize {
			size = totalSize - offset
		}
		if size > MaxFileSize {
			return nil, fmt.Errorf("%w: chunk %d size %d", ErrFileTooBig, i, size)
		}

		// Label every chunk consistently and 1-based so the filename matches the
		// caption ("part 2/3" → ".part2of3"). Note this is cosmetic only: the
		// display name in Telegram has no effect on download — reassembly uses
		// each chunk's index and file_id from the link, and the reassembled file
		// keeps the original name (opts.Name), set on the result below.
		chunkOpts := opts
		chunkOpts.Name = fmt.Sprintf("%s.part%dof%d", opts.Name, i+1, chunkCount)
		chunkOpts.Caption = fmt.Sprintf("telconyx: %s (part %d/%d)", opts.Name, i+1, chunkCount)

		// Stream the slice straight from the file — no chunk-sized buffer.
		// NewSectionReader returns EOF after `size` bytes, and a fresh reader
		// is created per attempt so retries restart from the slice start.
		result, err := c.uploadWithRetry(ctx, func() (io.Reader, error) {
			return io.NewSectionReader(f, offset, size), nil
		}, chunkOpts)
		if err != nil {
			err = fmt.Errorf("telconyx: upload chunk %d/%d: %w", i+1, chunkCount, err)
			if len(chunks) > 0 {
				return nil, &PartialUploadError{
					Uploaded: len(chunks),
					Total:    chunkCount,
					Link:     partialChunkLink(firstResult, chunks, opts.Name, chunkSize),
					Err:      err,
				}
			}
			return nil, err
		}
		if i == 0 {
			firstResult = result
		}
		chunks = append(chunks, ChunkRef{
			Index:     i,
			FileID:    result.FileID,
			MessageID: result.MessageID,
			Size:      result.Size,
		})
	}

	// Build the final result, overlaying the chunk info on the first chunk.
	res := *firstResult
	res.Size = int(totalSize)
	res.Name = opts.Name
	res.ChunkSize = int(chunkSize)
	res.ChunkCount = chunkCount
	res.Chunks = chunks
	return &res, nil
}

// partialChunkLink builds a FileLink referencing only the chunks that were
// uploaded before a failure, so they can be removed with DeleteChunks.
func partialChunkLink(first *UploadResult, chunks []ChunkRef, name string, chunkSize int64) *FileLink {
	l := &FileLink{
		FileID:       first.FileID,
		FileUniqueID: first.FileUniqueID,
		MessageID:    first.MessageID,
		ChatID:       first.ChatID,
		Size:         chunks[0].Size,
		Name:         name,
	}
	if len(chunks) > 1 {
		l.ChunkSize = int(chunkSize)
		l.ChunkCount = len(chunks)
		l.Chunks = chunks
	}
	return l
}

// UploadReader reads the entire source into memory then uploads it.
// Useful for non-file sources such as HTTP responses or in-memory buffers.
// Max source size: MaxFileSize (50 MB). For larger sources, use UploadFile.
func (c *Client) UploadReader(ctx context.Context, r io.Reader, opts UploadOpts) (*UploadResult, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("telconyx: read source: %w", err)
	}
	if int64(len(data)) > MaxFileSize {
		return nil, fmt.Errorf("%w: %d > %d (use UploadFile for larger files)",
			ErrFileTooBig, len(data), MaxFileSize)
	}
	if int64(len(data)) > c.cfg.MaxUploadSize {
		return nil, fmt.Errorf("telconyx: file size %d exceeds MaxUploadSize %d: %w",
			len(data), c.cfg.MaxUploadSize, ErrUploadTooLarge)
	}
	if opts.Name == "" {
		opts.Name = "file"
	}
	return c.uploadWithRetry(ctx, func() (io.Reader, error) {
		return bytes.NewReader(data), nil
	}, opts)
}

func (c *Client) uploadWithRetry(
	ctx context.Context,
	srcFn func() (io.Reader, error),
	opts UploadOpts,
) (*UploadResult, error) {
	var result *UploadResult
	err := c.withRetry(ctx, func(ctx context.Context) error {
		src, err := srcFn()
		if err != nil {
			return err
		}
		return c.uploadOnce(ctx, src, opts, &result)
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) uploadOnce(ctx context.Context, src io.Reader, opts UploadOpts, out **UploadResult) error {
	fields := map[string]string{
		"chat_id": c.cfg.ChatID,
	}
	if opts.Caption != "" {
		fields["caption"] = opts.Caption
	}

	resp, err := c.tp.PostMultipart(ctx, "sendDocument", fields, "document", opts.Name, src)
	if err != nil {
		return err
	}
	return parseSendDocumentResponse(resp.Body, opts, out)
}

// telegramMessage mirrors the subset of the Telegram Message object we need.
//
// We send everything via sendDocument, but Telegram may classify the stored
// file under a different media field in the returned Message: a .gif comes back
// as "animation", some media files as "video"/"audio", etc. All of these are
// successful uploads with a usable file_id, so we accept any of them.
type telegramMessage struct {
	MessageID int `json:"message_id"`
	Chat      struct {
		ID int64 `json:"id"`
	} `json:"chat"`
	Document  *telegramDocument  `json:"document,omitempty"`
	Video     *telegramDocument  `json:"video,omitempty"`
	Audio     *telegramDocument  `json:"audio,omitempty"`
	Animation *telegramDocument  `json:"animation,omitempty"`
	Voice     *telegramDocument  `json:"voice,omitempty"`
	VideoNote *telegramDocument  `json:"video_note,omitempty"`
	Sticker   *telegramDocument  `json:"sticker,omitempty"`
	Photo     []telegramDocument `json:"photo,omitempty"`
}

// media returns the stored file from whichever media field Telegram populated,
// or nil if the Message carries no media at all (e.g. a plain text warning such
// as "the file is too big"). For photos, the last entry is the largest size.
func (m *telegramMessage) media() *telegramDocument {
	switch {
	case m.Document != nil:
		return m.Document
	case m.Video != nil:
		return m.Video
	case m.Audio != nil:
		return m.Audio
	case m.Animation != nil:
		return m.Animation
	case m.Voice != nil:
		return m.Voice
	case m.VideoNote != nil:
		return m.VideoNote
	case m.Sticker != nil:
		return m.Sticker
	case len(m.Photo) > 0:
		return &m.Photo[len(m.Photo)-1]
	}
	return nil
}

type telegramDocument struct {
	FileID       string `json:"file_id"`
	FileUniqueID string `json:"file_unique_id"`
	FileName     string `json:"file_name,omitempty"`
	MimeType     string `json:"mime_type,omitempty"`
	FileSize     int    `json:"file_size,omitempty"`
}

type apiResponse struct {
	OK          bool            `json:"ok"`
	Result      json.RawMessage `json:"result,omitempty"`
	ErrorCode   int             `json:"error_code,omitempty"`
	Description string          `json:"description,omitempty"`
	Parameters  struct {
		RetryAfter      int   `json:"retry_after,omitempty"`
		MigrateToChatID int64 `json:"migrate_to_chat_id,omitempty"`
	} `json:"parameters,omitempty"`
}

func parseSendDocumentResponse(body []byte, opts UploadOpts, out **UploadResult) error {
	var api apiResponse
	if err := json.Unmarshal(body, &api); err != nil {
		return fmt.Errorf("telconyx: decode sendDocument response: %w", err)
	}
	if !api.OK {
		return apiErrorFromResponse(&api, "sendDocument")
	}
	var msg telegramMessage
	if err := json.Unmarshal(api.Result, &msg); err != nil {
		return fmt.Errorf("telconyx: decode message: %w", err)
	}
	doc := msg.media()
	if doc == nil {
		// The response is a Message with no media field at all — e.g. the file
		// was rejected and Telegram replied with a text warning ("the file is
		// too big"), the chat requires a premium account, or the bot lacks
		// permission. The file was NOT stored, so retrying would only spam the
		// group with duplicates. Mark this permanent.
		return &NonRetryableError{
			Method: "sendDocument",
			Reason: "response has no document field",
			Detail: truncate(string(api.Result), 256),
		}
	}
	res := &UploadResult{
		FileID:       doc.FileID,
		FileUniqueID: doc.FileUniqueID,
		MessageID:    msg.MessageID,
		ChatID:       msg.Chat.ID,
		Size:         doc.FileSize,
		MimeType:     doc.MimeType,
		Name:         doc.FileName,
	}
	if res.Name == "" {
		res.Name = opts.Name
	}
	if res.MimeType == "" {
		res.MimeType = opts.MimeType
	}
	*out = res
	return nil
}

func apiErrorFromResponse(api *apiResponse, method string) error {
	if api.Parameters.RetryAfter > 0 {
		return &FloodWaitError{Seconds: api.Parameters.RetryAfter}
	}
	return &APIError{
		Code:        api.ErrorCode,
		Description: api.Description,
		Method:      method,
	}
}

// parseGetFileResponse decodes the getFile response and returns the temporary file_path.
func parseGetFileResponse(body []byte) (string, error) {
	var api apiResponse
	if err := json.Unmarshal(body, &api); err != nil {
		return "", fmt.Errorf("telconyx: decode getFile response: %w", err)
	}
	if !api.OK {
		return "", apiErrorFromResponse(&api, "getFile")
	}
	var result struct {
		FilePath string `json:"file_path"`
	}
	if err := json.Unmarshal(api.Result, &result); err != nil {
		return "", fmt.Errorf("telconyx: decode getFile result: %w", err)
	}
	if result.FilePath == "" {
		return "", &NonRetryableError{
			Method: "getFile",
			Reason: "response has empty file_path",
			Detail: truncate(string(body), 256),
		}
	}
	return result.FilePath, nil
}

// DeleteMessage deletes a single message from a chat, retrying transient
// failures. Useful for cleaning up partial uploads when a chunked upload
// fails midway.
func (c *Client) DeleteMessage(ctx context.Context, chatID int64, messageID int) error {
	params := url.Values{}
	params.Set("chat_id", strconv.FormatInt(chatID, 10))
	params.Set("message_id", strconv.Itoa(messageID))

	return c.withRetry(ctx, func(ctx context.Context) error {
		resp, err := c.tp.PostForm(ctx, "deleteMessage", params)
		if err != nil {
			return err
		}
		var api apiResponse
		if err := json.Unmarshal(resp.Body, &api); err != nil {
			return fmt.Errorf("telconyx: decode deleteMessage: %w", err)
		}
		if !api.OK {
			return apiErrorFromResponse(&api, "deleteMessage")
		}
		return nil
	})
}

// DeleteChunks deletes all messages of a (chunked) file from the configured chat.
// It returns the first error encountered but tries to delete every chunk.
// Use this after a failed upload to remove the partial messages from the group
// before retrying.
//
// Message ids are chat-specific, so when the link records a chat id it must
// match this client's chat — deleting link message ids in a different chat
// would hit unrelated messages.
func (c *Client) DeleteChunks(ctx context.Context, link *FileLink) error {
	if link == nil {
		return ErrInvalidLink
	}
	chatID, err := strconv.ParseInt(c.cfg.ChatID, 10, 64)
	if err != nil {
		return fmt.Errorf("telconyx: DeleteChunks requires a numeric ChatID (got %q)", c.cfg.ChatID)
	}
	if link.ChatID != 0 && link.ChatID != chatID {
		return fmt.Errorf("telconyx: link belongs to chat %d but this client targets chat %d; refusing to delete", link.ChatID, chatID)
	}
	var firstErr error
	for _, ch := range link.AllChunks() {
		if ch.MessageID == 0 {
			continue
		}
		if err := c.DeleteMessage(ctx, chatID, ch.MessageID); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("delete chunk %d (msg %d): %w", ch.Index, ch.MessageID, err)
			}
		}
	}
	return firstErr
}
