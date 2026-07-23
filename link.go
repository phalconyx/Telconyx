package telconyx

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

const linkPrefix = "telconyx://file/"

// ChunkRef is a reference to a single chunk of a chunked upload.
// A non-chunked file has exactly one ChunkRef and the FileLink's top-level
// FileID/MessageID/Size fields are kept in sync with Chunks[0].
type ChunkRef struct {
	Index     int    `json:"index"`
	FileID    string `json:"file_id"`
	MessageID int    `json:"message_id"`
	Size      int    `json:"size"`
}

// FileLink is the portable reference to a file stored in Telegram.
// It contains all the metadata needed to identify and download the file later.
// For chunked files (ChunkCount > 1), the file is split into multiple
// Telegram messages; download reassembles them transparently.
type FileLink struct {
	FileID       string `json:"file_id"`
	FileUniqueID string `json:"file_unique_id"`
	MessageID    int    `json:"message_id"`
	ChatID       int64  `json:"chat_id"`
	Size         int    `json:"size"`
	MimeType     string `json:"mime_type,omitempty"`
	Name         string `json:"name,omitempty"`

	// Route is the alias of the Pool route (bot + chat) that uploaded the
	// file. Telegram file_ids are only valid for the bot that created them,
	// so downloads and deletes must go through the same route. Empty for
	// links created by a single-bot setup; a Pool resolves those to its
	// default route.
	Route string `json:"route,omitempty"`

	// Chunking (only present for files split into multiple chunks).
	ChunkSize  int        `json:"chunk_size,omitempty"`
	ChunkCount int        `json:"chunk_count,omitempty"`
	Chunks     []ChunkRef `json:"chunks,omitempty"`
}

// linkPayload is the on-the-wire representation inside a telconyx:// URL.
// Field names are kept short to minimise URL length.
type linkPayload struct {
	F string `json:"f"`
	U string `json:"u"`
	M int    `json:"m"`
	C int64  `json:"c"`
	S int    `json:"s"`
	T string `json:"t,omitempty"`
	N string `json:"n,omitempty"`

	// R is the route alias that uploaded the file (see FileLink.Route).
	// Absent in links from single-bot setups, which keeps them byte-identical
	// to links created before routing existed.
	R string `json:"r,omitempty"`

	// Chunking. F is the first chunk's file_id; CK contains file_ids for all
	// chunks (including the first, for simplicity), and CM the matching Telegram
	// message_ids (needed to delete every chunk). Only emitted when CC > 1.
	CS int      `json:"cs,omitempty"`
	CC int      `json:"cc,omitempty"`
	CK []string `json:"ck,omitempty"`
	CM []int    `json:"cm,omitempty"`
}

func encodeLink(l *FileLink) (string, error) {
	if l == nil {
		return "", ErrInvalidLink
	}
	if l.FileID == "" {
		return "", ErrInvalidLink
	}
	p := linkPayload{
		F: l.FileID,
		U: l.FileUniqueID,
		M: l.MessageID,
		C: l.ChatID,
		S: l.Size,
		T: l.MimeType,
		N: l.Name,
		R: l.Route,
	}
	if n := len(l.Chunks); n > 1 {
		p.CS = l.ChunkSize
		p.CC = n
		p.CK = make([]string, 0, n)
		p.CM = make([]int, 0, n)
		for _, ch := range l.Chunks {
			p.CK = append(p.CK, ch.FileID)
			p.CM = append(p.CM, ch.MessageID)
		}
	}
	b, err := json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("telconyx: encode link: %w", err)
	}
	return linkPrefix + base64.RawURLEncoding.EncodeToString(b), nil
}

// URL returns the portable telconyx:// link for this FileLink.
// The returned string is what users should store in their own database.
func (l *FileLink) URL() string {
	s, _ := encodeLink(l)
	return s
}

// IsChunked reports whether the file is split into multiple chunks.
func (l *FileLink) IsChunked() bool {
	return l != nil && len(l.Chunks) > 1
}

// AllChunks returns the full chunk list. For non-chunked (or legacy) FileLinks,
// it returns a single-element slice synthesised from the top-level fields so
// callers can iterate uniformly.
func (l *FileLink) AllChunks() []ChunkRef {
	if l == nil {
		return nil
	}
	if len(l.Chunks) > 0 {
		return l.Chunks
	}
	return []ChunkRef{{
		Index:     0,
		FileID:    l.FileID,
		MessageID: l.MessageID,
		Size:      l.Size,
	}}
}

// ParseURL parses a telconyx://file/... URL string into a FileLink.
func ParseURL(s string) (*FileLink, error) {
	if !strings.HasPrefix(s, linkPrefix) {
		return nil, fmt.Errorf("%w: missing prefix", ErrInvalidLink)
	}
	raw := strings.TrimPrefix(s, linkPrefix)
	if raw == "" {
		return nil, fmt.Errorf("%w: empty payload", ErrInvalidLink)
	}
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidLink, err)
	}
	var p linkPayload
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidLink, err)
	}
	if p.F == "" {
		return nil, fmt.Errorf("%w: missing file_id", ErrInvalidLink)
	}
	l := &FileLink{
		FileID:       p.F,
		FileUniqueID: p.U,
		MessageID:    p.M,
		ChatID:       p.C,
		Size:         p.S,
		MimeType:     p.T,
		Name:         p.N,
		Route:        p.R,
	}
	if p.CC > 1 && len(p.CK) == p.CC {
		l.ChunkSize = p.CS
		l.ChunkCount = p.CC
		l.Chunks = make([]ChunkRef, p.CC)
		hasMsgIDs := len(p.CM) == p.CC
		for i, fid := range p.CK {
			l.Chunks[i] = ChunkRef{
				Index:  i,
				FileID: fid,
			}
			if hasMsgIDs {
				l.Chunks[i].MessageID = p.CM[i]
			}
			if i == 0 {
				// Legacy links (no CM) still carry the first chunk's id in M.
				if l.Chunks[i].MessageID == 0 {
					l.Chunks[i].MessageID = p.M
				}
				l.Chunks[i].Size = p.S
			}
		}
	}
	return l, nil
}

// String implements fmt.Stringer; it returns the telconyx:// URL.
func (l *FileLink) String() string {
	if l == nil {
		return ""
	}
	return l.URL()
}
