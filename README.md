# Telconyx

> **Telconyx** is a Telegram-backed cloud-storage bridge — a thin wrapper that turns a Telegram bot and chat into a file store, exposed through a clean HTTP API and an embeddable Go library. Upload a file and receive a portable, self-describing `telconyx://` reference to persist in your own database; resolve it later to stream the file back. Files beyond Telegram's per-message size limit are transparently split into chunks on upload and reassembled on download. Stateless, dependency-free, and with optional API-key authentication, it is built to drop into SaaS backends as a lightweight storage layer.

## Why

- **Stdlib only** — zero third-party dependencies, just Go + `net/http`.
- **Stateless bridge** — Telconyx does not store anything. You save the link in your own database.
- **Chunked uploads** — files larger than the configurable chunk size (`ChunkSize`, default 49 MB, max 50 MB) are split into multiple parts and reassembled in parallel on download.
- **One binary, two modes** — `import` as a Go library, or run as an HTTP service on `:9090`.
- **Tiny Docker image** — multi-stage build, distroless base, ~8 MB.
- **Resilient** — built-in flood-wait retry, exponential backoff with jitter, context cancellation, per-chunk retry.

## Disclaimer

Telegram's [Terms of Service](https://telegram.org/tos) prohibit using the service as a CDN or distributed file storage. Accounts and bots that store large amounts of non-conversational data can be restricted. **Use at your own risk.** This project is suitable for personal, educational, and experimental use.

## Quick start

### 1. Get a bot token and chat id

1. Talk to [@BotFather](https://t.me/BotFather) on Telegram, send `/newbot`, copy the token.
2. Create a group, add the bot to it.
3. In the group, send `/start`. This gives the bot an update it can see — otherwise the `getUpdates` call below returns an empty `result` array.

4. Find the `chat.id` of the group you just posted in:

   ```bash
   curl https://api.telegram.org/bot<TOKEN>/getUpdates
   ```

   The `chat.id` of a group looks like `-1001234567890`. (It always starts with `-100` for supergroups.)

> Telconyx itself does not need privacy mode to be disabled — it only ever calls `sendDocument`, never reads incoming messages. But the `getUpdates` step above is just for you to discover the chat id, so the bot needs to "see" at least one message first.

### 2. Configure

```bash
cp .env.example .env
# edit .env
```

### 3. Run

**As a Go binary:**

```bash
make build
./bin/telconyx serve
```

**As a Docker container:**

```bash
docker compose up -d
```

**As a Go library:**

```go
import "github.com/phalconyx/telconyx"

client, _ := telconyx.NewClient(telconyx.Config{
    Token:         "123:ABC...",
    ChatID:        "-1001234567890",
    MaxUploadSize:  2 * 1024 * 1024 * 1024, // 2 GB (default)
    MaxDownloadSize: 2 * 1024 * 1024 * 1024,
    ChunkSize:       49 * 1024 * 1024, // default
    ChunkConcurrency: 3,              // default
})

// Upload — files > ChunkSize are auto-chunked.
result, _ := client.UploadFile(ctx, "big-backup.tar.gz")
fmt.Println(result.Link())  // telconyx://file/...
if result.ChunkCount > 1 {
    fmt.Printf("split into %d chunks\n", result.ChunkCount)
}

// Save result.Link() anywhere in your own storage.

// Later: download — chunks are reassembled in parallel.
link, _ := telconyx.ParseURL(result.Link())
client.Download(ctx, link, "big-backup.tar.gz")
```

## CLI

```text
telconyx serve                     Run HTTP server (default :9090)
telconyx upload <file>             Upload a file, print the telconyx:// link to stdout
telconyx download <url> <dest>     Download a file by telconyx:// URL
telconyx version                   Print version
telconyx help                      Show usage
```

Environment variables:

| Variable                    | Required | Default  | Description                                                          |
|-----------------------------|----------|----------|----------------------------------------------------------------------|
| `TELCONYX_BOT_TOKEN`        | yes      | —        | Bot token from @BotFather                                            |
| `TELCONYX_CHAT_ID`          | yes      | —        | Target chat ID (`-100...`) or `@name`                                |
| `TELCONYX_API_KEY`          | no       | empty    | API key for HTTP server auth                                         |
| `TELCONYX_LISTEN`           | no       | `:9090`  | Server listen address                                                |
| `TELCONYX_TIMEOUT`          | no       | `60s`    | Per-request HTTP timeout                                             |
| `TELCONYX_MAX_UPLOAD_SIZE`  | no       | `2GB`    | Max total file size for upload (e.g. `500MB`, `2GB`)                 |
| `TELCONYX_MAX_DOWNLOAD_SIZE`| no       | `2GB`    | Max total file size for download                                     |
| `TELCONYX_CHUNK_SIZE`       | no       | `49MB`   | Chunk size for split uploads (max 50MB)                              |
| `TELCONYX_CHUNK_CONCURRENCY`| no       | `3`      | Number of concurrent chunk downloads                                 |

Size suffixes are all **binary** (powers of 1024): `B`, `K`/`KB`, `M`/`MB`, `G`/`GB` all use 1024. So `49MB` = 49 × 1024 × 1024 bytes. This matches the on-disk byte count of files. For an exact decimal byte count, pass a bare number (e.g. `49000000`).

## HTTP API (server mode)

`GET /health`

```json
{"status": "ok", "time": "2025-01-15T10:30:00Z"}
```

`POST /upload` (multipart/form-data)

```bash
curl -X POST http://localhost:9090/upload \
  -H "X-API-Key: $TELCONYX_API_KEY" \
  -F "file=@report.pdf"
```

Response `201 Created`:

```json
{
  "url": "telconyx://file/eyJmIjoiQWdBQ0FnSS0tLS0ifQ==",
  "file_id": "AgACAgIAAxk...",
  "file_unique_id": "AgAD...",
  "message_id": 123,
  "chat_id": -1001234567890,
  "size": 1048576,
  "name": "report.pdf",
  "mime_type": "application/pdf"
}
```

For chunked uploads the response also includes:

```json
{
  "...": "...",
  "chunk_size": 51380224,
  "chunk_count": 5,
  "chunks": [
    {"index": 0, "file_id": "...", "message_id": 100, "size": 51380224},
    {"index": 1, "file_id": "...", "message_id": 101, "size": 51380224},
    ...
  ]
}
```

`POST /download` (application/json)

```bash
# -OJ saves the file under its ORIGINAL name + extension, taken from the
# response's Content-Disposition header (e.g. appstore_icon.webp) — whatever
# the real type is. Use -o myname.ext instead to pick the local name yourself.
curl -X POST http://localhost:9090/download \
  -H "X-API-Key: $TELCONYX_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"url":"telconyx://file/eyJmIjoiLi4uIn0="}' \
  -OJ
```

Response: the file bytes, with `Content-Type`, `Content-Disposition: attachment; filename="..."` and (for chunked files) `X-Telconyx-Chunks: N` headers when known. The real filename and type come from these headers — the output filename you pass to curl (`-o`) is just a local choice and does not have to match.

`POST /delete` (application/json)

```bash
curl -X POST http://localhost:9090/delete \
  -H "X-API-Key: $TELCONYX_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"url":"telconyx://file/eyJmIjoiLi4uIn0="}'
```

Response `200 OK`:

```json
{
  "success": true,
  "deleted_messages": 3,
  "total_chunks": 3,
  "skipped": 0
}
```

Deletes the Telegram message(s) backing the file. For chunked files every part is removed. Notes:

- Requires a **numeric** `TELCONYX_CHAT_ID` (not `@username`) — otherwise the request returns an error.
- The bot can only delete its own messages, and Telegram only allows deletion within a limited window (commonly ~48 hours); older files may return `400 Bad Request: message can't be deleted`.
- `skipped` counts parts whose Telegram message id is not stored in the link. Links created before this feature only carry the **first** chunk's message id, so only that part can be deleted — re-upload to get a fully deletable link.
- On failure some parts may already have been deleted (deletion is attempted for every part; the first error is returned).

## Chunking

Telegram's Bot API caps each file at **50 MB** for `sendDocument`. Telconyx automatically splits larger files into chunks of `ChunkSize` bytes (default 49 MB to leave headroom for multipart overhead) and uploads each as a separate message.

On download:
- The library uses up to `ChunkConcurrency` workers (default 3) to fetch chunks in parallel via `WriteAt`, so reassembly is fast.
- Chunks are pre-allocated as a single file via `Truncate` to avoid sparse-file issues.

The `telconyx://` link contains all chunk references, so a single URL is enough to reassemble the whole file. The URL is only slightly longer for chunked files (~100 bytes per extra chunk).

### Partial-upload cleanup

If a chunked upload fails partway through, the chunks that *did* succeed are already in your chat. To prevent duplicates on retry, Telconyx classifies permanent failures (e.g. `"sendDocument response has no document field"`, which usually means the file was rejected by the chat) as `*NonRetryableError` and stops retrying immediately. Transient failures (5xx, network) are still retried.

You can clean up the partial messages manually with `DeleteChunks`:

```go
link, _ := telconyx.ParseURL(savedURL) // URL of the partial upload
if err := client.DeleteChunks(ctx, link); err != nil {
    log.Printf("some chunks could not be deleted: %v", err)
}
```

`DeleteChunks` requires a numeric `ChatID` (not `@groupusername`) and deletes every message referenced in the link. After cleanup, retry the upload. The same operation is exposed over HTTP as `POST /delete` (see [HTTP API](#http-api-server-mode)).

## Limits

| Limit                          | Default      | Configurable       |
|--------------------------------|--------------|--------------------|
| Per-chunk upload size          | 50 MB (Bot API) | `ChunkSize` (capped at 50 MB) |
| Total file size for upload     | 2 GB         | `MaxUploadSize`    |
| Total file size for download   | 2 GB         | `MaxDownloadSize`  |
| Concurrent chunk downloads     | 3            | `ChunkConcurrency` |

## Project layout

```text
telconyx/
├── client.go                Client, Config, retry, defaults
├── upload.go                UploadFile (chunked), UploadReader
├── download.go              Download (parallel), DownloadTo
├── link.go                  FileLink, ChunkRef, telconyx:// codec
├── errors.go                APIError, FloodWaitError, ErrUploadTooLarge, ...
├── client_test.go
├── internal/transport/      raw HTTP (multipart, streaming)
├── server/                  net/http handlers (port 9090)
├── cmd/telconyx/            CLI entry point (serve, upload, download)
├── examples/basic/          library usage example
├── Dockerfile               multi-stage, distroless
├── docker-compose.yml
├── Makefile
├── .env.example
└── go.mod                   zero third-party deps
```

## Development

```bash
make tidy     # go mod tidy
make build    # build binary
make test     # go test -v -race ./...
make lint     # go vet ./...
```

## License

MIT
