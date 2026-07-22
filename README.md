# Telconyx

> **Telconyx** is a Telegram-backed cloud-storage bridge — a thin wrapper that turns a Telegram bot and chat into a file store, exposed through a clean HTTP API and an embeddable Go library. Upload a file and receive a portable, self-describing `telconyx://` reference to persist in your own database; resolve it later to stream the file back. Files beyond Telegram's per-message size limit are transparently split into chunks on upload and reassembled on download. Stateless, dependency-free, and with optional API-key authentication, it is built to drop into SaaS backends as a lightweight storage layer.

## Why

- **Stdlib only** — zero third-party dependencies, just Go + `net/http`.
- **Stateless bridge** — Telconyx does not store anything. You save the link in your own database.
- **Chunked uploads** — files larger than the configurable chunk size (`ChunkSize`, default 19 MB, max 50 MB) are split into multiple parts and reassembled in parallel on download.
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
    ChunkSize:       19 * 1024 * 1024, // default; keep under 20 MB on the hosted API
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
telconyx healthcheck               Probe the local server's /health (used by the Docker healthcheck)
telconyx version                   Print version
telconyx help                      Show usage
```

Environment variables:

| Variable                    | Required | Default  | Description                                                          |
|-----------------------------|----------|----------|----------------------------------------------------------------------|
| `TELCONYX_BOT_TOKEN`        | yes*     | —        | Bot token from @BotFather (*or use `TELCONYX_ROUTES`)                |
| `TELCONYX_CHAT_ID`          | yes*     | —        | Target chat ID (`-100...`) or `@name` (*or use `TELCONYX_ROUTES`)    |
| `TELCONYX_ROUTES`           | no       | —        | Multiple bot/chat routes: `alias=token@chat_id,...` (see [Scaling](#scaling-multiple-bots-and-routing)) |
| `TELCONYX_DEFAULT_ROUTE`    | no       | first route | Route alias assumed for links without a route marker              |
| `TELCONYX_API_BASE`         | no       | `https://api.telegram.org` | Bot API server root; set a [self-hosted server](#file-size-limits-and-self-hosting) to lift the 20MB download limit |
| `TELCONYX_API_KEY`          | no       | empty    | API key for HTTP server auth                                         |
| `TELCONYX_LISTEN`           | no       | `:9090`  | Server listen address                                                |
| `TELCONYX_TIMEOUT`          | no       | `60s`    | HTTP connection/response-header timeout (body transfer is never time-limited) |
| `TELCONYX_MAX_UPLOAD_SIZE`  | no       | `2GB`    | Max total file size for upload (e.g. `500MB`, `2GB`)                 |
| `TELCONYX_MAX_DOWNLOAD_SIZE`| no       | `2GB`    | Max total file size for download                                     |
| `TELCONYX_CHUNK_SIZE`       | no       | `19MB`   | Chunk size for split uploads (max 50MB; keep under 20MB on the hosted API) |
| `TELCONYX_CHUNK_CONCURRENCY`| no       | `3`      | Number of concurrent chunk downloads                                 |

Size suffixes are all **binary** (powers of 1024): `B`, `K`/`KB`, `M`/`MB`, `G`/`GB` all use 1024. So `49MB` = 49 × 1024 × 1024 bytes. This matches the on-disk byte count of files. For an exact decimal byte count, pass a bare number (e.g. `49000000`).

## HTTP API (server mode)

All JSON responses share a consistent envelope. **The HTTP status code is authoritative** — the body never contradicts it.

**Success** (`2xx`):

```json
{ "data": { ... }, "meta": { "request_id": "req_8f2a1c..." } }
```

**Error** (`4xx`/`5xx`):

```json
{ "error": { "code": "invalid_link", "message": "human-readable detail" }, "meta": { "request_id": "req_8f2a1c..." } }
```

`error.code` is a stable, machine-readable identifier (`unauthorized`, `invalid_json`, `missing_url`, `invalid_link`, `unknown_route`, `missing_file`, `invalid_multipart`, `upload_failed`, `upload_too_large`, `download_failed`, `delete_failed`, `internal`); `error.message` is for humans. Every response also carries an `X-Request-Id` header echoing `meta.request_id` — send your own `X-Request-Id` to propagate a trace id. The only non-JSON response is a successful `/download`, which streams raw file bytes.

`GET /health`

```json
{ "data": { "status": "ok", "time": "2026-06-17T10:30:00Z" }, "meta": { "request_id": "req_8f2a1c..." } }
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
  "data": {
    "url": "telconyx://file/eyJmIjoiQWdBQ0FnSS0tLS0ifQ==",
    "file_id": "AgACAgIAAxk...",
    "file_unique_id": "AgAD...",
    "message_id": 123,
    "chat_id": -1001234567890,
    "size": 1048576,
    "name": "report.pdf",
    "mime_type": "application/pdf"
  },
  "meta": { "request_id": "req_8f2a1c..." }
}
```

In multi-route mode (`TELCONYX_ROUTES`), `data` also includes `route` — the alias of the route that stored the file. For chunked uploads, `data` also includes `chunk_size`, `chunk_count`, and a `chunks` array:

```json
{
  "data": {
    "...": "...",
    "chunk_size": 51380224,
    "chunk_count": 5,
    "chunks": [
      {"index": 0, "file_id": "...", "message_id": 100, "size": 51380224},
      {"index": 1, "file_id": "...", "message_id": 101, "size": 51380224}
    ]
  },
  "meta": { "request_id": "req_8f2a1c..." }
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

Response: on success, the **raw file bytes** (not enveloped), with `Content-Type`, `Content-Disposition: attachment; filename="..."` and (for chunked files) `X-Telconyx-Chunks: N` headers when known. The real filename and type come from these headers — the output filename you pass to curl (`-o`) is just a local choice and does not have to match. Errors that occur *before* the first body byte (bad request, unknown link, expired `file_id`, Telegram unreachable) use the standard JSON error envelope (`download_failed`); a failure after streaming has begun can only abort the connection, which the client sees as a truncated body.

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
  "data": { "deleted_messages": 3, "total_chunks": 3, "skipped": 0 },
  "meta": { "request_id": "req_8f2a1c..." }
}
```

Deletes the Telegram message(s) backing the file. For chunked files every part is removed. Notes:

- Requires a **numeric** `TELCONYX_CHAT_ID` (not `@username`) — otherwise the request returns an error.
- If the link records a chat id that differs from the configured chat, the request is refused: Telegram message ids are chat-specific, and deleting them in a different chat would hit unrelated messages.
- The bot can only delete its own messages, and Telegram only allows deletion within a limited window (commonly ~48 hours); older files may return `400 Bad Request: message can't be deleted`.
- `skipped` counts parts whose Telegram message id is not stored in the link. Links created before this feature only carry the **first** chunk's message id, so only that part can be deleted — re-upload to get a fully deletable link.
- On failure some parts may already have been deleted (deletion is attempted for every part; the first error is returned).

## Chunking

The hosted Bot API has **asymmetric** file-size limits: bots may *send* files up to **50 MB** (`sendDocument`) but may only *download* files up to **20 MB** (`getFile`). Storage that cannot be read back is useless, so Telconyx splits files into chunks of `ChunkSize` bytes with a default of **19 MB** — under the download limit, with headroom — and uploads each as a separate message. See [File size limits and self-hosting](#file-size-limits-and-self-hosting) for raising these limits.

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

## Scaling: multiple bots and routing

Telegram rate limits apply per **bot** (~30 msg/s overall) and per **chat** (~20 msg/min in a group). To spread load, configure multiple routes — each a `(alias, bot token, chat)` triple:

```bash
TELCONYX_ROUTES='b1=1234:ABC@-1001111111111,b2=5678:DEF@-1002222222222'
# For a @username chat, keep its "@":  b3=5678:DEF@@mygroup
```

Two routes may share a token with different chats (raises the per-chat ceiling) or use different tokens (raises the per-bot ceiling).

**How routing works.** Each upload goes to the route with the fewest uploads currently in flight (**least-inflight**); ties rotate round-robin, so an idle pool cycles evenly. Routes in a flood-wait (429) cooldown are skipped automatically, and an upload fails over to the next route when a route errors before anything was stored. A Telegram `file_id` is only valid for the bot that uploaded it, so each `telconyx://` link records its route alias, and downloads/deletes are always routed back to the origin bot. All chunks of one file go through a single route.

**Aliases are permanent.** The alias is stored inside every link created through it (Telconyx itself stays stateless — no database). Renaming or removing an alias breaks the links that reference it; the server then returns `400 unknown_route`. Adding new routes is always safe.

**Migrating from a single bot.** Links created before routing carry no alias; a multi-route server resolves them to `TELCONYX_DEFAULT_ROUTE` (default: the first route). Keep your original bot+chat as that route and all old links keep working — single-bot setups also still emit byte-identical links, so nothing changes until you opt in.

As a library, the same is available via `telconyx.NewPool`:

```go
pool, _ := telconyx.NewPool(telconyx.PoolConfig{
    Routes: []telconyx.Route{
        {Alias: "b1", Token: "1234:ABC", ChatID: "-1001111111111"},
        {Alias: "b2", Token: "5678:DEF", ChatID: "-1002222222222"},
    },
    // Picker: custom strategy; default is least-inflight with round-robin
    // tie-break, plus flood-wait cooldown. NewRoundRobin() forces plain rotation.
})
result, _ := pool.UploadFile(ctx, "big-backup.tar.gz") // picks a route
link, _ := telconyx.ParseURL(result.Link())
pool.Download(ctx, link, "big-backup.tar.gz")          // routed to the origin bot
```

`Pool` exposes the same operations as `Client`, so `server.New` accepts either.

## File size limits and self-hosting

| Limit                          | Default      | Configurable       |
|--------------------------------|--------------|--------------------|
| Per-chunk upload size          | 50 MB (Bot API `sendDocument`) | `ChunkSize` (capped at 50 MB) |
| Per-chunk download size        | 20 MB (hosted Bot API `getFile`) | lifted by a self-hosted API server |
| Default chunk size             | 19 MB        | `ChunkSize`        |
| Total file size for upload     | 2 GB         | `MaxUploadSize`    |
| Total file size for download   | 2 GB         | `MaxDownloadSize`  |
| Concurrent chunk downloads     | 3            | `ChunkConcurrency` |

The binding constraint on the hosted API is the **20 MB `getFile` download limit** — that is why the default `ChunkSize` is 19 MB. To lift it, run a [self-hosted Bot API server](https://github.com/tdlib/telegram-bot-api) and point Telconyx at it:

```bash
TELCONYX_API_BASE=http://localhost:8081
TELCONYX_CHUNK_SIZE=49MB   # now safe: self-hosted getFile serves large files
```

Two caveats:

- Files uploaded with chunks **larger than 20 MB** (e.g. with the old 49 MB default) cannot be downloaded through the hosted API at all — only a self-hosted API server can retrieve them. Links themselves stay valid; it is purely a retrieval-path limitation.
- A quick way to validate your setup end to end: upload a ~25 MB file and download it back (`telconyx upload` / `telconyx download`). If `getFile` answers `400: file is too big`, your chunk size is above what your API endpoint can serve.

## Project layout

```text
telconyx/
├── client.go                Client, Config, retry, defaults
├── pool.go                  Pool, Route, Picker (least-inflight + flood-wait cooldown)
├── upload.go                UploadFile (chunked), UploadReader
├── download.go              Download (parallel), DownloadTo
├── link.go                  FileLink, ChunkRef, telconyx:// codec
├── errors.go                APIError, FloodWaitError, PartialUploadError, ...
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
