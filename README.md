# shrt-cpp

High-performance URL shortener backend — C++ port of
[shrt-ts](https://github.com/abhijitkrm/shrt-ts) /
[shrt-go](https://github.com/abhijitkrm/shrt-go) /
[shrt-rust](https://github.com/abhijitkrm/shrt-rust), tuned for maximum
throughput with zero external dependencies (C++20 + POSIX only).

* **HTTP**: custom thread-per-connection server (`SERVER=mini`, default) —
  one read batch answered by a single write, keep-alive + HTTP pipelining;
  `SERVER=kq` is a kqueue-based evented frontend (one thread, non-blocking).
* **Storage**: custom append-only log (AOF) — sharded in-memory index (256
  shards, `std::shared_mutex` + `std::unordered_map` + atomic hit counters)
  + batched `write()`/`fsync`. Reads never touch disk; writes are ~ns enqueue
  + one syscall batch per 5 ms.
* **Serialization**: log lines are hand-serialized into scratch buffers —
  zero JSON, zero per-row allocation on the read/bulk hot paths. Requests use
  a minimal purpose-built JSON field extractor.
* **Codes**: 8 chars = `ALPHABET[instance]` + 7 random base62 chars
  (62⁷ ≈ 3.5T per instance). The prefix shard-marks every code — unique
  across processes with zero coordination, and a read-miss knows exactly
  which sibling log to tail. Checked against the index, retried on collision.
* **Multi-instance**: `WORKERS=N` spawns N processes that **share one port
  via SO_REUSEPORT** (kernel load-balances) with per-instance log shards
  (`data-<i>.log`). Siblings are discovered and tailed **lazily on
  read-miss** — writes never pay replication cost, so write throughput scales
  ~linearly with instance count.
* **Durability**: every append reaches the OS page cache within 5 ms and is
  fsync'd every 500 ms — process crash loses ≤5 ms of writes, machine crash
  ≤~500 ms (tunable in `src/store.cpp`; snapshot+truncate via `compact()`).

## Build & run

```sh
make            # release build (O3 + LTO + -march=native)
./shrt          # :3000, mini server, 1 instance
WORKERS=4 ./shrt    # 4 instances sharing :3000
SERVER=kq ./shrt    # kqueue evented frontend
```

## API (identical to shrt-ts / shrt-go / shrt-rust)

| route | method | description |
|---|---|---|
| `/api/shorten` | POST | `{"url","alias"?,"ttl_ms"?}` → `{"code","short_url"}` |
| `/api/shorten/bulk` | POST | `{"urls":[...]}` (≤10k) → `{"count","codes"}` |
| `/:code` | GET | 302 redirect (counts a hit) |
| `/api/stats/:code` | GET | `{"code","url","hits","created_at","expires_at"}` |
| `/api/links` | GET | `?limit&offset&sort=hits|created&q` (admin list) |
| `/api/links/:code` | PATCH/DELETE | requires `ADMIN_TOKEN` + `x-admin-token` header (unset/empty = always 404, fail-closed) |
| `/api/health` | GET | 200 only when the store answers (RESP `PING` / rocksdb probe) — else 503 |
| `/api/metrics` `/metrics` | GET | JSON counters / Prometheus text exposition |
| `/` | GET | built-in UI (`ui/index.html`) |

## Config (env)

`PORT` (3000) · `DATA_DIR` (`data`) · `WORKERS` (1) · `SERVER` (`mini`|`kq`)
· `STORE` (`aof`|`dragonfly`|`redis`|`rocksdb`) · `DRAGONFLY_ADDR` (`127.0.0.1:6379`)
· `ROCKSDB_PATH` (`{DATA_DIR}/rocks`) — embedded RocksDB dir (needs
  `-lrocksdb`; `ROCKSDB_PREFIX` Makefile var points at the brew install)
· `CACHE` (100000, bounded hot FIFO entries) · `CACHE_TTL_MS` (5000)
· `KV_LAYOUT` (`key`|`hash`) — `hash` packs links as hash fields in
`l:{code % KV_BUCKETS}` (~40% less KV memory at ~105B values); expiry via
value check + janitor `KV_SWEEP_MS` (1h). Needs server
`hash-max-listpack-value` >= value size (~256) for full savings
· `SEED` (pre-generate N links at boot) · `ADMIN_TOKEN` · `CORS_ORIGIN` (`*`)
· `RATE_LIMIT` (0=off) — per-IP token bucket req/s on `POST /api/shorten`
  (cost 1) and `/api/shorten/bulk` (cost = url count); 429 when empty.
  `RATE_LIMIT_BURST` (default = RATE_LIMIT) sets capacity; `TRUST_PROXY`
  switches the key to the first `X-Forwarded-For` address
· `LINK_TTL_MS` (86400000, capped at this value)

`STORE=dragonfly` moves the whole corpus to an external RESP store
(DragonflyDB / Redis): keys `l:{code}` → `{exp}|{created}|{url}` (PX
self-evicts TTLs), `h:{code}` → hit counter (batched `INCRBY` every 5 ms).
Each node keeps only a bounded FIFO cache — memory stays flat as links
grow; a cold redirect costs one `GET`. No tailing — admin mutations work on
any node. Live tests: `SHRT_KV_ADDR=127.0.0.1:6379 ./shrt-test`.

`STORE=rocksdb` keeps the corpus on local disk in an embedded RocksDB
(`links` CF: `code` → `{exp}|{created}|{url}`; `hits` CF: u64 counters
accumulated by a merge operator — no read-modify-write). Expiry is
enforced on read and a compaction filter drops expired keys during
compaction — no janitor. Reads hit a bounded in-process FIFO cache first,
so the DB only sees misses; bloom filters make 404s free. Writes land via
`WriteBatch` (bulk = one batch). Measured ~210k redirects/s, ~78k
shortens/s, ~714k bulk rows/s — reads at parity with the in-RAM backend
since the cache absorbs the hot set. Single-writer: RocksDB holds an
exclusive LOCK on the dir, so one process per `ROCKSDB_PATH` — for
multi-instance/multi-node use the `dragonfly`/`redis` backend.

## Bench

```sh
./shrt-bench        # builds + spawns real servers, drives raw-TCP load
bash scripts/smoke.sh   # end-to-end API smoke
bash scripts/flood.sh   # write flood (autocannon if installed)
./shrt-test         # 32 tests: store engine + API × both frontends
```

Measured on Apple Silicon (client+server colocated, 64 conns, same in-repo
loadgen methodology across all implementations):

| scenario | shrt-cpp | shrt-rust | shrt-go | shrt-ts |
|---|---|---|---|---|
| redirect | ~158k req/s | ~208k | ~197k | ~133k |
| redirect, pipelined ×10 | **~607k req/s** | ~409k | ~457k | ~320k |
| mixed 95/5 | ~157k req/s | ~205k | ~185k | ~115k |
| shorten | ~165k req/s | ~175k | ~158k | ~72k |
| bulk ×1000 | ~3.4M rows/s | ~4.1M | ~2.1M | ~640k |
| redirect ×4 workers | **~214k req/s** | ~184k | — | — |

The C++ `mini` server wins pipelined reads outright (~607k req/s) — the
thread-per-connection loop answers a whole read batch with a single `send()`.
Rust still leads single-shot redirects and bulk writes.

## Layout

```
src/
  base62.hpp      code alphabet
  codec.{hpp,cpp} AOF row/hit/del line encode+parse (zero-dep)
  aof.{hpp,cpp}   append-only log: buffered push, replay, tail readers,
                  instance claiming via pid lock files
  store.{hpp,cpp} sharded index, hits batching, sibling tailing, compact
  app.{hpp,cpp}   transport-agnostic request handler
  server.{hpp,cpp} socket helpers, mini (thread/conn) + kq (kqueue) servers
  main.cpp        env config, WORKERS supervisor + SO_REUSEPORT, seed, signals
  bench.cpp       load generator + benchmark scenarios
tests/
  store_test.cpp  storage-engine tests
  api_test.cpp    API tests run against mini AND kq frontends
ui/index.html     admin UI (same as sibling repos)
```
