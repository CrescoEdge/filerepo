# filerepo

Distributed file/artifact repository plugin for the [Cresco](https://github.com/CrescoEdge) edge
framework. `filerepo` watches a directory, tracks every file in an embedded catalog, and moves
files and plugin jars across the mesh — via inline transfer for small files, byte-range dataplane
**streaming for files of any size**, watched-directory sync between agents, and push-to-remote-repo.

- **Bundle:** `io.cresco.filerepo` (OSGi Declarative Services component, no `Bundle-Activator`)
- **Capability namespace:** `filerepo` (self-describes via `getcapabilities` → the fabric capability inventory)
- **Catalog:** embedded Apache Derby (10.17), one `filelist` table (`filepath` PK, `md5`, `insync`, `lastmodified`, `filesize`)
- **Auto-load:** the agent auto-starts `filerepo` at boot (gated by `enable_filerepo`, default true)

---

## Table of contents
- [What it does](#what-it-does)
- [Architecture](#architecture)
- [Operating modes](#operating-modes)
- [Transfer model — how any size is moved](#transfer-model--how-any-size-is-moved)
- [Message actions (API)](#message-actions-api)
- [Configuration](#configuration)
- [Safeguards](#safeguards)
- [Performance](#performance)
- [Client usage](#client-usage)
- [Testing](#testing)

---

## What it does

`filerepo` gives a Cresco agent a content-addressed file store and a way to move that content to
other agents over the mesh dataplane. Typical uses:

- **Artifact/model distribution** — publish a jar or model file on one agent, pull it to others.
- **Watched-directory sync** — point two agents at a `filerepo_name`; files dropped on the producer
  are cataloged (path/md5/size/mtime) and synced to subscribed consumers.
- **On-demand file fetch** — a client asks an agent for a file by path (`getfile` inline for small
  files, `streamfile` over the dataplane for large ones).

Every file is tracked in an embedded Derby catalog by absolute path with its MD5, size, and mtime,
so change detection and integrity checks are cheap and exact.

## Architecture

| Class | Role |
|---|---|
| `Plugin` | DS `@Component` (service `PluginService`, `ConfigurationPolicy.REQUIRE`). `activate()` stores context; `isStarted()` builds the engines and blocks until the agent is active; `isStopped()` tears everything down (shuts the transfer pool, sync pool, timers, and DB). |
| `ExecutorImpl` | Implements `Executor` — the message router. Handles every `EXEC` action (see [API](#message-actions-api)), owns the inline-transfer size guard, the streamfile chunker, and the bounded transfer thread pool. Self-describes via `@CrescoCapabilities`/`@CrescoAction`. |
| `RepoEngine` | The repo logic — directory scan, MD5/catalog diff, mesh discovery/broadcast, subscriber management, and the peer-sync downloader (bounded sync pool). |
| `DBEngine` | The Derby catalog. All queries are **parameterized** (`PreparedStatement`); DBCP2 connection pool. |
| `FileObject` / `StreamObject` | Value types: a cataloged file (name/md5/path/mtime/size) and an in-flight streamfile transfer (id/path/range/bytes-transferred/active). |

## Operating modes

`RepoEngine.start()` selects a mode from config:

- **Producer** — `scan_dir` **and** `filerepo_name` set. Periodically scans `scan_dir`, catalogs
  new/changed files (MD5 + Derby), broadcasts its existence on the dataplane, and pushes file diffs
  to any subscribed consumer, which pulls the changed files. Scan cadence: `scan_delay` /
  `scan_period`; recursion via `scan_recursive`.
- **Consumer** — `filerepo_name` and `repo_dir` set, **no** `scan_dir`. Listens for producers of
  that `filerepo_name`, subscribes, receives file diffs (`repolistin`), and downloads changed files
  into `repo_dir`, then acknowledges (`repoconfirm`).
- **On-demand only** — no scan/sync config. The plugin still serves `getfile` / `streamfile` /
  `getrepofilelist` / `putjar` etc. against `repo_dir` (default `filerepo`).

Sync handshake: producer `discover` → consumer `subscribe` → producer sends `repolistin` (compressed
file diff) → consumer downloads changed files (dataplane) → consumer `repoconfirm`.

## Transfer model — how any size is moved

There are **two** transfer paths, and choosing the right one matters:

### Inline — `getfile` / `getjar` (small files)
The whole file is returned as bytes **in a single control-plane (wss) RPC reply**. The wss frame is
capped (~1 MB), so inline transfer is only for small files. `filerepo` enforces this with
`max_inline_bytes` (**default 512 KB**, deliberately under the frame limit): a larger file returns
`status = 5` ("use streamfile") instead of producing an oversized frame that would break the
connection. Inline is latency-bound (~one RPC round-trip), ideal for config files, small adapters, etc.

### Streaming — `streamfile` (any size)
`streamfile` seeks a byte range and pushes the file to a dataplane stream in **chunks**, so it
handles files of **any size** with bounded memory. Each chunk is a dataplane binary message framed as:

```
[ transfer_id (8 bytes) ][ seq_num (6-digit, zero-padded) ][ payload ]
```

A consumer subscribes to the stream (by `ident_key`/`ident_id`), **strips the 14-byte header**, and
**reassembles the payload ordered by `seq_num`**. filerepo's own peer-sync consumer does this; an
external client must do the same (see [Client usage](#client-usage)).

Chunk size is `buffer_size` (default `stream_buffer_size` = 256 KB) and is **hard-capped at 768 KB**
(`MAX_STREAM_BUFFER`): a chunk plus its header must fit one dataplane frame or it is silently dropped,
so an over-large `buffer_size` can never break a transfer. Cancel an in-flight transfer with
`streamfilecancel`.

## Message actions (API)

All actions are `MsgEvent` type `EXEC`, routed to the plugin instance (`region`/`agent`/`pluginid`).
`status`/`status_code` `10` = success. `object` returns are gzip+base64 compressed; `bytes` returns
are base64 in the reply.

| Action | Params | Returns | Purpose |
|---|---|---|---|
| `getrepofilelist` | `repo_name` *(req)*; optional `limit`, `offset` | `repofilelist` (json), `repo_total` | List catalog rows (path/md5/size/mtime). Paginated (Derby `OFFSET/FETCH`) — use `limit`/`offset` for large repos. |
| `repolist` | — | `repolist` (json) | Plugin/jar inventory of the repo dir + server (region/agent/pluginid). |
| `getfile` | `file_path` *(req)* | `file_data` (bytes), `file_metadata` | Return a **cataloged** file inline. Refused (`status 5`) if larger than `max_inline_bytes`; refused (`status 9`) if not in the catalog. |
| `getjar` | `action_pluginname`, `action_pluginmd5` | `jardata` (bytes) | Return a plugin jar by name+md5 (size-guarded). |
| `putjar` | `pluginname`, `md5`, `jarfile`, `version`, `jardata` (bytes) | `uploaded` | Write a plugin jar, **md5-verified** (mismatch → deleted + rejected). Path-traversal-safe. |
| `putfiles` | `repo_name`, `overwrite` (bool) | — | Land pushed files (MsgEvent attachments) into a repo. |
| `putfilesremote` | `file_list` (json, req), `dst_region`, `dst_agent`, `dst_plugin`, `repo_name` | `status` | Push a set of local files to another agent's repo. |
| `streamfile` | `file_path`, `start_byte` (long), `byte_length` (long), `transfer_id`, `ident_key`, `ident_id`, optional `buffer_size` | `status` | Stream a byte-range over the dataplane in chunks. Any size. |
| `streamfilecancel` | `transfer_id` | `status_code` | Cancel an in-flight streamfile transfer. |
| `getscandir` | — | `scan_dir` | Report the configured scan directory. |
| `removefile` | `repo_name`, `file_name` | `status` | Remove a file (disk + catalog row). Path-traversal-safe. |
| `clearrepo` | `repo_name` | `status` | Delete all files in a repo (disk + catalog). |
| `repolistin` | `repolistin` (json diff), `transfer_id` | `status_code` | Sync consumer side — receive a producer's diff and pull changed files. |
| `repoconfirm` | `transfer_id` | — | Sync handshake ack. |
| `getmetrics` | — | `metrics` (json) | Central metrics — `MeasurementEngine` gauges (`filerepo.files.count`, `filerepo.active.transfers`). Folded into the controller's `getmetricinventory`. |
| `getcapabilities` | — | `capabilities` (json) | Self-describing capability document (LLM tool specs). |

## Central health & metrics

filerepo is wired into Cresco's two central observability systems — the **same mechanisms every
other plugin uses** (sysinfo is the reference), so metrics and health are consistent fabric-wide:

- **Metrics** — a unified `MeasurementEngine` exposes `filerepo.files.count` (catalog size) and
  `filerepo.active.transfers` (in-flight `streamfile` count) as gauges. The `getmetrics` action
  returns the standard `getAllMetrics()` JSON, which the controller's `getmetricinventory` fan-out
  aggregates across the mesh. Query one node with `getmetrics`, or the whole fabric with
  `getmetricinventory` (`action_scope=node|region|global`).
- **Health** — a `FileRepoHealthCheck` (`org.apache.felix.hc.api.HealthCheck`, tag `local`) is
  registered as an OSGi service and auto-discovered by the controller's `CrescoHealthExecutor`. It
  reports `OK` with the catalog size and verifies the repo directory is writable (`WARN` if not),
  self-guarding to `TEMPORARILY_UNAVAILABLE` while starting. It shows up in the health summary and
  in the controller's `gethealthinventory` action (the queryable parallel of `getmetricinventory`).

## Configuration

Passed as the plugin config map (e.g. via the client's `add_plugin_agent`, or the agent INI).

| Key | Default | Meaning |
|---|---|---|
| `scan_dir` | *(unset)* | Directory to watch/catalog (producer mode). |
| `filerepo_name` | *(unset)* | Logical repo name for mesh discovery/sync. |
| `enable_scan` | `true` | Enable the periodic scan. |
| `scan_delay` | `5000` ms | Delay before the first scan. |
| `scan_period` | `15000` ms | Scan interval. |
| `scan_recursive` | `true` | Recurse into subdirectories. |
| `repo_dir` | `filerepo` | Repo directory for on-demand/consumer storage. |
| `instance_id` | *(unset)* | Optional repo instance identifier surfaced in listings. |
| `max_inline_bytes` | `524288` (512 KB) | Max file size returned inline by `getfile`/`getjar`. Keep under the wss frame limit. |
| `stream_buffer_size` | `262144` (256 KB) | Default streamfile chunk size (hard-capped at 768 KB). |
| `transfer_threads` | `8` | Max concurrent streamfile transfers (bounded pool). |
| `db_driver` | `org.apache.derby.jdbc.EmbeddedDriver` | JDBC driver. |
| `db_jdbc` | `jdbc:derby:<data>/derbydb-home/filerepo-db;create=true` | JDBC URL for the catalog. |

## Safeguards

`filerepo` is hardened against the obvious ways a file service gets abused or wedged:

- **SQL injection / bad paths** — every catalog query is a `PreparedStatement`; file paths and MD5s
  are bound parameters, so a path containing a quote can neither corrupt a query nor inject.
- **Path traversal** — `putjar`, `getjar`, and `removefile` resolve names through a canonical-path
  containment check; a `../..` name that escapes the repo dir is refused.
- **Arbitrary-read protection** — `getfile` only serves files present in the catalog (i.e. under the
  configured scan/repo dir); it cannot be used to read arbitrary files on the host.
- **Memory safety** — inline `getfile`/`getjar` are size-capped (`max_inline_bytes`); large files
  must stream. `streamfile` uses a fixed reusable buffer (bounded memory regardless of file size).
- **Transport safety** — `streamfile` chunk size is hard-capped at 768 KB so a chunk + header always
  fits one dataplane frame; short reads are honored (no stale bytes); the read handle is closed even
  on mid-transfer errors (try-with-resources).
- **Integrity** — `putjar` verifies MD5 and deletes+rejects a mismatch; the scan MD5s every new/changed
  file into the catalog.
- **Scan resilience** — files that vanish mid-scan (null MD5) or whose path exceeds the catalog column
  are skipped, so one bad file can't wedge the scanner.
- **Resource hygiene** — bounded, named, daemon thread pools for transfers and peer-sync (no unbounded
  thread creation); the streamfile transfer map is cleaned up on completion; all pools/timers/DB are
  shut down on plugin stop.
- **Scalability** — `getrepofilelist` supports `limit`/`offset` pagination (Derby `OFFSET/FETCH`) so a
  huge catalog is fetched in pages that stay under the wss frame limit; the scan batches the catalog
  into one query instead of a DB round-trip per file.

## Performance

Measured on a single live agent (`run/tests/filerepo_bench.py`, 15/15 correctness checks pass, all
transfers MD5-verified):

| Operation | Result |
|---|---|
| Storage — bulk small files | 2,000 × 512 B scanned + MD5'd + cataloged in **1.33 s ≈ 1,500 files/s** (300/300 integrity) |
| Storage — large file | 64 MB cataloged; filerepo-computed MD5 == real |
| Transfer — inline `getfile` | 64/256/512 KB, MD5-OK (latency-bound, ~110 ms/RPC) |
| Transfer — `streamfile` (any size) | **1 MB @ 9 MB/s · 16 MB @ 149 MB/s · 128 MB @ 566 MB/s**, all MD5-OK |
| Concurrency | 32 concurrent `getfile`, 32/32 MD5 correct |
| Update / delete | change detection and disk+catalog delete verified |

## Client usage

Using the Python client ([pycrescolib](https://github.com/CrescoEdge/pycrescolib)):

```python
import json, base64, os, time, threading
from pycrescolib.clientlib import clientlib
from pycrescolib.utils import decompress_param

c = clientlib("localhost", 8282, "service-key"); c.connect()
m = c.agents.messaging
REGION, AGENT = "my-region", "my-agent"

# Deploy a filerepo watching a directory
cfg = {"pluginname": "io.cresco.filerepo", "jarfile": "filerepo.jar", "persistence_code": "10",
       "scan_dir": "/data/artifacts", "filerepo_name": "artifacts", "enable_scan": "true"}
pid = c.agents.add_plugin_agent(REGION, AGENT, cfg)["pluginid"]

def ex(payload): return m.global_plugin_msgevent(True, "EXEC", payload, REGION, AGENT, pid)

# List the catalog (paginated)
r = ex({"action": "getrepofilelist", "repo_name": "artifacts", "limit": "100", "offset": "0"})
files = json.loads(decompress_param(r["repofilelist"]))       # [{filepath, md5, filesize, lastmodified}, ...]

# Fetch a small file inline (<= max_inline_bytes)
r = ex({"action": "getfile", "file_path": "/data/artifacts/model.cfg"})
data = base64.b64decode(r["file_data"])                        # status 5 means "too big, use streamfile"

# Stream a large file over the dataplane (any size), reassembling by seq_num
def cfg_stream(ident):
    return json.dumps({"ident_key": "stream_name", "ident_id": ident,
                       "io_type_key": "type", "output_id": "output", "input_id": "input"})

def stream_get(path, size):
    sid = "s_" + os.urandom(4).hex(); tid = "t_" + os.urandom(3).hex()
    hoff, hlen = len(tid), len(tid) + 6                        # header = transfer_id + 6-digit seq_num
    parts, done, lock = {}, threading.Event(), threading.Lock()
    def on_bytes(b):
        with lock:
            seq = int(bytes(b[hoff:hlen]).decode())
            parts[seq] = bytes(b[hlen:])                       # strip the header
            if sum(len(x) for x in parts.values()) >= size: done.set()
    rx = c.get_dataplane(cfg_stream(sid), None, on_bytes); rx.connect(); time.sleep(1)
    ex({"action": "streamfile", "file_path": path, "start_byte": "0", "byte_length": str(size),
        "transfer_id": tid, "ident_key": "stream_name", "ident_id": sid})
    done.wait(120); rx.close()
    return b"".join(parts[k] for k in sorted(parts))           # reassemble by seq_num
```

## Testing

- `run/tests/filerepo_functional.py` — end-to-end functional + safeguard test (deploy → scan → Derby
  catalog → `getfile` byte/MD5 round-trip; size guard, path-traversal block, pagination). 10/10.
- `run/tests/filerepo_bench.py` — robustness + performance benchmark (storage, inline + streaming
  transfer of any size, update, delete, concurrency, security). 15/15, all MD5-verified.
- `run/tests/stream_diag.py` — decodes the dataplane streamfile chunk framing.

## Build

```bash
mvn clean package bundle:bundle -DskipTests    # produces a real OSGi bundle
```
Requires JDK 21 (Derby 10.17). The bundle embeds derby + derbyshared + derbytools, dbcp2/pool2, and
gson; it imports `io.cresco.library.*` and the OSGi framework.
