# filerepo

Distributed file/artifact repository plugin for the [Cresco](https://github.com/CrescoEdge) edge
framework. `filerepo` watches a directory, tracks every file in an embedded catalog, and moves
files and plugin jars across the mesh — via inline transfer for small files, byte-range dataplane
**streaming for files of any size**, watched-directory sync between agents, and push-to-remote-repo.

- **Bundle:** `io.cresco.filerepo` (OSGi Declarative Services component, no `Bundle-Activator`)
- **Capability namespace:** `filerepo` (self-describes via `getcapabilities` → the fabric capability inventory)
- **Catalog:** embedded Apache Derby (10.17), one `filelist` table (`filepath` PK, `md5`, `insync`, `lastmodified`, `filesize`)
- **Auto-load:** the agent auto-starts `filerepo` at boot (gated by `enable_filerepo`, default true)
- **Live tier:** a repository can be backed by a live-tier copy or workspace held at its node by a live-tier
  provider service registered by another bundle on the same agent (see [Live-backed repositories](#live-backed-repositories))

---

## Table of contents
- [What it does](#what-it-does)
- [Architecture](#architecture)
- [Operating modes](#operating-modes)
- [Live-backed repositories](#live-backed-repositories)
- [What a repository is: `repostate`, and clearing it](#what-a-repository-is-repostate-and-clearing-it)
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
| `DBEngine` | The Derby catalog. All queries are **parameterized** (`PreparedStatement`); DBCP2 connection pool. A small `repometa` table keeps facts about the repository itself (that files were landed in it, the live-tier roots of this agent). |
| `LiveTierClient` | The client of a live-tier provider service on the same agent, if one is registered: tracked with a `ServiceTracker`, every call bounded. |
| `RepoState` | What `repostate` answers and `clearrepo` checks: `LIVE`, `WORKSPACE_CLEAN`, `WORKSPACE_DIRTY`, `MIRROR` or `ONLY_COPY`. |
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
- **Live-backed** — `live_row` set. The repository is a live-tier copy or workspace held at this node; it runs no
  scanner and no sync, whatever else is configured, and refuses every verb that would serve, write, delete, sync or
  list it (see below).

Sync handshake: producer `discover` → consumer `subscribe` → producer sends `repolistin` (compressed
file diff) → consumer downloads changed files (dataplane) → consumer `repoconfirm`.

## Live-backed repositories

A live tier keeps committed data as ordinary files near compute, on a *live node*: read-only **copies** (recreatable
by realising them again) and writable **workspaces** (seeded from a version, published as a new one). filerepo is a
client of the live tier, never part of it: it holds none of the live tier's code and talks to it only through a
**live-tier provider service registered by another bundle on the same agent**. Without such a provider, filerepo
works exactly as it always has.

### The provider

filerepo looks the provider up in the OSGi service registry with the filter

```
(&(objectClass=java.util.function.Function)(gfs.livetier=local))
```

that is, a `java.util.function.Function<Map<String,Object>,Map<String,Object>>` registered with the property
`gfs.livetier=local`. It is tracked with a `ServiceTracker` through filerepo's own bundle context, so its absence,
its arrival and its departure are all tolerated. The provider admits callers by the **bundle** the framework names
when the service is got (filerepo's symbolic name is `io.cresco.filerepo`), never by anything a message carries. A
provider that does not admit filerepo hands it nothing: filerepo never asks it anything and never counts it present,
but it still tracks it, because it cannot learn that provider's root (see [Live-tier roots](#live-tier-roots)). With
several providers registered, filerepo asks the highest-ranked one that admits it.

Every call is bounded: `live_timeout_ms` for `list` and `changes`, which the provider answers itself, and
`live_core_timeout_ms` for `realise` and `relay`, which it forwards. The two kinds run on separate small pools, so
forwarded calls that are held up elsewhere never starve the local answers that `repostate` and the root checks need.
A hung provider costs a caller its timeout and holds at most `live_max_calls` threads of each pool; while a pool's
threads are all held, further calls of that kind are refused at once. filerepo's message threads are never wedged.

The maps filerepo sends, and the answers it expects (every answer carries `status`, `10` for success, and
`status_desc` on a refusal):

| Op | Request | Answer |
|---|---|---|
| `list` | `{op: list}`, or `{op: list, row}` for one row | `rows`: one map per row held at this node (`row`, `state`, `mode` `copy` or `workspace`, `files`, `bytes`, `lease_until`, `damaged`, and what it holds), and `root`, the directory the rows are held under. Asked for one `row`, a provider may answer with that row alone (one lookup, whatever the number of rows); filerepo finds the row either way |
| `changes` | `{op: changes, row}` | a workspace's cached change summary: `state` `clean` or `dirty`, `changed`, `refused`, `as_of`, and `scanning` while its first walk runs |
| `realise` | `{op: realise, params: {collection, vid, prefix, lease_ms, request_id, mode: copy}}` | the realise reply (`job_id`); made by the node itself, **copies only, at this node only**: filerepo refuses a `mode` other than `copy`, or a `node`, before anything is sent |
| `relay` | `{op: relay, request: {...}}` | the reply to a request **already signed by its end user**, forwarded unchanged; it comes back as it came |

What a provider answers may name collections and paths. It stays inside the agent: filerepo's replies carry states,
counts and ids, never a path, a collection or a version.

### Configuration

A repository is live-backed when its plugin configuration names a live-tier row:

```
live_row = <the row id, as the live tier reported it when the copy or workspace was realised>
```

The backing is read once, when the plugin starts. No message can set, change or clear it: to back a repository by a
different row, configure a new plugin instance. A live-backed repository:

- runs **no scanner, no broadcast and no subscription**, whatever `scan_dir`, `filerepo_name` or `enable_scan` say;
- answers only `repostate`, `liverelay`, `liverealise`, `getmetrics` and `getcapabilities`, none of which names a
  path;
- **refuses every other verb** (including any added later) with `status 9` and `refusal=live-backed`: `getfile`,
  `streamfile` and `getjar` (serve), `putfiles` and `putjar` (write), `removefile` and `clearrepo` (delete, `force` or
  not), `putfilesremote` (serve and push), `repolistin` (subscriber sync), `repoconfirm` (producer sync), and also
  `repolist`, `getrepofilelist`, `streamfilecancel` and `getscandir` (which would name where the row is held). Its
  files are read through the filesystem at the node only. The refusal needs no provider: it holds even when the
  provider cannot be reached.

Its health check reports `OK` while a provider admitting filerepo is registered, `WARN` otherwise; it never calls
the provider.

### Verbs

- **`liverelay`** carries a request signed by its end user (a compressed JSON object of strings in `request`)
  unchanged through the provider, and returns the reply as it came (`reply`, compressed, with its own status in
  `reply_status`). Whoever signed the request is the one authorised and audited; filerepo adds and signs nothing.
  Any repository may carry a relay. `status 10` means the provider answered (the answer may itself be a refusal),
  `8` no request, `11` the provider did not answer.
- **`liverealise`** keeps a live-backed repository's copy live: it realises the same copy again, as the node itself
  (a copy still held is a hit, which raises its lease), with an optional `lease_ms` and `request_id`. What is
  realised comes from the provider's own row; nothing is taken from the request, and only `reply_status` and `job_id`
  come back. A workspace, a row no longer held at this node, or a repository that is not live-backed is refused
  (`status 9`, `refusal` = `workspace`, `not-held`, `not-live-backed`); a refused realise is `9` with
  `refusal=realise`.

### Live-tier roots

Files under a provider's live root are the provider's, whatever any repository's configuration says. filerepo
learns the root of every registered provider from its `list` answer (once per registration) and remembers it in its
catalog, so the rule holds while the provider is away and across restarts. Roots can also be named in configuration
(`live_roots`, comma-separated), which makes them hold from the start, before any provider has registered (for
example on a fresh catalog). For every such root:

- `getfile`, `streamfile`, `putfilesremote` and `putfiles` refuse any file that lies in a live-tier root, symbolic
  links resolved (`status 9`, `refusal=live-root`);
- a repository whose `scan_dir` or `repo_dir` lies in, or contains, a live-tier root is refused like a live-backed one,
  and its scanner skips;
- the scanner skips links into a root and never re-offers a cataloged file that lies in one, the sync downloader
  refuses destinations that lead into one, and a clear forgets cataloged files in a root without deleting them;
- while any registered provider's root is not known, **every** file path is refused (`refusal=live-root`, "cannot be
  checked"), whatever roots are already known: a provider whose `list` has not answered yet or failed (retried after
  30 s, or as soon as it registers again), a provider that does not admit filerepo (for as long as it is registered:
  admit `io.cresco.filerepo` in its configuration), or a framework whose registrations cannot be followed.

## What a repository is: `repostate`, and clearing it

`repostate` answers what would be lost with the repository. It comes from the provider's cached answers (a
live-backed repository) or from configuration and filerepo's own catalog (any other), **never from markers in the
tree or a walk**: its cost does not grow with the number of files. A live-backed repository asks for its own row's
listing, and reuses a successful one for `live_list_cache_ms`, so a stream of `repostate` calls costs the provider at
most one `list` per period.

| State | Meaning |
|---|---|
| `LIVE` | backed by a live copy: read-only here, recreatable by realising it again |
| `WORKSPACE_CLEAN` | backed by a workspace whose cached change summary is clean |
| `WORKSPACE_DIRTY` | backed by a workspace with unpublished changes, or whose changes cannot be read now, or whose first walk is still running (never counted clean unseen) |
| `MIRROR` | a subscriber mirror: what it holds, its producer holds |
| `ONLY_COPY` | the origin of what it holds: a producer, an on-demand repository, or a mirror that files were landed in directly by `putfiles` or `putjar` (it stays so until a clear removes everything) |

Besides `state`, the reply carries `backing` (`live` or `local`) and facts such as `role` (`producer`, `subscriber`,
`on-demand`), `landed`, `live_row`, `mode`, `row_state`, `files`, `bytes`, `changed` and `as_of`. `status` is `10`
with a state, `9` when the live row is not held at this node, and `11` when the provider did not answer.

`clearrepo` takes the **repository gate** before it checks, refuses an `ONLY_COPY` unless `force=true`
(`status 9`, `refusal=only-copy`), and holds the gate through the delete. The gate is a fair read-write lock: a clear
holds it exclusively, while scans and every landing (`putfiles`, `putjar`) hold it shared, so no file filerepo lands
can arrive between a clear's check and its delete; scans and landings still run side by side. Every wait for the gate
is bounded (30 s): a clear or landing that cannot have it says so and does nothing.

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
are base64 in the reply. A refusal of the live-tier and clear rules is `status 9` (and `status_code 9`) with a
`refusal` naming the rule: `live-backed`, `live-root`, `only-copy`, `not-live-backed`, `not-held`, `workspace`,
`realise`. `status 11` means the live-tier provider did not answer (absent, timed out or saturated): retry later.

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
| `getscandir` | — | `scan_dir` | Report the configured scan directory. Refused on a live-backed repository. |
| `removefile` | `repo_name`, `file_name` | `status` | Remove a file (disk + catalog row). Path-traversal-safe. |
| `clearrepo` | `repo_name`, optional `force` | `status`, `state`, `refusal` | Delete all files in a repo (disk + catalog). An only copy is refused unless `force=true`. |
| `repostate` | — | `state`, `backing`, facts | What the repo is: `LIVE`, `WORKSPACE_CLEAN`, `WORKSPACE_DIRTY`, `MIRROR` or `ONLY_COPY`. |
| `liverelay` | `request` (json, req) | `reply` (json), `reply_status` | Relay a request signed by its end user, unchanged, through the live-tier provider. |
| `liverealise` | optional `lease_ms`, `request_id` | `job_id`, `reply_status` | Realise a live-backed repo's copy again, as the node (keeps it live). |
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
| `live_row` | *(unset)* | Back this repository by that live-tier row (a copy or a workspace). Read once at start. |
| `live_timeout_ms` | `5000` | Bound on a provider call answered by the provider itself (`list`, `changes`). |
| `live_core_timeout_ms` | `25000` | Bound on a provider call the provider forwards (`realise`, `relay`). |
| `live_max_calls` | `4` | Provider calls of each kind (answered locally; forwarded) in flight at once; more are refused at once. |
| `live_list_cache_ms` | `1000` | How long a live-backed repository reuses its row's successful listing (`0`: never). |
| `live_roots` | *(unset)* | Comma-separated directories that are live-tier roots on this agent, refused from the start (before any provider registers). |

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
- **Only copies** — `clearrepo` refuses a repository that is the only copy of what it holds unless `force=true`,
  deciding under the repository gate that every landing also takes, so nothing lands between the check and the delete.
- **Live tier** — a live-backed repository serves, writes, deletes, syncs and lists nothing, provider or not; files
  in a live-tier root are never served, landed, deleted or synced through any repository, and while any registered
  provider's root is unknown no file path is.
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

`mvn test` runs filerepo's JUnit 5 suite (part of every `package` build): the live-tier client against a real
`ServiceTracker` over a fake registry (absence, admission by bundle, coming and going, a hung provider, forwarded
calls that never starve local ones, the reuse of a row's listing, the exact request maps), every repository state,
every refused verb on live-backed repositories with and without a provider, the clear refusals and the gate
(including two concurrent clearrepo/putfiles sequences), the live-root rules (including failing closed while any
registered provider's root is unknown, several providers, and configured roots), and the touched verbs answering as
before on ordinary repositories. It needs no agent: filerepo's own engines run over a
stand-in agent, with the real Derby catalog in temporary directories.

Live checks against a running agent:

- `run/tests/filerepo_functional.py` — end-to-end functional + safeguard test (deploy → scan → Derby
  catalog → `getfile` byte/MD5 round-trip; size guard, path-traversal block, pagination). 10/10.
- `run/tests/filerepo_bench.py` — robustness + performance benchmark (storage, inline + streaming
  transfer of any size, update, delete, concurrency, security). 15/15, all MD5-verified.
- `run/tests/stream_diag.py` — decodes the dataplane streamfile chunk framing.

## Build

```bash
mvn clean package bundle:bundle                # runs the tests, then produces a real OSGi bundle
mvn clean package bundle:bundle -DskipTests    # the bundle only
```
Requires JDK 21 (Derby 10.17). The bundle embeds derby + derbyshared + derbytools, dbcp2/pool2, and
gson; it imports `io.cresco.library.*`, the OSGi framework and `org.osgi.util.tracker` (exported by the framework).
JUnit is test scope only and never embedded.
