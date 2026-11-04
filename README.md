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
  `getrepofilelist` / `putjar` etc. against `repo_dir` (default `<agent data dir>/filerepo/repos/<filerepo_name, or
  "default">`, absolute).
- **Live-backed** — `live_row` set. The repository is a live-tier copy or workspace held at this node; it runs no
  scanner and no sync, whatever else is configured, and refuses every verb that would serve, write, delete, sync or
  list it (see below).

Every verb uses **one repository directory**: `scan_dir`, else `repo_dir`, else
`<agent data dir>/filerepo/repos/<filerepo_name, or "default">`. (Before 1.3 hardening `putjar` used `repo_dir` with
the relative default `filerepo`, so on a producer a jar landed in a stray `./filerepo` that was never scanned or served;
filerepo logs a WARNING at start if such a directory exists.)

**Where a repository's state lives.** Nothing is kept in the plugin's own data directory, which the controller
deletes when it stops a plugin added at runtime: the default repository directory is under the agent's data
directory, by repository name, and the **catalog** (the Derby database: what was cataloged and landed, the sync state,
the producer pin, the mirror's generation) is in `<agent data dir>/filerepo/state/<SHA-256 of the repository
directory's real path>`, so the same directory finds the same catalog whatever plugin id serves it and however its
path is spelled: a redeployed plugin keeps its catalog and its subscribers. One plugin serves one repository directory
at a time: it is locked by its real path, and by its file key (device and inode, `<agent data dir>/filerepo/locks`),
so a second plugin over the same directory (another spelling, a symlinked parent, a bind mount, or a second unnamed
default repository on one agent) opens no catalog, refuses every verb (`refusal=catalog-closed`) and says so in its
health check. A start that fails releases the lock. A catalog or default repository from before is moved there once,
at start: a rename, journaled (`migration.journal` in the state directory) so a crash between the rename and the
rewrite of the catalog's paths is completed at the next start; a catalog on another filesystem is copied and verified
file by file before it is adopted (an interrupted copy is discarded and made again); a default repository on another
filesystem is not copied at all. Nothing is ever deleted; if both the old and the new default directory hold files,
the old one is left alone and an ERROR names both.

**Who a mirror's producer is: its node and its key.** Each producer has its **own signing key** (EC P-384,
SHA384withECDSA, made once with the JDK at its first start and kept owner-only as `producer.key` in the repository's
state directory, `<agent data dir>/filerepo/state/<...>`, which only the plugin holding the directory's lock opens). A
producer is known by what nothing else can forge: the **node it runs on**, from the broker's stamp of its caller
(`<region>/<agent>`), with the repository name and the **fingerprint of its key** (SHA-256 of its public key), which it
proves by signing each mirror's fresh nonce in its answer. Its offers and deletes (every `repolistin` page) are signed
with the same key and checked by the mirror. Not its plugin id (a producer redeployed over the same directory finds the
same key), not the directory it reports (shown to admins only), and not anything a holder of a shared secret could
claim: another plugin on the producer's agent cannot sign as it. The producer's `repostate` shows its `producer_key`
and the `producer_pin` to configure on its mirrors (`region/agent#<fingerprint>`); its log names it at the first start.
Each mirror has a **pinned producer**, set only by an admin, and only it may add, change or delete files there; a
mirror with no pin **syncs nothing**:

- **`sync_producers`** (recommended): an admin's comma list of pins, `region/agent#<fingerprint>` (from the producer's
  `repostate`). The mirror asks only those nodes. A pin without a fingerprint (`region/agent`, `region/agent:/dir`,
  `region/agent/pluginid`, the round-2 and round-3 forms) pins nothing, with an ERROR: a node or a directory is no
  identity.
- **`syncpin`** (no `sync_producers`): an admin sends it to the mirror (`confirm=<the mirror's directory>`,
  `producer=region/agent#<fingerprint>`); the pin is kept in the catalog. Nothing is ever pinned because it answered,
  first or at all. Without `sync_producers` and before a `syncpin`, the mirror syncs nothing: it still asks the producers
  of its name, and `repostate` lists those that answered and proved a key as `candidate_producers`
  (`region/agent#<fingerprint> (directory)`), for the admin to choose from (a WARN an hour per producer names the
  `syncpin` command too). `syncpin reset=true` forgets the pin: the mirror syncs nothing again until an admin pins one.
  Changing the pin, or resetting it, orphans what the old producer delivered (kept, never deleted, changed by nobody
  else) unless `adopt=true` hands it to the new producer (`from=` names the old producer, a pin or the holder
  `repostate` shows, when it is not the previous pin). A new generation follows, so the new producer offers everything.
- Other producers of the name are still known (`repostate` counts `unpinned_producers`), and so is a plugin that answers
  without a valid signature (it names no key and is never pinned), but everything they offer is refused and everything
  they delete ignored: they settle at the producer (logged there as refused) and nothing at the mirror changes. Any
  mirror, or any bundle on a mirror's or a producer's agent, can run a producer of the name; it is never the pinned one.
- **Orphaned content.** What a producer that is not pinned now delivered here (re-pinned or reset without `adopt`, a
  producer that moved or lost its key, a pin by directory from an earlier build of this branch) is frozen: kept, never
  deleted, and changed by nobody but that producer. `repostate` shows it: `orphaned_files` (counted up to 10000),
  `orphaned_from` (each producer's holder and identity) and `orphaned_resolve`, the command that hands it to the pinned
  producer (`syncpin confirm=<dir> adopt=true from=<holder> producer=<pin>`; where `sync_producers` is configured, this
  hand-over to a configured producer is the one `syncpin` taken there).
- **Keep the key.** A producer redeployed over the same directory keeps its key; a producer moved to another directory
  or node, or whose `producer.key` was lost, is a new producer: re-pin its mirrors (`adopt=true` carries the old content
  over). A key file that cannot be read is never replaced (the producer serves no mirror and says why, in its log and its
  `repostate` `producer_key_error`): restore it from a backup of the state directory, or remove it and re-pin. A key file
  found readable by others is made owner-only again, with an ERROR (replace it if it may have been copied).
- **The in-JVM boundary.** Code running in the agent's JVM can read any file the agent's user can, the key included.
  Per the caller-authorization contract (`docs/caller-authorization.md` in the controller), in-process plugins are
  trusted code (only signed plugins are installed): the key keeps out every other node and every plugin that is not
  trusted code of the producer's agent. A second filerepo over the same directory never opens the state directory (the
  real-path and file-key lock) and so never reads or makes the key.
- **`sync_role`** (`producer`, `mirror` or `none`; unset: a scan dir makes a producer, a repo_dir a mirror): a
  repository configured as a mirror never answers as a producer, even with a scan dir.
- **`sync_key`** (optional): one secret per repository name, on the producer(s) and every mirror. It is an additional
  proof of membership, never an identity: a keyed mirror ignores a discover without a fresh HMAC proof of it (a forged
  discover costs nothing, and sends nothing), takes a `reposubscribed` only with a proof bound to both addresses, its
  nonce, its generation and the producer's directory; a keyed producer refuses a mirror that does not prove it.

**Sync protocol 2.** A producer broadcasts `discover` on the dataplane (only an address, with its proof); a consumer
answers with `reposubscribe`, a MsgEvent to that address; the producer takes it only as a producer of the same
`filerepo_name` speaking protocol 2 and answers `reposubscribed`; a consumer takes a producer only from that answer,
and **both sides know each other by their stamped caller (`cresco_caller`), never by a message's source fields**.
Then the producer sends `repolistin` pages (files, plus `repodeletes`) → the consumer pulls each changed file with the
producer's own `streamfile` **over the dataplane**, verifies it and moves it into place → `repoconfirm`.

- **Over the dataplane, verified.** Only one small `streamfile` request per file crosses the control plane; the bytes
  ride the dataplane. Chunks are written at their `offset`; a range lost in transit is asked for again
  (`sync_stream_rounds`); a stall (`sync_stream_idle_ms` without a frame) cancels and retries. A file is committed (an
  atomic rename from a `.filerepo-*.part` staging file) only when its SHA-384 equals the sender's terminal frame and
  its content hash equals the producer's offer; otherwise it is reported failed and offered again. The old path
  (controller `getfiledata` RPCs) is no longer used.
- **Per subscriber.** The producer keeps, for each subscriber, what it still needs (the catalog's `pending` and
  `pendingdel` tables): this cycle's changes go to every subscriber at once, and what one subscriber failed, or a
  subscriber that is new or was cleared (it advertises a new *sync generation*), is offered to that subscriber alone
  until it confirms, `sync_pages_per_cycle` pages a cycle, read by a key cursor in path order so nothing slides past
  while the catalog changes. A confirm settles only its own transfer: transfer ids carry the producer instance's
  random epoch, and a transfer is bound to the subscriber it was offered to, to the file versions it offered, and to
  the subscriber's generation it was offered for.
- **Generations.** A mirror's sync generation is kept in its catalog, and a marker of it inside the mirror's directory
  (`.filerepo-staging/generation`), so a plain restart resumes where it left off (nothing is offered again), while a
  directory that came back without its marker (a new, replaced or reformatted volume), with another generation's, or
  empty though the catalog records what its producer delivered (an `rm -rf` instead of `clearrepo`), starts a new
  generation at once (checked at start and at most once a minute): its producer offers it everything again, and
  nothing is deleted. A clear renews it before the first file goes. An offer for an earlier generation is
  refused by the mirror; a transfer the mirror was applying when it was cleared commits nothing more (each file and
  delete is committed under the repository gate, only while the generation is the one the transfer was taken for),
  and its confirm settles nothing at the producer. A producer resets a subscriber for a new generation (everything
  offered again) at most a few times at once, then once per `sync_reset_interval_ms`.
- **A partial loss is refilled.** A mirror scans too (at start, then every `scan_period` after `scan_delay`): its
  generation marker, and the files its catalog says its pinned producer delivered (the claims, a page of
  `sync_refill_check_batch` a run, going on where the last run stopped) that are gone from the directory (files deleted
  by hand, `find -type f -delete`, a failed disk under one subtree, its directories kept). It asks that producer for
  them (`reporefill`, at most 1000 names a request, each name at most once per `sync_offer_timeout_ms`); the producer
  offers the ones it still catalogs again to that mirror alone (a burst of requests, then one a second), and they are
  fetched and verified as any offer. A file gone from a mirror is never a delete: a mirror produces nothing.
  `filerepo.sync.refill.asked` (mirror) and `filerepo.sync.refill.offered` (producer) count them.
- **Several pinned producers of one name** (`sync_producers` listing several). A mirror records, per name, which
  producer's content it holds: a *fetched* claim (the bytes came from that producer) or a *matched* one (a producer
  offered exactly the content already there). The producer with the fetched claim updates and deletes the file; a
  matched claim never blocks its updates, but a file another producer still claims is never deleted (only the deleting
  producer's claim goes).
- **A silent producer's files.** A producer not heard from for `sync_holder_ttl_ms` (30 days) has its claims
  orphaned: its files may then be replaced by **it alone** (no other producer, pinned or not), and **deleting them
  always needs an admin**: such a delete is held at the mirror (`repostate` `deletes_held`, `deletes_hold_id`, a health
  WARN, an ERROR) until an admin sends `releasedeletes` to the mirror with `confirm` and that `hold_id`.
- **A mirror's own content is safe.** A consumer records which producer delivered which content under which name
  (the catalog's `mirrored` table). Sync never overwrites a file landed there (`putfiles`, `putjar`), nor content that
  is not that producer's, nor another producer's; it never deletes a file another producer still holds there; and a
  name that collides with an existing one in case only (on a case-insensitive filesystem) is refused, never written or
  deleted. A refusal is reported as a failed file and logged; `filerepo.sync.conflicts` counts them.
- **Deletes.** A file gone from the scan dir (or removed with `removefile`) becomes a *tombstone*, pending for every
  subscriber until it applies it. A tombstone is purged (after `sync_tombstone_ttl_ms`) only when no subscriber the
  producer keeps still has it pending: a subscriber is kept, with everything it owes, for `sync_subscriber_ttl_ms`
  (7 days) of silence; one owing confirms for `sync_lag_alarm_ms` (a day) is an alarm (an ERROR a day, the
  `filerepo.sync.subscribers.lagging` gauge, a health WARN); one silent longer is dropped with an ERROR (it may hold
  files deleted since: clear it), and told so if it comes back. A consumer deletes its copy only if that producer
  delivered exactly the deleted content and no other producer holds it.
- **Mass-delete guard.** Held, never offered:
  - always, every delete under a **mount point that vanished** (the scanner records which directories of the tree sit
    on another device than their parent; one that is gone, or no longer a mount point, was unmounted), and everything
    gone when the scan dir itself changed device or is suddenly empty;
  - every delete of a **whole subtree** that lost all of its cataloged files, when they are at least
    `sync_delete_guard_subtree_min` (10) files **and** more than `sync_delete_guard_subtree_fraction` (1%) of the
    catalog: a nested volume going away where device ids cannot tell it, while `rm -rf` of a small directory in a
    large repository goes through;
  - everything when the remaining loss is at least `sync_delete_guard_min` (3) files **and** more than
    `sync_delete_guard_fraction` (10%) of the catalog, counting the deletes recorded in the last
    `sync_delete_guard_window_ms` (15 min) too: a loss spread over cycles (a deletion still running while a scan reads
    the tree, a slow drip) is held once it adds up, so what gets through unheld is bounded by the threshold. (In a small
    repository, a few deletes are a large fraction: raise the fraction, or shorten the window, for a small churny one.);
  - everything when the **originals** deleted or rewritten in the last 24 hours, with this cycle's, are at least the
    minimum **and** more than `sync_delete_guard_day_fraction` (25%) of the originals: a slower drip is held within a
    day;
  - everything when the originals deleted or rewritten since the last admin acknowledgement, with this cycle's, are more
    than `sync_delete_guard_total_fraction` (50%) of the originals: however slowly a catalog is drained or churned, never
    past half of it without an admin. Each longer budget is at least as large as the shorter ones (it follows a raised
    fraction), and `0` turns it off.

  The day and the total count **gross**: every original deleted (and every original rewritten, which the rewrite guard
  counts too) counts, whatever is added meanwhile, so deleting `x` and writing `x.locked` is caught however the
  catalog's size moves. An **original** of a period is a file first cataloged before the period began (a day ago, or
  the last acknowledgement if later; for the total, the last acknowledgement, which is the first scan until an admin
  releases a hold); files added since neither count nor dilute the budget, and the period's originals are the budget's
  base. What the guards let through is kept in the catalog (the `guardlog` table), so a restart forgets nothing, on the
  guards' own clock: monotonic while the plugin runs (a jump of the wall clock never ages a record), and carried across
  a restart, where the time down counts as elapsed only up to an hour (a wall clock behind the last record, or more than
  an hour past it, is logged; no budget is ever cleared by a clock). A release consumes only the budgets that held what
  it releases (the window, the day, the total) and acknowledges the deletes it releases (they count toward nothing);
  every other budget keeps counting: releasing an unmounted volume's hold does not reset a drip's day.

  The rows stay; `repostate` reports `deletes_held` and `deletes_hold_id`, the health check WARNs and
  `filerepo.sync.deletes.held` counts them, every cycle, until the files return or an admin sends `releasedeletes`
  with `confirm=<the repository directory>` **and `hold_id`** (a release that names no hold, or another, is refused).
  A release is bound to the digest of exactly the held set, spent by the next scan whatever that finds, and expires
  after `sync_release_ttl_ms`: it never releases a later or another loss. A scan that could not read part of the tree
  takes nothing for deleted.
- **Rewrite guard.** A cycle that rewrites at least `sync_rewrite_guard_min` (10) existing files **and** more than
  `sync_rewrite_guard_fraction` (25%) of the catalog (ransomware, the wrong volume or an old snapshot on the mount
  point) has those rewrites held: offered to no subscriber, new ones included, while every mirror keeps its copy.
  A held file changed again stays held; one changed back to the content the mirrors have is no longer held and offered
  to nobody. The same longer horizons hold a slower rewrite: originals rewritten (each once) or deleted in the last 24
  hours more than `sync_rewrite_guard_day_fraction` (25%) of the originals, or since the last acknowledgement (a
  `releaserewrites`) more than `sync_rewrite_guard_total_fraction` (50%); each at least the minimum, kept across
  restarts, and consumed by a release only for the budgets that held it, as for deletes. `repostate` reports `rewrites_held` and
  `rewrites_hold_id`, the health check WARNs, until an admin sends `releaserewrites` with `confirm` and that `hold_id`.
  A touch (a new time, the same content) is no change at all.
- **Bounded.** A consumer keeps at most `sync_pending_per_peer` diffs per producer and `sync_max_peers` producers,
  on `sync_threads` threads with `sync_queue_max` waiting; past any bound a diff is refused as busy
  (`status_code 11`) and the producer offers it again next cycle. A producer offers to `sync_offer_threads`
  subscribers at once and a stop ends a waiting cycle at once. A consumer asks each producer at most once per
  `sync_subscribe_retry_ms` (forged discovers, which carry no proof of the key, cost nothing and delay no producer);
  old-protocol peers are remembered in a bounded table and warned about at most once a minute.

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

`clearrepo` wipes the whole repository directory, so it is armed explicitly: `confirm` must name that directory
exactly (its `scan_dir`, or `repo_dir` when it has none, as configured or as its absolute path; otherwise
`refusal=unconfirmed`); `repo_name`, when given, must name this repository (`refusal=repo-name`). It never clears a
protected directory (`refusal=protected-dir`): the filesystem root, a system directory, or one that is or contains
the home directory, the agent's working directory, the JVM, the agent's data directory or this plugin's data
directory; nor one that is or holds a live-tier root, force or not. A catalog row outside the directory is forgotten
and its file left alone; links are removed, never followed.

`clearrepo` takes the **repository gate** before it checks, refuses an `ONLY_COPY` unless `force=true`
(`status 9`, `refusal=only-copy`), and holds the gate through the delete. `force=true` needs the **admin** role
(`refusal=force-needs-admin`). The gate is a fair read-write lock: a clear
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
handles files of **any size** with bounded memory.

- **The file is checked on the descriptor it is read through.** It is opened first (never through a link as the
  named file), then checked: still the same file the path named (file key), a regular file, inside the repository
  directory with every link resolved, in no live-tier root, and in the catalog. Its size is the open file's own, and
  the bytes come through that descriptor, so nothing swapped in after the check is ever sent.
- **`transfer_id`** is 1-128 of `[A-Za-z0-9_.:-]`, unique among the caller's own transfers (a duplicate is refused,
  `status 8`), or generated when absent; the reply carries it. `streamfilecancel` cancels only the caller's own
  transfer, running or still queued.
- **`ident_key`** must be a property name (`[A-Za-z_][A-Za-z0-9_]*`, not a selector word, not `JMS*`, not one of the
  frame's own properties).
- Every chunk carries the properties `<ident_key>=<ident_id>`, `transfer_id`, `seq_num` and `offset` (its byte
  offset in the file).
- **A terminal frame ends every accepted transfer**: an empty message with `transfer_end=true`, `transfer_status`
  (`10` complete, `9` cancelled, `8` failed, e.g. the file shrank while streaming), `transfer_desc`, `total_bytes`,
  and on success `sha384` — the SHA-384 of the bytes sent. A receiver verifies it before it commits anything.
- **Bounded and fair**: `transfer_threads` running (at most `transfer_caller_fraction` of them for one caller, and
  `transfer_sync_reserve` of them kept for subscribers' sync fetches), `transfer_queue_max` waiting,
  `transfer_max_per_caller` per caller; past a
  bound the request is refused as busy (`status 11`), never queued without limit.

Each chunk is a dataplane binary message; through wsapi a client sees it framed as:

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

Roles (`@CrescoAction(roles=...)`, enforced by the agent before filerepo runs; see the controller's
`docs/caller-authorization.md`): **`clearrepo`, `putjar`, `removefile`, `putfiles`, `putfilesremote`,
`releasedeletes`, `releaserewrites`, `syncpin` and `liverealise` need `infra` or `admin`**; `repolistin`, `repoconfirm`, `reposubscribe`,
`reposubscribed` and `reporefill` need `plugin` (another filerepo); every other action is open to callers of this agent's tenant and to
`infra`. Wherever filerepo itself needs to know who called (the owner of a transfer, an admin's `force`, a sync peer)
it reads only the stamped caller; a message without one is anonymous only in explicit insecure development mode
(`broker_security_enabled=false` given to the agent), and otherwise refused. A refused call answers `status_code=403`.

| Action | Params | Returns | Purpose |
|---|---|---|---|
| `getrepofilelist` | `repo_name` *(req)*; optional `limit`, `offset` | `repofilelist` (json), `repo_total` | List catalog rows (path/md5/size/mtime). Paginated (Derby `OFFSET/FETCH`) — use `limit`/`offset` for large repos. |
| `repolist` | — | `repolist` (json) | Plugin jars in the repository directory (those with a `Bundle-SymbolicName`) + server (region/agent/pluginid). |
| `getfile` | `file_path` *(req)* | `file_data` (bytes), `file_metadata` | Return a **cataloged** file in the repository directory inline. Refused (`status 5`) if larger than `max_inline_bytes`; refused (`status 9`) if not in the catalog, not a regular file, or outside the directory. |
| `getjar` | `action_pluginname`, `action_pluginmd5` | `jardata` (bytes), `status` | Return a plugin jar by name + content hash (size-guarded). The bytes sent are rechecked against the hash. |
| `putjar` *(infra/admin)* | `pluginname`, `jarfile`, `jardata` (bytes); optional `md5`, `version` | `uploaded`, `md5-confirm`, `sha384` | Store a plugin jar. **Accepted only if signed** by a signer chaining to `plugin_signing_truststore` (`refusal=signature` otherwise); the repo computes SHA-384 and the content hash itself; a supplied `md5` that differs is refused; `pluginname` must be the jar's `Bundle-SymbolicName`; `jarfile` is a plain name. Staged, then moved into place. |
| `putfiles` *(infra/admin)* | `repo_name`, `file_hashes` (json: name → content hash, req), `overwrite` (bool) | `stored`, `unchanged`, `skipped`, `failed` (json) | Land pushed files (MsgEvent attachments), each verified before an atomic rename. `10` only when every file was stored or already held. |
| `putfilesremote` *(infra/admin)* | — | `status 9`, `refusal=removed` | **Removed**: always refused (a path in a live-tier root is still refused as such first). An admin sends `putfiles` to the target repository directly. |
| `streamfile` | `file_path`, `start_byte` (long), `byte_length` (long), `ident_key`, `ident_id`; optional `transfer_id`, `buffer_size` | `status`, `transfer_id` | Stream a byte-range over the dataplane in chunks, ending with a terminal frame. Any size. |
| `streamfilecancel` | `transfer_id` | `status_code` | Cancel one of the caller's own transfers. |
| `getscandir` | — | `scan_dir` | Report the configured scan directory. Refused on a live-backed repository. |
| `removefile` *(infra/admin)* | `file_name`; optional `repo_name` | `status` | Remove a file (disk + catalog row), contained to the repository directory; on a producer, a delete for its subscribers. |
| `clearrepo` *(infra/admin)* | `confirm` *(req)*; optional `repo_name`, `force` | `status`, `state`, `refusal` | Delete every file in the repository directory. An only copy needs `force=true` (admin). |
| `releasedeletes` *(infra/admin)* | `confirm` *(req)*, `hold_id` *(req)* | `released`, `hold_id` | On a producer: let the next scan record exactly the deletes the mass-delete guard holds. On a mirror: apply the held deletes of a silent producer's files. Refused without `hold_id`, or if it names another set. |
| `releaserewrites` *(infra/admin)* | `confirm` *(req)*, `hold_id` *(req)* | `released` | Offer exactly the rewrites the rewrite guard holds (refused without `hold_id`, or if it names another set). |
| `syncpin` *(infra/admin)* | `confirm` *(req)*, `producer` (`region/agent#<fingerprint>`) or `reset=true`, optional `adopt`, `from` | `pinned_producer` | Pin a mirror's producer, or forget the pin (the mirror then syncs nothing); `adopt=true` hands the previous (or the `from`) producer's content to the new one; a new generation follows. Where `sync_producers` is configured only `adopt=true from=... producer=<a configured pin>` is taken. |
| `repostate` | — | `state`, `backing`, facts | What the repo is: `LIVE`, `WORKSPACE_CLEAN`, `WORKSPACE_DIRTY`, `MIRROR` or `ONLY_COPY`. |
| `liverelay` | `request` (json, req) | `reply` (json), `reply_status` | Relay a request signed by its end user, unchanged, through the live-tier provider. |
| `liverealise` *(infra/admin)* | optional `lease_ms` (at most `live_lease_max_ms`), `request_id` | `job_id`, `reply_status` | Realise a live-backed repo's copy again, as the node (keeps it live). |
| `repolistin` *(plugin)* | `repolistin` (json diff), optional `repodeletes` (json), `transfer_id` | `status_code` | Sync consumer side: `10` accepted, `9` not a subscribed producer, `11` busy. |
| `reposubscribe` *(plugin)* | `filerepo_name`, `sync_gen`, `sync_proto` | — (answered with `reposubscribed`) | Sync protocol 2: a consumer asks a producer to serve it. |
| `reposubscribed` *(plugin)* | `accepted`, `filerepo_name`, `sync_proto`, `refusal` | — | Sync protocol 2: a producer's answer. |
| `repoconfirm` *(plugin)* | `transfer_id`, optional `failedfiles`, `faileddeletes` | — | Sync handshake ack (from a subscriber only). |
| `reporefill` *(plugin)* | `filerepo_name`, `sync_gen`, `names` (json, at most 1000) | `refill` | A subscriber of this generation asks for files it lost: those still cataloged are offered to it again (to it alone). Never a delete. |
| `getmetrics` | — | `metrics` (json) | Central metrics — `MeasurementEngine` gauges (see below). Folded into the controller's `getmetricinventory`. |
| `getcapabilities` | — | `capabilities` (json) | Self-describing capability document (LLM tool specs). |

## Central health & metrics

filerepo is wired into Cresco's two central observability systems — the **same mechanisms every
other plugin uses** (sysinfo is the reference), so metrics and health are consistent fabric-wide:

- **Metrics** — a unified `MeasurementEngine` exposes `filerepo.files.count` (catalog size),
  `filerepo.active.transfers` (in-flight `streamfile` count, running and queued), `filerepo.transfer.queued`,
  `filerepo.transfer.rejected`, `filerepo.sync.queued`, `filerepo.sync.active`, `filerepo.sync.rejected`,
  `filerepo.sync.refused`, `filerepo.sync.fetched`, `filerepo.sync.failed`, `filerepo.sync.verify.failed`,
  `filerepo.sync.deleted`, `filerepo.sync.tombstones`, `filerepo.sync.deletes.held`, `filerepo.sync.conflicts` and
  `filerepo.sync.subscribers` as gauges. The `getmetrics` action
  returns the standard `getAllMetrics()` JSON, which the controller's `getmetricinventory` fan-out
  aggregates across the mesh. Query one node with `getmetrics`, or the whole fabric with
  `getmetricinventory` (`action_scope=node|region|global`).
- **Health** — a `FileRepoHealthCheck` (`org.apache.felix.hc.api.HealthCheck`, tag `local`) is
  registered as an OSGi service and auto-discovered by the controller's `CrescoHealthExecutor`. It
  reports `OK` with the catalog size and verifies the repo directory is writable (`WARN` if not, and `WARN` while
  the mass-delete guard holds deletes),
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
| `scan_period` | `15000` ms | Scan interval (a producer's scan, and a mirror's check of its own directory). |
| `scan_recursive` | `true` | Recurse into subdirectories. |
| `repo_dir` | `<agent data dir>/filerepo/repos/<filerepo_name, or "default">` | Repo directory for on-demand/consumer storage (absolute default, outside the plugin's data directory; configure an absolute path). |
| `instance_id` | *(unset)* | Optional repo instance identifier surfaced in listings. |
| `max_inline_bytes` | `524288` (512 KB) | Max file size returned inline by `getfile`/`getjar`. Keep under the wss frame limit. |
| `stream_buffer_size` | `262144` (256 KB) | Default streamfile chunk size (hard-capped at 768 KB). |
| `transfer_threads` | `16` | Max concurrent streamfile transfers (bounded pool). |
| `transfer_caller_fraction` | `0.5` | The share of `transfer_threads` one caller's transfers may run on at once; its others wait, holding no thread. |
| `transfer_sync_reserve` | `0.25` | The share of `transfer_threads` only sync may use (a subscriber of this producer fetching what it was offered): no set of other callers starves the mirrors. |
| `transfer_queue_max` | `64` | Streamfile transfers waiting for a thread; beyond it, busy (`11`). |
| `transfer_max_per_caller` | `16` | Streamfile transfers in flight (running or queued) per caller. |
| `scan_batch_size` | `500` | Catalog rows written per transaction during a scan. |
| `scan_follow_links` | `false` | Follow symbolic links while scanning (links are otherwise skipped). |
| `sync_threads` / `sync_queue_max` | `8` / `64` | Consumer updater pool and its queue. |
| `sync_pending_per_peer` | `4` | Diffs waiting per producer. |
| `sync_max_peers` | `256` | Producers syncing at once (and subscribers kept by a producer). |
| `sync_diff_page` / `sync_delete_page` | `1000` / `1000` | Files / deletes per `repolistin` page. |
| `sync_pages_per_cycle` | `8` | Pages a subscriber is offered per cycle beyond this cycle's changes (catch-up after a failure, a new or a cleared subscriber). `sync_full_pages_per_cycle` is the older name. |
| `sync_producers` | *(unset)* | **A mirror's pinned producers**: `region/agent#<fingerprint of the producer's key>` (the producer's `repostate` `producer_pin`), comma-separated. Only they may change the mirror; it asks no other node. A pin without a fingerprint pins nothing. Unset: the pin an admin sets with `syncpin`; until then the mirror syncs nothing (`repostate` lists `candidate_producers`). |
| `sync_role` | *(from the directories)* | `producer`, `mirror` or `none`. A mirror never acts as a producer. |
| `sync_key` | *(unset)* | An additional proof of membership (a secret of at least 16 characters, the same on a repository's producer(s) and mirrors): discovers, subscribes and answers without its proof are ignored. Never an identity. |
| `sync_subscribe_retry_ms` | `10000` | A consumer asks one producer at most this often. |
| `sync_peer_ttl_ms` | `3600000` | A producer (at a consumer) not heard from for this long is forgotten until it answers again. |
| `sync_subscriber_ttl_ms` | `604800000` (7 days) | A subscriber (at a producer) silent this long is dropped, with an ERROR; until then it is kept with everything it owes (deletes included). |
| `sync_lag_alarm_ms` | `86400000` | A subscriber owing confirms this long is an alarm (ERROR a day, gauge, health WARN). |
| `sync_holder_ttl_ms` | `2592000000` (30 days) | At a mirror: claims of a producer not heard from this long are orphaned: only it may replace those files, and deleting them needs an admin's `releasedeletes` at the mirror. |
| `sync_reset_interval_ms` | `600000` | A subscriber's new generations (each re-offers the whole catalog) are taken a few at once, then one per this interval. |
| `sync_refill_check_batch` | `10000` | A mirror's scan checks this many of its producer's files for loss a run (`scan_period`), going on where it stopped. |
| `sync_offer_timeout_ms` | `600000` | An offered file is not offered again for this long unless confirmed or failed. |
| `sync_stream_buffer` | `262144` | Chunk size a consumer asks for (4 KB - 768 KB). |
| `sync_stream_idle_ms` | `30000` | A transfer with no frame for this long is cancelled and retried. |
| `sync_stream_rounds` | `3` | Attempts per file, and rounds of re-requesting lost ranges. |
| `sync_rpc_timeout_ms` | `30000` | Bound on a sync request (`repolistin`, `streamfile`). |
| `sync_offer_threads` | `8` | Subscribers a producer offers to at once. A cycle costs the slowest subscriber's answer, not the sum: a subscriber that went away costs one unanswered offer (`sync_rpc_timeout_ms`), concurrently with the others, and is then skipped until it subscribes again. |
| `sync_tombstone_ttl_ms` | `604800000` (7 days) | How long a delete every kept subscriber applied is kept. |
| `sync_delete_guard_min` / `sync_delete_guard_fraction` | `3` / `0.10` | The mass-delete guard: a loss of at least this many files and more than this fraction of the catalog is held. |
| `sync_delete_guard_window_ms` | `900000` (15 min) | Deletes recorded this recently count with a cycle's loss for the fraction (`0`: each cycle alone). |
| `sync_delete_guard_day_fraction` / `sync_delete_guard_total_fraction` | `0.25` / `0.5` | Originals deleted or rewritten in a rolling 24 h, and since the last acknowledgement, beyond which every delete is held (`0`: off). Gross, kept across restarts. |
| `sync_delete_guard_subtree_min` / `sync_delete_guard_subtree_fraction` | `10` / `0.01` | A whole subtree that lost all its cataloged files, at least this many and more than this fraction of the catalog, is held. (A vanished mount and an emptied scan dir are always held.) |
| `sync_rewrite_guard_min` / `sync_rewrite_guard_fraction` | `10` / `0.25` | The rewrite guard: a cycle rewriting at least this many existing files and more than this fraction of the catalog is held. `1.0` turns it off. |
| `sync_rewrite_guard_day_fraction` / `sync_rewrite_guard_total_fraction` | `0.25` / `0.5` | Originals rewritten (each once) or deleted in a rolling 24 h, and since the last `releaserewrites`, beyond which rewrites are held (`0`: off). |
| `sync_release_ttl_ms` | `600000` | How long an admin's release waits for the scan that applies it. |
| `live_lease_max_ms` | `86400000` | The longest lease `liverealise` may ask for. |
| `putfiles_require_hashes` | `true` | `putfiles` refuses a file without an expected hash. |
| `plugin_signing_truststore` | *(agent setting)* | The trust store `putjar` jars must chain to (the agent's plugin-signing policy; without one every jar is refused). |
| `plugin_signing_required` | `true` *(agent setting)* | `false` lets unsigned jars through — development only, logged as a WARNING at start. |
| `db_driver` | `org.apache.derby.jdbc.EmbeddedDriver` | JDBC driver. |
| `db_jdbc` | `jdbc:derby:<agent data dir>/filerepo/state/<SHA-256 of the repository directory>/catalog;create=true` | JDBC URL for the catalog. |
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
- **Descriptor-relative opens** — where the platform has them (`SecureDirectoryStream`: Linux), `getfile` and
  `streamfile` open every directory from the repository root down, and the file, by descriptor without following a
  link; elsewhere the file is opened without following a final link, checked by path around the open, and then proven
  by its content: the opened descriptor's size (fstat) must be the cataloged size before anything is read, and exactly
  that many bytes are read through it and must hash to the content the catalog holds for it, so a directory swapped for
  a link mid-open can never make it serve a file outside the repository, and a file that grew since its scan is never
  read whole.
- **Only verified jars** — `repolist` and `getjar` consider only `*.jar` files, and list and serve only those that
  pass the agent's signing policy, however they reached the directory (verified once per size, mtime and file key).
- **Arbitrary-read protection** — `getfile` only serves files present in the catalog (i.e. under the
  configured scan/repo dir); it cannot be used to read arbitrary files on the host.
- **Memory safety** — inline `getfile`/`getjar` are size-capped (`max_inline_bytes`); large files
  must stream. `streamfile` uses a fixed reusable buffer (bounded memory regardless of file size).
- **Transport safety** — `streamfile` chunk size is hard-capped at 768 KB so a chunk + header always
  fits one dataplane frame; short reads are honored (no stale bytes); the read handle is closed even
  on mid-transfer errors (try-with-resources).
- **Integrity and authenticity** — `putjar` accepts only a jar signed by a trusted signer and computes its SHA-384
  itself; `putfiles` verifies every file's content hash; sync verifies SHA-384 and the offered hash before it commits;
  nothing half-written is ever visible under its name (staging files and atomic renames).
- **Only copies** — `clearrepo` refuses a repository that is the only copy of what it holds unless `force=true`,
  deciding under the repository gate that every landing also takes, so nothing lands between the check and the delete.
- **Live tier** — a live-backed repository serves, writes, deletes, syncs and lists nothing, provider or not; files
  in a live-tier root are never served, landed, deleted or synced through any repository, and while any registered
  provider's root is unknown no file path is.
- **Scan resilience** — files that vanish mid-scan (null MD5) or whose path exceeds the catalog column
  are skipped, so one bad file can't wedge the scanner.
- **Resource hygiene** — bounded, named, daemon thread pools **with bounded queues** for transfers and peer-sync
  (busy, not unbounded backlog); the streamfile transfer map is cleaned up on completion; all pools/timers/DB are
  shut down on plugin stop.
- **Scalability** — `getrepofilelist` supports `limit`/`offset` pagination (Derby `OFFSET/FETCH`) so a
  huge catalog is fetched in pages that stay under the wss frame limit; the scan reads the catalog in one query and
  writes its rows in batches, not one forced commit per file.

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

## Upgrade notes (1.3 hardening)

What an operator sees change, and what to do:

- **Pin each mirror's producer by its key.** Every producer makes its own signing key at its first start (logged, with
  the pin to use; also in its `repostate` as `producer_pin`). Set `sync_producers=region/agent#<fingerprint>` on every
  mirror, or send it `syncpin producer=region/agent#<fingerprint>`. **A mirror with no pin syncs nothing** (there is no
  trust on first use): its `repostate` says so and lists the `candidate_producers` that answered. Only the pinned
  producer changes a mirror; any other producer of the name is refused. Pins by node or directory (`region/agent`, `region/agent:/dir`)
  from earlier builds of this branch pin nothing now, nor does a pin a mirror recorded then (taken on first use, by
  directory): such a mirror syncs nothing until pinned by key, and `repostate` shows what that producer delivered as
  orphaned content with the `syncpin ... adopt=true` command that hands it on. Back up each producer's state directory (`producer.key`): a
  producer that loses its key, or moves to another directory or node, must be re-pinned on its mirrors (`syncpin
  ... adopt=true` keeps what the old one delivered). `sync_key` is optional (an extra proof of membership).
  `sync_role=mirror` keeps a mirror from ever producing.
- **State moved out of the plugin's data directory.** The catalog moves (renamed, once, at start) to
  `<agent data dir>/filerepo/state/...`, and the default repository directory to
  `<agent data dir>/filerepo/repos/<filerepo_name, or "default">`, with its catalog rows; nothing is deleted. Two
  plugins may no longer serve one directory (two unnamed default repositories on one agent: give each a
  `filerepo_name` or a `repo_dir`).
- **Roles.** `clearrepo`, `putjar`, `removefile`, `putfiles`, `putfilesremote`, `releasedeletes` and `releaserewrites` now need the
  `infra` or `admin` role; `repolistin` and `repoconfirm` need `plugin`. A refused call answers `status_code=403`.
  With security explicitly off (development) the caller is `anonymous` with every role and nothing changes.
- **`putfilesremote` is removed** (it read any file on the agent and relayed it): it is always refused. An admin
  sends `putfiles` to the target repository directly.
- **Sync protocol 2.** Producers and consumers must both be this version: a consumer refuses a producer whose
  `discover` carries no `sync_proto=2`, and a producer refuses (and logs) a protocol 1 subscribe, each saying why.
  Upgrade every filerepo of a `filerepo_name` together. Sync identities come from the stamped caller only: on an agent
  whose controller does not stamp callers yet, sync works only in explicit insecure mode
  (`broker_security_enabled=false`).
- **A mirror's existing files.** A consumer adopts a file it already holds when the producer offers exactly that
  content; a file it holds with other content and no record of the producer delivering it is never overwritten (a
  sync conflict, logged and counted): move or remove such files once after upgrading.
- **`streamfile`, `streamfilecancel` and `clearrepo force`** need a verified caller (or insecure mode).
- **`clearrepo`** needs `confirm=<the repository directory>` exactly (its `scan_dir`, or `repo_dir` when it has
  none); `repo_name` must name this repository if given; `force=true` needs `admin`; protected directories and live
  roots are never cleared. Scripts that cleared with just `repo_name` must add `confirm`.
- **`putjar`** accepts only jars signed by a signer chaining to the agent's `plugin_signing_truststore`. Configure the
  trust store on every agent that runs filerepo or repo **before upgrading**, and sign plugin jars (`jarsigner` with a
  code-signing certificate issued under it). Without a trust store every `putjar` is refused, and filerepo says so at
  start. `md5` is optional now; when sent it must match. `pluginname` must equal the jar's `Bundle-SymbolicName`.
- **Repository directory.** With neither `scan_dir` nor `repo_dir` configured the repository is
  `<agent data dir>/filerepo/repos/<filerepo_name, or "default">`, no longer the relative `./filerepo`. Move anything
  landed in `./filerepo` before (filerepo logs a WARNING naming both paths). On a producer, `putjar` now lands in
  `scan_dir`, so the jar is scanned and synced.
- **`putfiles`** requires `file_hashes` (file name → content hash). The source is copied, never moved, and deleted only after it landed and only when it is a transfer file of the agent's dataplane journal. A file that already
  exists with other content and `overwrite=false` is reported `skipped` with `status 9` (it used to be `10`).
  `putfiles_require_hashes=false` restores landing without hashes.
- **Sync** now moves file bytes over the dataplane with the producer's `streamfile`; the controller's `getfiledata`
  is no longer used (and no longer needs `file_access_roots` for filerepo). Producer and consumer must both be this
  version: an older consumer asks for `getfiledata`, an older producer sends no terminal frame.
- **Deletes now propagate.** A file removed from a producer's scan dir (or with `removefile`) is deleted on its
  subscribers if they still hold that exact content from that producer. The mass-delete guard holds a vanished mount,
  an emptied scan dir, a large lost subtree and any loss of 3+ files over 10% of the catalog (within 15 minutes); release it with
  `releasedeletes` and the `hold_id` from `repostate` (**required**) once intended. Check `repostate`
  (`deletes_held`) after an upgrade if the scan dir changed while filerepo was down.
- **Mass rewrites are held** like mass deletes (`rewrites_held`, `releaserewrites` with `hold_id`), also when spread
  over a day (25%) or since the last acknowledgement (50%). A job that legitimately regenerates most of a repository
  must be followed by a release, or set the rewrite fractions to `1.0` for that repository. Deletes have the same
  longer budgets (25% a day, 50% since the last release). Both count originals gross (a file deleted and replaced by
  another still counts), deletes and rewrites together, and keep their records in the catalog across restarts: a job
  that deletes or regenerates more than a quarter of a repository's files in a day needs a release (or higher
  fractions).
- **A mirror scans its own directory** (every `scan_period`): files its producer delivered that are gone are asked for
  again and refetched (`reporefill`, a new sync message: upgrade producers and mirrors of a name together).
- **A mirror's directory carries a generation marker** (`.filerepo-staging/generation`). A mirror upgraded from a build
  without one starts a new generation once (its producer offers everything; it holds what it has and fetches nothing
  twice).
- **A producer silent for 30 days** (`sync_holder_ttl_ms`): its files at a mirror are changed by it alone, and deleted
  only with an admin's `releasedeletes` at the mirror.
- **On macOS and Windows** (no descriptor-relative opens), `getfile` and `streamfile` prove an opened file by reading
  it once through its descriptor (it must hash to its cataloged content) before sending it: after the size check
  (getfile answers "too large" from the catalog without opening the file) and after the transfer is admitted (on its
  own thread). The proven descriptor is kept for a minute for the same caller and file version, so the next ranges of
  that file are not read again. A file changed since the last scan is refused until rescanned: first by the size of
  the opened descriptor (fstat), which must be the cataloged size, before a byte is read (a file that grew since its
  scan is never read whole); then a proof reads exactly the cataloged size, never more. Linux opens
  descriptor-relative and reads once.
- **`streamfile`**: `transfer_id` is optional and validated; `ident_key` must be a property name; chunks carry
  `offset`; every transfer ends with an empty terminal frame (`transfer_end=true`, `sha384` on success). A client that
  counts frames should skip the terminal one (it carries no payload). Busy is `status 11`.
- **Symbolic links in a scan dir are no longer followed** (`scan_follow_links=true` restores it); a scan dir that is
  itself a link is followed. A file is served only from inside the repository directory.
- **`liverealise`** needs `infra` or `admin`, and `lease_ms` at most `live_lease_max_ms`.
- **`repolist`/`getjar`** list and serve only signature-verified `*.jar` files.
- **New metrics** (`filerepo.transfer.*`, `filerepo.sync.*`, `filerepo.rewrites.held`) and a health WARN while
  deletes or rewrites are held or a subscriber lags.

## Testing

`mvn test` runs filerepo's JUnit 5 suite (part of every `package` build): the live-tier client against a real
`ServiceTracker` over a fake registry (absence, admission by bundle, coming and going, a hung provider, forwarded
calls that never starve local ones, the reuse of a row's listing, the exact request maps), every repository state,
every refused verb on live-backed repositories with and without a provider, the clear refusals and the gate
(including two concurrent clearrepo/putfiles sequences), the live-root rules (including failing closed while any
registered provider's root is unknown, several providers, and configured roots), and the touched verbs answering as
before on ordinary repositories; and the 1.3 hardening: the role table, clearrepo's guards, signed putjar
(test-time keys from keytool, checked by the library's JarSignatures), verified putfiles, the streamfile protocol and
its bounds, the descriptor check, and two filerepos syncing for real over a stand-in fabric (paging, lost and
corrupted frames, a lost terminal frame, deletes, the mass-delete guard, refused sources). It needs no agent: filerepo's own engines run over a
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
