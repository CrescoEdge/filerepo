package io.cresco.filerepo;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.cresco.library.data.DataPlaneService;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.metrics.CMetric;
import io.cresco.library.metrics.MeasurementEngine;
import io.cresco.library.plugin.Executor;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import jakarta.jms.BytesMessage;
import jakarta.jms.DeliveryMode;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystemException;
import java.nio.file.SecureDirectoryStream;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.jar.JarFile;
import java.util.regex.Pattern;

import io.cresco.library.capability.*;

@CrescoCapabilities(namespace = "filerepo", target = "plugin",
    routingParams = {"region", "agent", "pluginid"},
    summary = "Distributed file/artifact repository: scans a directory, tracks files in an embedded " +
              "catalog, and moves files/jars across the mesh via inline transfer, byte-range dataplane " +
              "streaming, watched-directory sync, and push-to-remote-repo.")
@CrescoActions({
    @CrescoAction(name = "repolist", type = "EXEC",
        summary = "List the plugin/jar inventory of this repo.", why = "Discover available artifacts.",
        returns = @CrescoReturn(name = "repolist", type = "object", compressed = true, description = "JSON inventory")),
    @CrescoAction(name = "getrepofilelist", type = "EXEC",
        summary = "List files (path/md5/size/mtime) in a named repo.", why = "Enumerate a repo (e.g. find a model adapter).",
        params = @CrescoParam(name = "repo_name", required = true, description = "repo name"),
        returns = @CrescoReturn(name = "repofilelist", type = "object", compressed = true, description = "JSON file list")),
    @CrescoAction(name = "getfile", type = "EXEC",
        summary = "Return a whole file inline (bytes in the reply).", why = "Fetch a small file in one message.",
        params = @CrescoParam(name = "file_path", required = true, description = "absolute path on the source agent"),
        returns = @CrescoReturn(name = "file_data", type = "bytes", description = "the file bytes")),
    @CrescoAction(name = "getjar", type = "EXEC",
        summary = "Return a plugin jar inline, matched by name+md5.", why = "Pull a plugin bundle to an agent.",
        params = {@CrescoParam(name = "action_pluginname", required = true, description = "plugin name"),
                  @CrescoParam(name = "action_pluginmd5", required = true, description = "plugin content hash (the inventory's md5 field)")},
        returns = @CrescoReturn(name = "jardata", type = "bytes", description = "the jar bytes")),
    @CrescoAction(name = "putjar", type = "EXEC", roles = {"infra", "admin"},
        summary = "Store a plugin jar (inline bytes) in this repo. Accepted only if it is signed by a signer that chains to " +
                  "plugin_signing_truststore; the repo computes its SHA-384 itself.",
        why = "Publish a plugin bundle.",
        params = {@CrescoParam(name = "pluginname", required = true, description = "must match the jar's Bundle-SymbolicName"),
                  @CrescoParam(name = "jarfile", required = true, description = "file name: letters, digits, '.', '_', '-'"),
                  @CrescoParam(name = "md5", description = "advisory content hash: a mismatch is refused, a match proves nothing"),
                  @CrescoParam(name = "version"),
                  @CrescoParam(name = "jardata", required = true, type = "bytes", description = "jar bytes")},
        returns = {@CrescoReturn(name = "uploaded", description = "plugin name on success"),
                   @CrescoReturn(name = "md5-confirm", description = "the content hash the repo computed"),
                   @CrescoReturn(name = "sha384", description = "the SHA-384 the repo computed")}),
    @CrescoAction(name = "putfiles", type = "EXEC", roles = {"infra", "admin"},
        summary = "Land pushed files (MsgEvent attachments) into a repo, each verified against its expected content hash.",
        why = "Receiving side of a file push.",
        params = {@CrescoParam(name = "repo_name", required = true),
                  @CrescoParam(name = "file_hashes", required = true, compressed = true, type = "object",
                          description = "JSON object: file name -> expected content hash"),
                  @CrescoParam(name = "overwrite", type = "boolean")},
        returns = {@CrescoReturn(name = "status", description = "10 every file stored; 9 some skipped or failed (see the lists)"),
                   @CrescoReturn(name = "stored", type = "object", compressed = true),
                   @CrescoReturn(name = "unchanged", type = "object", compressed = true),
                   @CrescoReturn(name = "skipped", type = "object", compressed = true),
                   @CrescoReturn(name = "failed", type = "object", compressed = true)}),
    @CrescoAction(name = "putfilesremote", type = "EXEC", roles = {"infra", "admin"},
        summary = "Removed: always refused. An admin sends putfiles to the target repository directly.",
        why = "Kept so an old caller learns where to go.",
        params = {@CrescoParam(name = "file_list", required = true, compressed = true, type = "object", description = "JSON list of paths"),
                  @CrescoParam(name = "dst_region", required = true), @CrescoParam(name = "dst_agent", required = true),
                  @CrescoParam(name = "dst_plugin", required = true), @CrescoParam(name = "repo_name", required = true)},
        returns = @CrescoReturn(name = "status", description = "10 request sent")),
    @CrescoAction(name = "streamfile", type = "EXEC",
        summary = "Stream a byte-range of a cataloged file over the dataplane in chunks, ending with a terminal frame that " +
                  "carries the SHA-384 of the bytes sent.",
        why = "Move large artifacts efficiently.",
        params = {@CrescoParam(name = "file_path", required = true), @CrescoParam(name = "start_byte", required = true, type = "long"),
                  @CrescoParam(name = "byte_length", required = true, type = "long"),
                  @CrescoParam(name = "transfer_id", description = "1-128 of [A-Za-z0-9_.:-], unique among the caller's transfers; generated when absent"),
                  @CrescoParam(name = "ident_key", required = true, description = "dataplane property name the frames carry"),
                  @CrescoParam(name = "ident_id", required = true, description = "its value"),
                  @CrescoParam(name = "buffer_size", type = "int", description = "chunk size, default 262144")},
        returns = {@CrescoReturn(name = "status", description = "10 accepted; 11 busy (bounded queue full), retry later"),
                   @CrescoReturn(name = "transfer_id", description = "the transfer's id")}),
    @CrescoAction(name = "streamfilecancel", type = "EXEC",
        summary = "Cancel one of the caller's own streamfile transfers (running or queued).", why = "Stop a large transfer.",
        params = @CrescoParam(name = "transfer_id", required = true),
        returns = @CrescoReturn(name = "status_code", description = "10 cancelled")),
    @CrescoAction(name = "getscandir", type = "EXEC",
        summary = "Return this repo's configured scan directory.", why = "Discover where the repo watches.",
        returns = @CrescoReturn(name = "scan_dir", description = "the scan directory")),
    @CrescoAction(name = "removefile", type = "EXEC", roles = {"infra", "admin"},
        summary = "Remove a file from this repo (file + catalog row); a producer offers the delete to its subscribers.",
        why = "Delete an artifact.",
        params = {@CrescoParam(name = "repo_name", description = "when given, must name this repository"),
                  @CrescoParam(name = "file_name", required = true)},
        returns = @CrescoReturn(name = "status", description = "10 removed")),
    @CrescoAction(name = "clearrepo", type = "EXEC", roles = {"infra", "admin"},
        summary = "Delete every file in this repository's directory. Must be confirmed with that directory's exact name. " +
                  "An only copy (repostate ONLY_COPY) is refused unless force=true, which needs the admin role; " +
                  "a protected directory or one holding a live-tier root is never cleared.",
        why = "Wipe a repo.",
        params = {@CrescoParam(name = "confirm", required = true, description = "this repository's directory, exactly (scan_dir, or repo_dir)"),
                  @CrescoParam(name = "repo_name", description = "when given, must name this repository"),
                  @CrescoParam(name = "force", type = "boolean", description = "clear an only copy anyway (admin only)")},
        returns = @CrescoReturn(name = "status", description = "10 cleared; 9 refused (see refusal) or not cleared")),
    @CrescoAction(name = "releasedeletes", type = "EXEC", roles = {"infra", "admin"},
        summary = "Release the deletes the mass-delete guard is holding, so the next scan offers them to subscribers. " +
                  "Must name the held set (repostate deletes_hold_id): a release that names none, or another, is refused.",
        why = "Confirm that many files really were deleted from a producer's scan dir.",
        params = {@CrescoParam(name = "confirm", required = true, description = "this repository's directory, exactly"),
                  @CrescoParam(name = "hold_id", required = true, description = "the held set to release (repostate deletes_hold_id); refused if it changed")},
        returns = @CrescoReturn(name = "released", description = "how many held deletes the next scan records, if it finds exactly that set")),
    @CrescoAction(name = "releaserewrites", type = "EXEC", roles = {"infra", "admin"},
        summary = "Release the rewrites the rewrite guard is holding, so they are offered to subscribers. " +
                  "Must name the held set (repostate rewrites_hold_id): a release that names none, or another, is refused.",
        why = "Confirm that a producer really rewrote that many files at once.",
        params = {@CrescoParam(name = "confirm", required = true, description = "this repository's directory, exactly"),
                  @CrescoParam(name = "hold_id", required = true, description = "the held set to release (repostate rewrites_hold_id)")},
        returns = @CrescoReturn(name = "released", description = "how many held rewrites are now offered")),
    @CrescoAction(name = "repostate", type = "EXEC",
        summary = "Report what this repo is: LIVE, WORKSPACE_CLEAN, WORKSPACE_DIRTY, MIRROR or ONLY_COPY.",
        why = "Know what would be lost with a repo before clearing, releasing or relying on it.",
        returns = {@CrescoReturn(name = "state", description = "LIVE | WORKSPACE_CLEAN | WORKSPACE_DIRTY | MIRROR | ONLY_COPY"),
                   @CrescoReturn(name = "backing", description = "live (a live-tier row) or local"),
                   @CrescoReturn(name = "status", description = "10 answered; 9 the live row is not held here; 11 the live-tier provider did not answer")}),
    @CrescoAction(name = "liverelay", type = "EXEC",
        summary = "Relay a request signed by its end user, unchanged, through this agent's live-tier provider.",
        why = "Act on live-tier copies and workspaces with the end user's own authority.",
        params = @CrescoParam(name = "request", required = true, compressed = true, type = "object",
                description = "the signed request: a JSON object of strings"),
        returns = {@CrescoReturn(name = "reply", type = "object", compressed = true, description = "the reply, as it came"),
                   @CrescoReturn(name = "reply_status", description = "the reply's own status"),
                   @CrescoReturn(name = "status", description = "10 answered (see reply_status); 8 no request; 11 the live-tier provider did not answer")}),
    @CrescoAction(name = "liverealise", type = "EXEC", roles = {"infra", "admin"},
        summary = "Keep a live-backed repo's copy live: realise it again at this node as the node itself (copies only).",
        why = "Keep the copy a live-backed repo is configured with available at this node.",
        params = {@CrescoParam(name = "lease_ms", type = "long", description = "requested lease, at most live_lease_max_ms"),
                  @CrescoParam(name = "request_id", description = "idempotency key: 1-64 letters, digits, - or _")},
        returns = {@CrescoReturn(name = "job_id", description = "the realise job"),
                   @CrescoReturn(name = "reply_status", description = "the realise reply's own status"),
                   @CrescoReturn(name = "status", description = "10 realise accepted; 8 bad params; 9 refused (see refusal); 11 the live-tier provider did not answer")}),
    @CrescoAction(name = "repolistin", type = "EXEC", roles = {"plugin"},
        summary = "Receive a producer's file-diff (and deletes) and pull changed files over the dataplane (sync consumer side).",
        why = "Directory-sync receiving half. Accepted only from a producer this repo subscribed to.",
        params = {@CrescoParam(name = "repolistin", required = true, compressed = true, type = "object", description = "JSON diff"),
                  @CrescoParam(name = "repodeletes", compressed = true, type = "object", description = "JSON list of deletes"),
                  @CrescoParam(name = "transfer_id", required = true)},
        returns = @CrescoReturn(name = "status_code", description = "10 accepted; 9 refused; 11 busy, offer again")),
    @CrescoAction(name = "reposubscribe", type = "EXEC", roles = {"plugin"},
        summary = "Sync protocol 2: a subscriber asks this producer to serve it (answered with reposubscribed).",
        why = "Sync handshake; the subscriber is its stamped caller.",
        params = {@CrescoParam(name = "filerepo_name", required = true), @CrescoParam(name = "sync_gen", required = true),
                  @CrescoParam(name = "sync_proto", required = true)}),
    @CrescoAction(name = "reposubscribed", type = "EXEC", roles = {"plugin"},
        summary = "Sync protocol 2: a producer's answer to reposubscribe.",
        why = "Sync handshake; the producer is its stamped caller.",
        params = {@CrescoParam(name = "accepted", required = true), @CrescoParam(name = "filerepo_name"),
                  @CrescoParam(name = "sync_proto")}),
    @CrescoAction(name = "repoconfirm", type = "EXEC", roles = {"plugin"},
        summary = "Acknowledge a subscriber received a transfer generation.", why = "Sync handshake.",
        params = @CrescoParam(name = "transfer_id", required = true)),
    @CrescoAction(name = "getmetrics", type = "EXEC",
        summary = "Return filerepo metrics (catalog, transfers, sync) as MeasurementEngine gauges JSON.",
        why = "Standard cross-bundle metrics contract; folded into the controller's getmetricinventory.",
        returns = @CrescoReturn(name = "metrics", type = "object", description = "getAllMetrics() JSON")),
    @CrescoAction(name = "getcapabilities", type = "EXEC",
        summary = "Return this plugin's self-describing capability document (its message actions as LLM tool specs).",
        why = "Discovery: lets a client/LLM learn what this plugin can do and how to call it.",
        returns = @CrescoReturn(name = "capabilities", type = "object", description = "CapabilityDocument JSON"))
})
public class ExecutorImpl implements Executor {

    // Verbs a live-backed repository still answers: none of them names a path. Every other verb, including any added
    // later, is refused: the repository is a live-tier copy or workspace, read only through the filesystem at this
    // node, and filerepo never serves, writes, deletes, syncs or lists it, nor says where it is (getscandir).
    static final Set<String> LIVE_BACKED_VERBS = Set.of("repostate", "liverelay", "liverealise",
            "getmetrics", "getcapabilities");

    /** Verifies a jar that arrived over the network; returns its SHA-384 (lowercase hex) or throws to refuse it. */
    interface JarVerifier {
        String verify(Path jar) throws Exception;
    }

    private PluginBuilder plugin;
    private CLogger logger;
    private Gson gson;
    private RepoEngine repoEngine;
    private Type listType;
    private final Type stringMapType = new com.google.gson.reflect.TypeToken<Map<String, String>>(){}.getType();
    // the live-tier client; may be null (no provider, ever)
    private final LiveTierClient liveTier;

    // caller|transfer_id -> transfer, from the moment it is accepted (so a queued transfer can be cancelled too)
    private final Map<String, StreamObject> transferStreams = new ConcurrentHashMap<>();

    // B-2 unified metrics: filerepo catalog size / in-flight transfers as MeasurementEngine gauges,
    // exposed via the standard getmetrics EXEC so they fold into the controller's metric inventory.
    private MeasurementEngine metricEngine;
    // set under `this` monitor in cleanup(); blocks a racing getmetrics from recreating the engine
    private boolean closed = false;

    // Fair, bounded streamer scheduling (FR-D5): transfer_threads at once, at most transfer_caller_fraction of them for
    // any one caller, transfer_max_per_caller in flight per caller and transfer_queue_max waiting in all; beyond a bound
    // a streamfile is refused as busy (11). Inline reads (getfile/getjar) are capped to keep them off the heap.
    private final TransferScheduler transfers;
    // explicit insecure development mode (broker_security_enabled=false on the agent): an unstamped caller is anonymous
    private final boolean insecure;
    private final long liveLeaseMaxMs;
    private final AtomicLong transfersRejected = new AtomicLong();
    private final long maxInlineBytes;
    private final int defaultStreamBuffer;
    // Hard ceiling on a streamfile chunk: it + its framing header must fit one dataplane frame.
    private static final int MAX_STREAM_BUFFER = 786432; // 768 KB

    private static final Pattern TRANSFER_ID = Pattern.compile("[A-Za-z0-9_.:-]{1,128}");
    private static final Pattern PROPERTY_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,63}");
    private static final Pattern JAR_NAME = Pattern.compile("[A-Za-z0-9_][A-Za-z0-9._-]{0,254}");
    /** Properties a streamfile frame carries itself: an ident_key may not shadow one. */
    private static final Set<String> FRAME_PROPERTIES = Set.of("transfer_id", "seq_num", "offset", "transfer_end",
            "transfer_status", "transfer_desc", "total_bytes", "sha384", "start_byte", "byte_length");
    /** JMS selector words: a property may not be named after one. */
    private static final Set<String> SELECTOR_WORDS = Set.of("NULL", "TRUE", "FALSE", "NOT", "AND", "OR", "BETWEEN",
            "LIKE", "IN", "IS", "ESCAPE");

    private final JarInventory jarInventory;
    private volatile JarVerifier jarVerifier;
    // test hook: runs on the transfer thread before a streamer reads its first byte
    volatile Runnable beforeStream;

    public ExecutorImpl(PluginBuilder pluginBuilder, RepoEngine repoEngine, LiveTierClient liveTier) {
        this.plugin = pluginBuilder;
        logger = plugin.getLogger(ExecutorImpl.class.getName(), CLogger.Level.Info);
        insecure = Callers.insecureMode(plugin);
        gson = new Gson();
        this.repoEngine = repoEngine;
        this.liveTier = liveTier;
        listType = new TypeToken<ArrayList<String>>(){}.getType();
        this.jarVerifier = defaultJarVerifier();
        // repolist and getjar: only *.jar files are looked at (never a producer's data files), and only those that pass
        // the agent's signing policy are listed or served, however they reached the directory
        this.jarInventory = new JarInventory(n -> n.toLowerCase(Locale.ROOT).endsWith(".jar"), jar -> jarVerifier.verify(jar));

        int maxThreads = (int) Math.max(2L, plugin.getConfig().getLongParam("transfer_threads", 16L));
        int queueMax = (int) Math.max(1L, Math.min(100_000L, plugin.getConfig().getLongParam("transfer_queue_max", 64L)));
        int maxPerCaller = (int) Math.max(1L, plugin.getConfig().getLongParam("transfer_max_per_caller", 16L));
        double fraction = 0.5, syncFraction = 0.25;
        try {
            Double d = plugin.getConfig().getDoubleParam("transfer_caller_fraction");
            if (d != null && d > 0 && d < 1) fraction = d;
            Double r = plugin.getConfig().getDoubleParam("transfer_sync_reserve");
            if (r != null && r > 0 && r < 1) syncFraction = r;
        } catch (Exception ignore) { }
        this.transfers = new TransferScheduler(maxThreads, fraction, syncFraction, maxPerCaller, queueMax);
        this.liveLeaseMaxMs = Math.max(1L, plugin.getConfig().getLongParam("live_lease_max_ms", 86_400_000L));
        // Inline getfile returns the whole file in ONE control-plane RPC message, which the wss
        // frame caps at ~1MB (base64-inflated). Default the cap BELOW that so getfile can never
        // produce an oversized frame; anything larger returns status 5 -> use streamfile (which
        // has no size limit — it byte-ranges over the dataplane in chunks). Raise this only if the
        // wsapi frame limit is also raised.
        this.maxInlineBytes = plugin.getConfig().getLongParam("max_inline_bytes", 524288L); // 512 KB
        this.defaultStreamBuffer = (int)(long) plugin.getConfig().getLongParam("stream_buffer_size", 262144L); // 256 KB
    }

    /**
     * The production jar check: the agent's plugin-signing policy (JarSignatures against plugin_signing_truststore,
     * through the PluginBuilder). Refuses an unsigned, partially signed, tampered or foreign-signed jar, and every jar
     * when no trust store is configured; only plugin_signing_required=false (development) lets one through, with a
     * WARNING. Returns the SHA-384 the library computed.
     */
    private JarVerifier defaultJarVerifier() {
        return jar -> plugin.verifyJarSignature(jar);
    }

    /** The jar inventory (tests). */
    JarInventory jarInventory() {
        return jarInventory;
    }

    /** Tests: the jar check. */
    void setJarVerifier(JarVerifier verifier) {
        this.jarVerifier = verifier;
    }

    /** Shut the transfer pool down; called from Plugin.isStopped(). */
    public void cleanup() {
        try {
            for (StreamObject s : transferStreams.values()) s.setActive(false);
            transfers.shutdown();
            // a transfer that never started still holds its open file
            for (StreamObject s : transferStreams.values()) s.closeChannel();
        } catch (Exception ex) {
            logger.error("transfer shutdown error", ex);
        }
        // same monitor getMetricsJson() uses: a getmetrics racing plugin stop can otherwise create
        // a fresh MeasurementEngine after this ran and leak its gauge registrations across a reload
        synchronized (this) {
            closed = true;
            try {
                if (metricEngine != null) { metricEngine.shutdown(); metricEngine = null; }
            } catch (Exception ex) {
                logger.error("metricEngine shutdown error", ex);
            }
        }
    }

    /** Unified metrics payload — standard getAllMetrics() shape the metric inventory expects. */
    private synchronized String getMetricsJson() {
        try {
            if (closed) return "{}";
            if (metricEngine == null) {
                metricEngine = new MeasurementEngine(plugin);
                metricEngine.setGauge("filerepo.files.count", "files tracked in the local repo catalog", "filerepo", CMetric.MeasureClass.GAUGE_LONG);
                metricEngine.setGauge("filerepo.active.transfers", "in-flight streamfile transfers (running and queued)", "filerepo", CMetric.MeasureClass.GAUGE_INT);
                metricEngine.setGauge("filerepo.transfer.queued", "streamfile transfers waiting for a thread", "filerepo", CMetric.MeasureClass.GAUGE_INT);
                metricEngine.setGauge("filerepo.transfer.rejected", "streamfile requests refused as busy (bounded queue or per-caller cap)", "filerepo", CMetric.MeasureClass.GAUGE_LONG);
                metricEngine.setGauge("filerepo.sync.queued", "producer diffs waiting for a sync updater", "filerepo", CMetric.MeasureClass.GAUGE_INT);
                metricEngine.setGauge("filerepo.sync.active", "sync updaters running", "filerepo", CMetric.MeasureClass.GAUGE_INT);
                metricEngine.setGauge("filerepo.sync.rejected", "producer diffs refused as busy (bounded queues)", "filerepo", CMetric.MeasureClass.GAUGE_LONG);
                metricEngine.setGauge("filerepo.sync.refused", "diffs refused from a source that is not a subscribed producer", "filerepo", CMetric.MeasureClass.GAUGE_LONG);
                metricEngine.setGauge("filerepo.sync.fetched", "files fetched and verified by sync", "filerepo", CMetric.MeasureClass.GAUGE_LONG);
                metricEngine.setGauge("filerepo.sync.failed", "file fetches that failed (offered again)", "filerepo", CMetric.MeasureClass.GAUGE_LONG);
                metricEngine.setGauge("filerepo.sync.verify.failed", "fetched files refused by hash verification", "filerepo", CMetric.MeasureClass.GAUGE_LONG);
                metricEngine.setGauge("filerepo.sync.deleted", "local copies deleted because their producer deleted them", "filerepo", CMetric.MeasureClass.GAUGE_LONG);
                metricEngine.setGauge("filerepo.sync.tombstones", "deletes recorded for subscribers (offered and confirmed)", "filerepo", CMetric.MeasureClass.GAUGE_LONG);
                metricEngine.setGauge("filerepo.sync.deletes.held", "deletes held by the mass-delete guard", "filerepo", CMetric.MeasureClass.GAUGE_INT);
                metricEngine.setGauge("filerepo.sync.conflicts", "offers a mirror refused to protect its own content (landed, another producer's, case collision)", "filerepo", CMetric.MeasureClass.GAUGE_LONG);
                metricEngine.setGauge("filerepo.sync.subscribers", "subscribers this producer serves", "filerepo", CMetric.MeasureClass.GAUGE_INT);
                metricEngine.setGauge("filerepo.sync.subscribers.lagging", "subscribers owing confirms for longer than sync_lag_alarm_ms", "filerepo", CMetric.MeasureClass.GAUGE_INT);
                metricEngine.setGauge("filerepo.sync.subscribers.dropped", "subscribers dropped after sync_subscriber_ttl_ms of silence", "filerepo", CMetric.MeasureClass.GAUGE_LONG);
                metricEngine.setGauge("filerepo.sync.unverified", "discovers, subscribes and answers refused for want of the sync_key's proof", "filerepo", CMetric.MeasureClass.GAUGE_LONG);
                metricEngine.setGauge("filerepo.rewrites.held", "rewrites the rewrite guard holds back from subscribers", "filerepo", CMetric.MeasureClass.GAUGE_LONG);
            }
            long fileCount = 0L;
            long tombstones = 0L;
            try { fileCount = repoEngine.getRepoCount(); } catch (Exception ignore) {}
            try { tombstones = repoEngine.getTombstoneCount(); } catch (Exception ignore) {}
            metricEngine.updateLongGauge("filerepo.files.count", fileCount);
            metricEngine.updateIntGauge("filerepo.active.transfers", transferStreams.size());
            metricEngine.updateIntGauge("filerepo.transfer.queued", transfers.queued());
            metricEngine.updateLongGauge("filerepo.transfer.rejected", transfersRejected.get());
            metricEngine.updateIntGauge("filerepo.sync.queued", repoEngine.getSyncQueued());
            metricEngine.updateIntGauge("filerepo.sync.active", repoEngine.getSyncActive());
            metricEngine.updateLongGauge("filerepo.sync.rejected", repoEngine.syncRejected.get());
            metricEngine.updateLongGauge("filerepo.sync.refused", repoEngine.syncRefused.get());
            metricEngine.updateLongGauge("filerepo.sync.fetched", repoEngine.syncFetched.get());
            metricEngine.updateLongGauge("filerepo.sync.failed", repoEngine.syncFetchFailed.get());
            metricEngine.updateLongGauge("filerepo.sync.verify.failed", repoEngine.syncVerifyFailed.get());
            metricEngine.updateLongGauge("filerepo.sync.deleted", repoEngine.syncDeleted.get());
            metricEngine.updateLongGauge("filerepo.sync.tombstones", tombstones);
            metricEngine.updateIntGauge("filerepo.sync.deletes.held", repoEngine.getDeletesHeld());
            metricEngine.updateLongGauge("filerepo.sync.conflicts", repoEngine.syncConflicts.get());
            metricEngine.updateIntGauge("filerepo.sync.subscribers", repoEngine.getSubscriberCount());
            metricEngine.updateIntGauge("filerepo.sync.subscribers.lagging", repoEngine.getLaggingSubscribers());
            metricEngine.updateLongGauge("filerepo.sync.subscribers.dropped", repoEngine.subscribersDropped.get());
            metricEngine.updateLongGauge("filerepo.sync.unverified", repoEngine.syncUnverified.get());
            metricEngine.updateLongGauge("filerepo.rewrites.held", repoEngine.getRewritesHeld());
            return gson.toJson(metricEngine.getAllMetrics());
        } catch (Exception ex) {
            logger.error("getMetricsJson() " + ex.getMessage(), ex);
            return "{}";
        }
    }

    @Override
    public MsgEvent executeCONFIG(MsgEvent incoming) { return null; }

    @Override
    public MsgEvent executeDISCOVER(MsgEvent incoming) { return null; }
    @Override
    public MsgEvent executeERROR(MsgEvent incoming) { return null; }
    @Override
    public MsgEvent executeINFO(MsgEvent incoming) { return null; }
    @Override
    public MsgEvent executeEXEC(MsgEvent incoming) {

        // log the action + param KEYS only: the value map can carry base64 jardata/file_data,
        // dumping whole file/jar contents into the log (disclosure + unbounded log growth)
        logger.debug("Processing Exec action: " + incoming.getParam("action") + " params=" + incoming.getParams().keySet());

        if(incoming.getParams().containsKey("action")) {
            String action = incoming.getParam("action");
            // the backing is fixed by configuration, so this refusal needs no gate and no provider: it holds even when
            // the live-tier provider cannot be reached
            if (repoEngine.isLiveBacked() && (action == null || !LIVE_BACKED_VERBS.contains(action))) {
                return refuseLiveBacked(incoming);
            }
            // a repository whose own directory lies in (or contains) a live-tier root is refused the same way
            if (action != null && !LIVE_BACKED_VERBS.contains(action)) {
                String conflict = repoEngine.repoDirConflict();
                if (conflict != null) {
                    return refuseLiveRoot(incoming, "this repository's directory " + conflict);
                }
            }
            switch (action) {

                case "repolist":
                    return repoList(incoming);
                case "getrepofilelist":
                    return getRepoFileList(incoming);
                case "repostate":
                    return repoState(incoming);
                case "liverelay":
                    return liveRelay(incoming);
                case "liverealise":
                    return liveRealise(incoming);
                case "clearrepo":
                    return clearRepo(incoming);
                case "releasedeletes":
                    return releaseDeletes(incoming);
                case "releaserewrites":
                    return releaseRewrites(incoming);
                case "getjar":
                    return getPluginJar(incoming);
                case "getfile":
                    return getFile(incoming);
                case "getscandir":
                    return getScanDir(incoming);
                case "streamfile":
                    return streamFile(incoming);
                case "streamfilecancel":
                    return streamFileCancel(incoming);
                case "putjar":
                    return putPluginJar(incoming);
                case "putfiles":
                    return putFiles(incoming);
                case "removefile":
                    return removeFile(incoming);
                case "putfilesremote":
                    return putFileRemote(incoming);
                case "repolistin":
                    return repoListIn(incoming);
                case "repoconfirm":
                    confirmTransfer(incoming);
                    break;
                case "reposubscribe":
                    repoEngine.onSubscribe(incoming);
                    break;
                case "reposubscribed":
                    repoEngine.onSubscribed(incoming);
                    break;
                case "getmetrics":
                    // B-2 unified metrics: filerepo catalog / transfer gauges
                    incoming.setParam("metrics", getMetricsJson());
                    incoming.setParam("status", "10");
                    return incoming;
                case "getcapabilities":
                    return CapabilityResponder.respond(incoming, this);

            }
        }
        return null;

    }

    @Override
    public MsgEvent executeWATCHDOG(MsgEvent incoming) { return null; }

    @Override
    public MsgEvent executeKPI(MsgEvent incoming) { return null; }

    private MsgEvent getRepoFileList(MsgEvent msg) {
        try {
            if(msg.paramsContains("repo_name")) {
               String repo_name = msg.getParam("repo_name");
               String scanDirString = plugin.getConfig().getStringParam("scan_dir");
               if(scanDirString != null) {
                   msg.setParam("scan_dir", scanDirString);
               }
               String repoInstanceId = plugin.getConfig().getStringParam("instance_id");
               if(repoInstanceId != null) {
                   msg.setParam("instance_id", repoInstanceId);
               }
               // optional pagination for very large catalogs (limit<=0 -> whole list, back-compat)
               int limit = 0, offset = 0;
               try { if (msg.getParam("limit") != null) limit = Integer.parseInt(msg.getParam("limit")); } catch (Exception ignore) {}
               try { if (msg.getParam("offset") != null) offset = Integer.parseInt(msg.getParam("offset")); } catch (Exception ignore) {}
               msg.setCompressedParam("repofilelist", repoEngine.getFileRepoString(repo_name, limit, offset));
               msg.setParam("repo_total", String.valueOf(repoEngine.getRepoCount()));
               msg.setParam("status","10");
               msg.setParam("status_desc","found list");
            } else {
                msg.setParam("status","9");
                msg.setParam("status_desc","list not found");
            }

        }catch (Exception ex) {
            logger.error(ex.getMessage());
            msg.setParam("status","8");
            msg.setParam("status_desc",ex.getMessage());
        }
        return msg;
    }

    private MsgEvent repoList(MsgEvent msg) {

        try {
            Map<String, List<Map<String, String>>> repoMap = new HashMap<>();
            repoMap.put("plugins", pluginInventory());
            repoMap.put("server", getRepoInfo());
            msg.setCompressedParam("repolist", gson.toJson(repoMap));
        }catch (Exception ex) {
            logger.error(ex.getMessage());
        }

        return msg;

    }

    /** The plugin jars in the repository directory, as the inventory reports them (null when there are none). */
    private List<Map<String, String>> pluginInventory() {
        List<Map<String, String>> out = new ArrayList<>();
        for (JarInventory.Entry e : jarInventory.list(getRepoDir())) out.add(e.asMap());
        return out.isEmpty() ? null : out;
    }

    private List<Map<String,String>> getRepoInfo() {
        List<Map<String,String>> repoInfo = null;
        try {
            repoInfo = new ArrayList<>();
            Map<String, String> repoMap = new HashMap<>();
            repoMap.put("region",plugin.getRegion());
            repoMap.put("agent",plugin.getAgent());
            repoMap.put("pluginid",plugin.getPluginID());
            repoInfo.add(repoMap);
        } catch(Exception ex) {
            logger.error("filerepo error", ex);
        }
        return repoInfo;
    }

    /** One repository directory for every verb (FR-D9): putjar lands where the scanner looks and getjar serves. */
    private File getRepoDir() {
        return repoEngine.getRepoDir();
    }

    /**
     * putjar (FR-D3): the bytes are pinned. They are written to a staging file in the repository's owner-only staging
     * directory and verified there (the agent's plugin-signing policy: signed by a signer that chains to
     * plugin_signing_truststore); every hash and the bundle name come from the bytes in memory; the verifier must have
     * read exactly those bytes and the staged file must still hold them; after the atomic move the stored jar is
     * checked once more. On any difference nothing is stored. A hash the caller supplies is advisory, and one that
     * differs is refused.
     */
    private MsgEvent putPluginJar(MsgEvent incoming) {

        Path staged = null;
        try {
            String pluginName = incoming.getParam("pluginname");
            String pluginMD5 = incoming.getParam("md5");
            String pluginJarFile = incoming.getParam("jarfile");
            byte[] jarData = incoming.getDataParam("jardata");
            // copy the (large, base64) payload out and drop it from the message early
            if(incoming.getParams().containsKey("jardata")) incoming.removeParam("jardata");

            if (pluginName == null || pluginJarFile == null || jarData == null) {
                incoming.setParam("status","7");
                incoming.setParam("status_desc","missing required putjar params (pluginname, jarfile, jardata)");
                return incoming;
            }
            if (!JAR_NAME.matcher(pluginJarFile).matches() || pluginJarFile.contains("..")
                    || RepoEngine.isStaging(pluginJarFile)) {
                incoming.setParam("status","8");
                incoming.setParam("status_desc","invalid jarfile name");
                return incoming;
            }
            // what was sent, pinned in memory
            String sha384 = Hashes.sha384(jarData);
            String contentHash = Hashes.content(jarData);
            if (pluginMD5 != null && !pluginMD5.equals(contentHash)) {
                incoming.setParam("status","9");
                incoming.setParam("status_desc","the jar's content hash differs from the md5 supplied, upload rejected");
                return incoming;
            }
            String declared = symbolicName(jarData);
            if (declared != null && !declared.equals(pluginName)) {
                incoming.setParam("status","9");
                incoming.setParam("status_desc","pluginname does not match the jar's Bundle-SymbolicName");
                return incoming;
            }
            File repoDir = getRepoDir();
            Path target = repoDir.toPath().resolve(pluginJarFile);
            // the shared side of the repository gate, as for putfiles: a jar never lands mid-clear
            if (!repoEngine.enterGate()) {
                incoming.setParam("status","9");
                incoming.setParam("status_desc","repository busy: a clear holds it");
                return incoming;
            }
            try {
                staged = repoEngine.privateStaging().resolve(RepoEngine.STAGING_PREFIX
                        + UUID.randomUUID().toString().replace("-", "") + RepoEngine.STAGING_SUFFIX);
                Files.write(staged, jarData, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                jarData = null;
                String verified;
                try {
                    verified = jarVerifier.verify(staged);
                } catch (Exception refused) {
                    logger.warn("putjar " + pluginJarFile + " refused: " + refused.getMessage());
                    incoming.setParam("status","9");
                    incoming.setParam("status_desc","jar refused: " + refused.getMessage());
                    incoming.setParam("refusal","signature");
                    return incoming;
                }
                if (!sha384.equals(verified) || !sha384.equals(Hashes.sha384(staged))) {
                    logger.error("putjar " + pluginJarFile + ": the staged jar changed during verification; refused");
                    incoming.setParam("status","9");
                    incoming.setParam("status_desc","jar refused: the staged jar changed while it was verified");
                    incoming.setParam("refusal","changed");
                    return incoming;
                }
                repoEngine.markLanded();
                Files.move(staged, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                staged = null;
                if (!sha384.equals(Hashes.sha384(target))) {
                    Files.deleteIfExists(target);
                    logger.error("putjar " + pluginJarFile + ": the jar changed while it was moved into place; removed");
                    incoming.setParam("status","9");
                    incoming.setParam("status_desc","jar refused: it changed while it was moved into place");
                    incoming.setParam("refusal","changed");
                    return incoming;
                }
                jarInventory.invalidate(target);
                incoming.setParam("uploaded", pluginName);
                incoming.setParam("md5-confirm", contentHash);
                incoming.setParam("sha384", sha384);
                incoming.setParam("status","10");
                incoming.setParam("status_desc","uploaded");
            } finally {
                repoEngine.exitGate();
            }

        } catch(Exception ex){
            logger.error("putPluginJar error", ex);
            incoming.setParam("status","6");
            incoming.setParam("status_desc","putjar error: " + ex.getClass().getSimpleName());
        } finally {
            // a partial, refused or unverified jar is never left where the inventory would advertise and serve it
            if (staged != null) {
                try { Files.deleteIfExists(staged); } catch (Exception ignore) {}
            }
        }

        if(incoming.getParams().containsKey("jardata")) {
            incoming.removeParam("jardata");
        }

        return incoming;
    }

    /** The bundle's Bundle-SymbolicName without directives, read from the bytes in memory; null if none. */
    private static String symbolicName(byte[] jar) {
        try (java.util.jar.JarInputStream in = new java.util.jar.JarInputStream(new java.io.ByteArrayInputStream(jar))) {
            java.util.jar.Manifest mf = in.getManifest();
            if (mf == null) return null;
            String v = mf.getMainAttributes().getValue("Bundle-SymbolicName");
            if (v == null) return null;
            int semi = v.indexOf(';');
            return (semi < 0 ? v : v.substring(0, semi)).trim();
        } catch (Exception ex) {
            return null;
        }
    }

    /**
     * getjar: the inventory is cached by size, mtime and file key; the bytes sent are rechecked against the requested
     * hash, so a jar changed in place is never served under its old hash.
     */
    private MsgEvent getPluginJar(MsgEvent incoming) {

        try {
            String requestPluginName = incoming.getParam("action_pluginname");
            String requestPluginMD5 = incoming.getParam("action_pluginmd5");
            if (requestPluginName == null || requestPluginMD5 == null) {
                incoming.setParam("status","8");
                incoming.setParam("status_desc","getjar takes action_pluginname and action_pluginmd5");
                return incoming;
            }
            File repoDir = getRepoDir();
            for (JarInventory.Entry e : jarInventory.list(repoDir)) {
                if (!requestPluginName.equals(e.pluginname) || !requestPluginMD5.equals(e.md5)) continue;
                Path jar = repoDir.toPath().resolve(e.jarfile);
                if (e.size > maxInlineBytes) {
                    incoming.setParam("status","5");
                    incoming.setParam("status_desc","jar " + e.size + " bytes exceeds max_inline_bytes; use streamfile");
                    return incoming;
                }
                byte[] data = Files.readAllBytes(jar);
                if (data.length > maxInlineBytes || !requestPluginMD5.equals(Hashes.content(data))) {
                    jarInventory.invalidate(jar);
                    incoming.setParam("status","9");
                    incoming.setParam("status_desc","the jar changed since it was listed; list again");
                    return incoming;
                }
                incoming.setDataParam("jardata", data);
                incoming.setParam("status","10");
                incoming.setParam("status_desc","found");
                return incoming;
            }
            incoming.setParam("status","9");
            incoming.setParam("status_desc","no such jar");
        } catch(Exception ex) {
            logger.error("getPluginJar error", ex);
            incoming.setParam("status","7");
            incoming.setParam("status_desc","getjar error: " + ex.getClass().getSimpleName());
        }
        return incoming;
    }

    /** A file opened to be served, and checked on the descriptor it will be read through. */
    private static final class Served {
        final FileChannel channel;
        final long size;

        Served(FileChannel channel, long size) {
            this.channel = channel;
            this.size = size;
        }
    }

    /** Why a file cannot be served, as a status and description (the open itself refused it). */
    private static final class NotServed extends Exception {
        final String status;
        final String refusal;

        NotServed(String status, String desc, String refusal) {
            super(desc);
            this.status = status;
            this.refusal = refusal;
        }
    }

    /**
     * Open a cataloged file to serve it, then check it on that same descriptor (the TOCTOU fix): the final component is
     * opened without following a link, the file must still be the one the path named a moment before (same file key),
     * a regular file, inside this repository's directory and in no live-tier root once every link is resolved, and in
     * the catalog. Its size is the open file's own. Nothing swapped in after the check can be read: the bytes come
     * through this descriptor. The caller owns the returned channel.
     */
    private Served openServable(String filePath, String missingStatus) throws NotServed {
        Path p = Paths.get(filePath);
        String lexical = repoEngine.liveRootConflict(p);
        if (lexical != null) throw new NotServed("9", "the file " + lexical, "live-root");
        Map<String, String> cataloged = repoEngine.getFileInfo(filePath);
        if (cataloged == null) {
            throw new NotServed("9", "file not in catalog", null);
        }
        Path base;
        try {
            base = getRepoDir().getCanonicalFile().toPath();
        } catch (IOException ex) {
            throw new NotServed(missingStatus, "the repository directory cannot be resolved", null);
        }
        // Where the platform opens relative to a directory descriptor (SecureDirectoryStream: Linux), every directory
        // from the repository root down and the file itself are opened by descriptor, without following a link: no link
        // anywhere on the path can redirect the open, whatever is swapped in meanwhile.
        try (DirectoryStream<Path> rootStream = Files.newDirectoryStream(base)) {
            if (rootStream instanceof SecureDirectoryStream) {
                return openRelative(base, p, (SecureDirectoryStream<Path>) rootStream, missingStatus);
            }
        } catch (NotServed n) {
            throw n;
        } catch (IOException ex) {
            throw new NotServed(missingStatus, "the repository directory cannot be opened", null);
        }
        Served byPath = openByPath(p, base, missingStatus);
        // No descriptor-relative open here: a directory on the path swapped for a link in the instant between the path
        // checks and the open is not seen by them. So what was opened is proven by what it holds: every byte is read
        // through the open descriptor and must hash to the content the catalog holds for this file; anything else (a
        // file outside the repository, or one changed since it was cataloged) is never served. The bytes then served
        // come through that same descriptor. This costs one extra read of the file, on these platforms only.
        boolean ok = false;
        try {
            String held = Hashes.content(byPath.channel);
            if (!held.equals(cataloged.get("md5"))) {
                throw new NotServed("9", "the file opened does not hold its cataloged content (changed since it was"
                        + " cataloged, or not this repository's file)", null);
            }
            ok = true;
            return byPath;
        } catch (IOException ex) {
            throw new NotServed(missingStatus, "file could not be read: " + ex.getClass().getSimpleName(), null);
        } finally {
            if (!ok) {
                try { byPath.channel.close(); } catch (IOException ignore) { }
            }
        }
    }

    /** The descriptor-relative open (see openServable). */
    private Served openRelative(Path base, Path p, SecureDirectoryStream<Path> root, String missingStatus) throws NotServed {
        Path rel;
        try {
            Path parentReal = p.toAbsolutePath().getParent().toRealPath();
            if (!parentReal.startsWith(base)) throw new NotServed("9", "the file is not in this repository's directory", null);
            rel = base.relativize(parentReal.resolve(p.getFileName()));
        } catch (NotServed n) {
            throw n;
        } catch (IOException ex) {
            throw new NotServed(missingStatus, "file does not exist on this agent", null);
        }
        List<SecureDirectoryStream<Path>> opened = new ArrayList<>();
        SecureDirectoryStream<Path> dir = root;
        FileChannel ch = null;
        boolean ok = false;
        try {
            for (int i = 0; i < rel.getNameCount() - 1; i++) {
                dir = dir.newDirectoryStream(rel.getName(i), LinkOption.NOFOLLOW_LINKS);
                opened.add(dir);
            }
            Path name = rel.getFileName();
            BasicFileAttributes before = dir.getFileAttributeView(name, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS).readAttributes();
            if (!before.isRegularFile()) throw new NotServed("9", "not a regular file", null);
            ch = (FileChannel) dir.newByteChannel(name, new HashSet<java.nio.file.OpenOption>(
                    Arrays.asList(StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)));
            BasicFileAttributes after = dir.getFileAttributeView(name, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS).readAttributes();
            if (!after.isRegularFile() || !Objects.equals(before.fileKey(), after.fileKey())) {
                throw new NotServed("9", "the file was replaced while it was being opened", null);
            }
            String conflict = repoEngine.liveRootConflict(base.resolve(rel));
            if (conflict != null) throw new NotServed("9", "the file " + conflict, "live-root");
            Served s = new Served(ch, ch.size());
            ok = true;
            return s;
        } catch (NotServed n) {
            throw n;
        } catch (NoSuchFileException ex) {
            throw new NotServed(missingStatus, "file does not exist on this agent", null);
        } catch (IOException | ClassCastException ex) {
            throw new NotServed("9", "not a regular file reached without links", null);
        } finally {
            for (SecureDirectoryStream<Path> d : opened) {
                try { d.close(); } catch (IOException ignore) { }
            }
            if (!ok && ch != null) {
                try { ch.close(); } catch (IOException ignore) { }
            }
        }
    }

    /**
     * The open where no descriptor-relative open exists (macOS, Windows): the final component is opened without
     * following a link, and the file is checked by path around the open (same file key before and after, a regular
     * file, inside the repository and in no live-tier root once every link is resolved). A directory on the path that is
     * swapped for a link in the instant between the check and the open is not caught by these checks: openServable
     * then proves the open file by its content (see there).
     */
    private Served openByPath(Path p, Path base, String missingStatus) throws NotServed {
        BasicFileAttributes before;
        try {
            before = Files.readAttributes(p, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (IOException ex) {
            throw new NotServed(missingStatus, "file does not exist on this agent", null);
        }
        if (!before.isRegularFile()) {
            // a link, a directory, a device: never opened at all
            throw new NotServed("9", "not a regular file", null);
        }
        FileChannel ch;
        try {
            ch = FileChannel.open(p, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
        } catch (NoSuchFileException ex) {
            throw new NotServed(missingStatus, "file does not exist on this agent", null);
        } catch (FileSystemException ex) {
            throw new NotServed("9", "not a regular file", null);
        } catch (IOException ex) {
            throw new NotServed(missingStatus, "file could not be opened", null);
        }
        boolean ok = false;
        try {
            BasicFileAttributes after = Files.readAttributes(p, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!after.isRegularFile()) {
                throw new NotServed("9", "not a regular file", null);
            }
            if (!Objects.equals(before.fileKey(), after.fileKey())) {
                throw new NotServed("9", "the file was replaced while it was being opened", null);
            }
            Path real = p.toRealPath();
            String conflict = repoEngine.liveRootConflict(real);
            if (conflict != null) throw new NotServed("9", "the file " + conflict, "live-root");
            if (!real.startsWith(base)) {
                throw new NotServed("9", "the file is not in this repository's directory", null);
            }
            Served s = new Served(ch, ch.size());
            ok = true;
            return s;
        } catch (NotServed n) {
            throw n;
        } catch (IOException ex) {
            throw new NotServed(missingStatus, "file could not be checked: " + ex.getClass().getSimpleName(), null);
        } finally {
            if (!ok) {
                try { ch.close(); } catch (IOException ignore) { }
            }
        }
    }

    private MsgEvent getFile(MsgEvent incoming) {

        try {

            if(incoming.getParam("file_path") != null){
                String filePath = incoming.getParam("file_path");
                String conflict = repoEngine.liveRootConflict(Paths.get(filePath));
                if (conflict != null) {
                    return refuseLiveRoot(incoming, "the file " + conflict);
                }
                Map<String,String> fileInfo = repoEngine.getFileInfo(filePath);
                if (fileInfo == null) {
                    incoming.setParam("status","9");
                    incoming.setParam("status_desc","fileInfo == null");
                    return incoming;
                }
                Served served;
                try {
                    served = openServable(filePath, "6");
                } catch (NotServed n) {
                    if ("live-root".equals(n.refusal)) return refuseLiveRoot(incoming, n.getMessage());
                    incoming.setParam("status", n.status);
                    incoming.setParam("status_desc", n.getMessage());
                    return incoming;
                }
                try (FileChannel ch = served.channel) {
                    if (served.size > maxInlineBytes) {
                        // Memory safeguard: never slurp a huge file onto the heap for an inline reply.
                        incoming.setParam("status","5");
                        incoming.setParam("status_desc","file " + served.size + " bytes exceeds max_inline_bytes "
                                + maxInlineBytes + "; use streamfile");
                    } else {
                        ByteBuffer buf = ByteBuffer.allocate((int) served.size);
                        while (buf.hasRemaining() && ch.read(buf, buf.position()) > 0) { }
                        incoming.setCompressedParam("file_metadata",gson.toJson(fileInfo));
                        incoming.setDataParam("file_data", Arrays.copyOf(buf.array(), buf.position()));
                        incoming.setParam("status","10");
                        incoming.setParam("status_desc","found list");
                    }
                }
            } else {
                incoming.setParam("status","8");
                incoming.setParam("status_desc","no filename parameter");
            }

        } catch(Exception ex) {
            incoming.setParam("status","7");
            incoming.setParam("status_desc","getFile error " + ex.getClass().getSimpleName());
            logger.error("getFile error", ex);
        }
        return incoming;
    }

    private MsgEvent getScanDir(MsgEvent incoming) {

        try {

            String scanDirString = plugin.getConfig().getStringParam("scan_dir");
            if(scanDirString != null) {
                incoming.setParam("scan_dir", scanDirString);
                incoming.setParam("status","10");
                incoming.setParam("status_desc","found scan_dir");
            } else {
                incoming.setParam("status","9");
                incoming.setParam("status_desc","scan_dir == null");
            }


        } catch(Exception ex) {
            incoming.setParam("status","7");
            incoming.setParam("status_desc","getScanDir error " + ex.getMessage());
            logger.error("filerepo error", ex);
        }
        return incoming;
    }

    /** Why {@code identKey} cannot name a frame property, or null. */
    private static String badIdentKey(String identKey) {
        if (identKey == null) return "ident_key is required";
        if (!PROPERTY_NAME.matcher(identKey).matches()) return "ident_key must be a property name ([A-Za-z_][A-Za-z0-9_]*, at most 64)";
        if (identKey.toUpperCase(Locale.ROOT).startsWith("JMS")) return "ident_key may not start with JMS";
        if (FRAME_PROPERTIES.contains(identKey)) return "ident_key may not be " + identKey + ": the frames carry it themselves";
        if (SELECTOR_WORDS.contains(identKey.toUpperCase(Locale.ROOT))) return "ident_key may not be a selector word";
        return null;
    }

    /**
     * streamfile (FR-D6): the transfer id is the caller's own (validated; unique among the caller's transfers) or one
     * generated here; the file is opened and checked on its descriptor before the transfer is accepted (TOCTOU);
     * every chunk carries its offset; a terminal frame ends every accepted transfer (status 10 with the SHA-384 of the
     * bytes sent and their count, or the reason it stopped). Bounded (FR-D5): busy (11) when the pool's queue or the
     * caller's own cap is full.
     */
    private MsgEvent streamFile(MsgEvent incoming) {

        try {
            String filePath = incoming.getParam("file_path");
            if (filePath == null) {
                incoming.setParam("status","6");
                incoming.setParam("status_desc","no file_path parameter");
                return incoming;
            }
            String conflict = repoEngine.liveRootConflict(Paths.get(filePath));
            if (conflict != null) {
                return refuseLiveRoot(incoming, "the file " + conflict);
            }
            long startByte, byteLength;
            try {
                startByte = Long.parseLong(incoming.getParam("start_byte"));
                byteLength = Long.parseLong(incoming.getParam("byte_length"));
            } catch (Exception bad) {
                incoming.setParam("status", "8");
                incoming.setParam("status_desc", "start_byte and byte_length are whole numbers");
                return incoming;
            }
            String identKey = incoming.getParam("ident_key");
            String identId = incoming.getParam("ident_id");
            String why = badIdentKey(identKey);
            if (why == null && (identId == null || identId.length() > 1024)) why = "ident_id is required (at most 1024 characters)";
            if (why != null) {
                incoming.setParam("status", "8");
                incoming.setParam("status_desc", why);
                return incoming;
            }
            String transferId = incoming.getParam("transfer_id");
            if (transferId == null || transferId.isEmpty()) {
                transferId = "fr-" + SyncReceiver.token();
            } else if (!TRANSFER_ID.matcher(transferId).matches()) {
                incoming.setParam("status", "8");
                incoming.setParam("status_desc", "transfer_id is 1-128 of letters, digits, '_', '.', ':' or '-'");
                return incoming;
            }
            int reqBuf = defaultStreamBuffer;
            try { if (incoming.getParam("buffer_size") != null) reqBuf = Integer.parseInt(incoming.getParam("buffer_size")); }
            catch (Exception ignore) { }
            // Clamp the chunk size: each chunk carries a small framing header and must stay under the dataplane frame
            // limit, else chunks are silently dropped.
            if (reqBuf > MAX_STREAM_BUFFER) reqBuf = MAX_STREAM_BUFFER;
            if (reqBuf < 4096) reqBuf = 4096;

            String owner = Callers.key(incoming, insecure);
            if (owner == null) {
                incoming.setParam("status", "9");
                incoming.setParam("status_desc", "unauthorized: no verified caller (a transfer belongs to its caller)");
                incoming.setParam("refusal", "no-caller");
                return incoming;
            }
            String key = owner + "|" + transferId;

            Served served;
            try {
                served = openServable(filePath, "7");
            } catch (NotServed n) {
                if ("live-root".equals(n.refusal)) return refuseLiveRoot(incoming, n.getMessage());
                incoming.setParam("status", n.status);
                incoming.setParam("status_desc", n.getMessage());
                return incoming;
            }
            boolean handedOff = false;
            try {
                // overflow-safe range validation against the OPEN file's size
                if (!(startByte >= 0 && byteLength > 0 && startByte <= served.size && byteLength <= served.size - startByte)) {
                    incoming.setParam("status", "8");
                    incoming.setParam("status_desc", "invalid byte range");
                    return incoming;
                }
                StreamObject so = new StreamObject(transferId, filePath, startByte, byteLength);
                if (transferStreams.putIfAbsent(key, so) != null) {
                    incoming.setParam("status", "8");
                    incoming.setParam("status_desc", "transfer_id is in use by another of this caller's transfers");
                    return incoming;
                }
                final String tid = transferId;
                final int buffer = reqBuf;
                final FileChannel channel = served.channel;
                so.setChannel(channel);
                // a subscriber of this producer fetching what it was offered may use the threads kept for sync
                boolean sync = repoEngine.isSubscriberCaller(incoming);
                TransferScheduler.Admit admit = transfers.submit(owner, sync, () -> stream(channel, so, key, tid, identKey, identId, buffer));
                if (admit == TransferScheduler.Admit.CALLER_FULL || admit == TransferScheduler.Admit.FULL) {
                    so.setChannel(null);
                    transferStreams.remove(key, so);
                    transfersRejected.incrementAndGet();
                    incoming.setParam("status", "11");
                    incoming.setParam("status_desc", admit == TransferScheduler.Admit.CALLER_FULL
                            ? "busy: this caller has transfer_max_per_caller transfers in flight, retry later"
                            : "busy: the transfer queue is full (transfer_queue_max), retry later");
                    return incoming;
                }
                handedOff = true;
                incoming.setParam("transfer_id", transferId);
                incoming.setParam("status", "10");
                incoming.setParam("status_desc", "transfer accepted");
                return incoming;
            } finally {
                if (!handedOff) {
                    try { served.channel.close(); } catch (IOException ignore) { }
                }
            }

        } catch(Exception ex) {
            incoming.setParam("status","5");
            incoming.setParam("status_desc","streamfile error " + ex.getClass().getSimpleName());
            logger.error("filerepo error", ex);
        }
        return incoming;
    }

    /** The streamer: chunks with their offsets, then the terminal frame, whatever happened. */
    private void stream(FileChannel channel, StreamObject so, String key, String transferId, String identKey,
                        String identId, int bufferSize) {
        DataPlaneService dp = plugin.getAgentService().getDataPlaneService();
        MessageDigest sha = Hashes.sha384();
        long sent = 0;
        int seqNum = 0;
        String status = "10", desc = "complete";
        try (FileChannel ch = channel) {
            Runnable hook = beforeStream;
            if (hook != null) hook.run();
            if (!so.isActive()) {
                status = "9";
                desc = "cancelled";
            } else {
                ByteBuffer buf = ByteBuffer.allocate(bufferSize);
                long pos = so.getStartByte();
                long remaining = so.getByteLength();
                while (remaining > 0) {
                    if (!so.isActive()) {
                        status = "9";
                        desc = "cancelled";
                        break;
                    }
                    buf.clear();
                    buf.limit((int) Math.min((long) bufferSize, remaining));
                    // Honor the ACTUAL bytes read: a read may return fewer bytes than requested
                    int got = ch.read(buf, pos);
                    if (got <= 0) {
                        status = "8";
                        desc = "the file is shorter than the requested range (it changed while streaming)";
                        break;
                    }
                    sha.update(buf.array(), 0, got);
                    BytesMessage m = dp.createBytesMessage();
                    m.writeBytes(buf.array(), 0, got);
                    m.setStringProperty(identKey, identId);
                    m.setStringProperty("transfer_id", transferId);
                    m.setStringProperty("seq_num", String.valueOf(seqNum));
                    m.setStringProperty("offset", String.valueOf(pos));
                    if (!dp.sendMessage(TopicType.GLOBAL, m, DeliveryMode.NON_PERSISTENT, 0, 0)) {
                        status = "8";
                        desc = "the dataplane did not take a frame";
                        break;
                    }
                    pos += got;
                    remaining -= got;
                    sent += got;
                    seqNum += 1;
                    so.setBytesTransfered(sent);
                }
            }
        } catch (Exception ex) {
            status = "8";
            desc = "read or send failed: " + ex.getClass().getSimpleName();
            logger.error("streamFile " + transferId + " error: " + ex.getMessage());
        } finally {
            try {
                BytesMessage end = dp.createBytesMessage();
                end.setStringProperty(identKey, identId);
                end.setStringProperty("transfer_id", transferId);
                end.setStringProperty("seq_num", String.valueOf(seqNum));
                end.setStringProperty("offset", String.valueOf(so.getStartByte() + sent));
                end.setStringProperty("transfer_end", "true");
                end.setStringProperty("transfer_status", status);
                end.setStringProperty("transfer_desc", desc);
                end.setStringProperty("total_bytes", String.valueOf(sent));
                end.setStringProperty("start_byte", String.valueOf(so.getStartByte()));
                end.setStringProperty("byte_length", String.valueOf(so.getByteLength()));
                if ("10".equals(status)) end.setStringProperty("sha384", Hashes.hex(sha.digest()));
                dp.sendMessage(TopicType.GLOBAL, end, DeliveryMode.PERSISTENT, 0, 0);
            } catch (Exception ex) {
                logger.error("streamFile " + transferId + ": terminal frame not sent: " + ex.getMessage());
            }
            // Remove the finished/failed transfer so the map doesn't grow unbounded
            so.setChannel(null);
            transferStreams.remove(key, so);
        }
    }

    private MsgEvent streamFileCancel(MsgEvent incoming) {
        try {
            incoming.setParam("status_code","9");
            String transferId = incoming.getParam("transfer_id");
            if (transferId != null) {
                // only the caller's own transfer: no one cancels another caller's
                String owner = Callers.key(incoming, insecure);
                StreamObject so = owner == null ? null : transferStreams.get(owner + "|" + transferId);
                if (so != null) {
                    so.setActive(false);
                    incoming.setParam("status_code","10");
                    logger.info("streamFileCancel transferId: " + transferId + " set canceled transfered " + so.getBytesTransfered() + " bytes." );
                } else {
                    logger.debug("streamFileCancel transferId: " + transferId + " not found among the caller's transfers");
                }
            } else {
                logger.error("streamFileCancel transferId not found in MsgEvent!");
            }

        } catch(Exception ex) {
            incoming.setParam("status","7");
            incoming.setParam("status_desc","streamfilecancel error " + ex.getMessage());
            logger.error("filerepo error", ex);
        }
        return incoming;
    }


    private void confirmTransfer(MsgEvent incoming) {
        // the subscriber is who the verified caller says, not what the message's source fields claim
        String[] from = Callers.plugin(incoming, insecure);
        if (from == null) {
            logger.warn("repoconfirm ignored: no verified plugin caller");
            return;
        }
        String gen = incoming.getParam("sync_gen");
        if (gen == null) {
            logger.warn("repoconfirm ignored: no sync_gen (a subscriber of an earlier protocol build)");
            return;
        }
        repoEngine.confirmTransfer(incoming.getParam("transfer_id"), incoming.getCompressedParam("failedfiles"),
                incoming.getCompressedParam("faileddeletes"), gen, from[0], from[1], from[2]);
    }

    /**
     * putfilesremote is removed (coordinator decision, 2026-11-04): it read any path on this agent the caller named and
     * relayed it to any plugin, and a relayed putfiles arrives as a plugin caller, which may not land files. It is
     * refused, and the caller is told to send putfiles to the target repository directly, as an admin.
     */
    private MsgEvent putFileRemote(MsgEvent incoming) {
        // a path in a live-tier root is still refused as such first (the live tier's rule, whatever else happens)
        try {
            String listJson = incoming.getCompressedParam("file_list");
            List<String> fileList = listJson == null ? null : gson.fromJson(listJson, listType);
            for (String f : fileList == null ? Collections.<String>emptyList() : fileList) {
                String conflict = repoEngine.liveRootConflict(Paths.get(f));
                if (conflict != null) {
                    incoming.removeParam("file_list");
                    return refuseLiveRoot(incoming, "a listed file " + conflict);
                }
            }
        } catch (Exception unparseable) {
            // refused below either way
        }
        if (incoming.getParams().containsKey("file_list")) incoming.removeParam("file_list");
        incoming.setParam("status","9");
        incoming.setParam("status_desc","refused: putfilesremote is removed; send putfiles (with file_hashes) to the target"
                + " repository directly, as an admin");
        incoming.setParam("refusal","removed");
        return incoming;
    }

    /** Refuse, when {@code repo_name} is given and this repository has a name, a call that names another repository. */
    private MsgEvent refuseOtherRepo(MsgEvent incoming) {
        String asked = incoming.getParam("repo_name");
        String mine = repoEngine.getRepoName();
        if (asked == null || mine == null || asked.equals(mine)) return null;
        incoming.setParam("status","9");
        incoming.setParam("status_desc","refused: repo_name '" + asked + "' does not name this repository");
        incoming.setParam("refusal","repo-name");
        return incoming;
    }

    /**
     * clearrepo (FR-D2): wipes this repository's whole directory, so it must be confirmed with that directory's exact
     * name; repo_name, when given, must name this repository; force (clear an only copy) needs the admin role, and no
     * force clears a protected directory or one that is or holds a live-tier root.
     */
    private MsgEvent clearRepo(MsgEvent incoming) {

        try {
            MsgEvent other = refuseOtherRepo(incoming);
            if (other != null) return other;

            boolean force = "true".equalsIgnoreCase(incoming.getParam("force"));
            if (force && !Callers.hasRole(incoming, Callers.ROLE_ADMIN, insecure)) {
                incoming.setParam("status","9");
                incoming.setParam("status_desc","refused: force=true needs the admin role");
                incoming.setParam("refusal","force-needs-admin");
                return incoming;
            }
            // force=true clears an only copy too: an admin who sends it has decided the contents are disposable.
            // The check runs inside clearRepo, under the repository gate, never before it.
            RepoEngine.Clear cleared = repoEngine.clearRepo(incoming.getParam("confirm"), force);
            if (cleared.state != null && cleared.state.state != null) {
                incoming.setParam("state", cleared.state.state);
            }
            switch (cleared.outcome) {
                case RepoEngine.Clear.CLEARED:
                    incoming.setParam("status","10");
                    incoming.setParam("status_desc", cleared.remaining == 0 ? "repo cleared" : "repo cleared; " + cleared.why);
                    break;
                case RepoEngine.Clear.ONLY_COPY:
                    incoming.setParam("status","9");
                    incoming.setParam("status_desc","refused: this repository is the only copy of what it holds ("
                            + cleared.why + "); an admin may pass force=true to clear it anyway");
                    incoming.setParam("refusal","only-copy");
                    break;
                case RepoEngine.Clear.UNCONFIRMED:
                    incoming.setParam("status","9");
                    incoming.setParam("status_desc","refused: " + cleared.why);
                    incoming.setParam("refusal","unconfirmed");
                    break;
                case RepoEngine.Clear.PROTECTED:
                    incoming.setParam("status","9");
                    incoming.setParam("status_desc","refused: " + cleared.why);
                    incoming.setParam("refusal","protected-dir");
                    break;
                case RepoEngine.Clear.LIVE_ROOT:
                    return refuseLiveRoot(incoming, cleared.why);
                default:
                    incoming.setParam("status","9");
                    incoming.setParam("status_desc","repo not cleared: " + cleared.why);
                    break;
            }

        } catch(Exception ex){
            logger.error("clearrepo: " + ex.getMessage());
            incoming.setParam("status","8");
            incoming.setParam("status_desc","clear repo error: " + ex.getMessage());
        }

        return incoming;
    }

    /** releasedeletes: let the next scan offer the deletes the mass-delete guard holds; confirmed like clearrepo. */
    private MsgEvent releaseDeletes(MsgEvent incoming) {
        if (!repoEngine.confirms(incoming.getParam("confirm"))) {
            incoming.setParam("status","9");
            incoming.setParam("status_desc","refused: confirm must name this repository's directory exactly");
            incoming.setParam("refusal","unconfirmed");
            return incoming;
        }
        int released = repoEngine.releaseHeldDeletes(incoming.getParam("hold_id"));
        if (released == DeleteGuard.RELEASE_NEEDS_ID) {
            incoming.setParam("status","9");
            incoming.setParam("status_desc","refused: name the held set to release (hold_id: repostate's deletes_hold_id)");
            incoming.setParam("refusal","hold-id-required");
            return incoming;
        }
        if (released < 0) {
            incoming.setParam("status","9");
            incoming.setParam("status_desc","refused: the held deletes changed; read repostate's deletes_hold_id again");
            incoming.setParam("refusal","hold-changed");
            return incoming;
        }
        incoming.setParam("released", String.valueOf(released));
        incoming.setParam("status", released > 0 ? "10" : "9");
        incoming.setParam("status_desc", released > 0 ? released + " held delete(s) released: the next scan records them"
                + " if it finds exactly that set held; the release is spent by that scan whatever it finds"
                : "no deletes are held");
        if (released > 0) incoming.setParam("hold_id", String.valueOf(repoEngine.getHoldId()));
        return incoming;
    }

    /** releaserewrites: offer the rewrites the rewrite guard holds; confirmed like clearrepo, naming the held set. */
    private MsgEvent releaseRewrites(MsgEvent incoming) {
        if (!repoEngine.confirms(incoming.getParam("confirm"))) {
            incoming.setParam("status","9");
            incoming.setParam("status_desc","refused: confirm must name this repository's directory exactly");
            incoming.setParam("refusal","unconfirmed");
            return incoming;
        }
        int released = repoEngine.releaseHeldRewrites(incoming.getParam("hold_id"));
        if (released == DeleteGuard.RELEASE_NEEDS_ID) {
            incoming.setParam("status","9");
            incoming.setParam("status_desc","refused: name the held set to release (hold_id: repostate's rewrites_hold_id)");
            incoming.setParam("refusal","hold-id-required");
            return incoming;
        }
        if (released < 0) {
            incoming.setParam("status","9");
            incoming.setParam("status_desc","refused: the held rewrites changed; read repostate's rewrites_hold_id again");
            incoming.setParam("refusal","hold-changed");
            return incoming;
        }
        incoming.setParam("released", String.valueOf(released));
        incoming.setParam("status", released > 0 ? "10" : "9");
        incoming.setParam("status_desc", released > 0 ? released + " held rewrite(s) released: offered to every subscriber"
                : "no rewrites are held");
        return incoming;
    }

    /** A live-backed repository's answer to every verb outside LIVE_BACKED_VERBS. */
    private MsgEvent refuseLiveBacked(MsgEvent incoming) {
        // drop any payload the refused verb carried, as the verbs themselves do
        for (String payload : new String[]{"jardata", "filedata", "file_data", "repolistin"}) {
            if (incoming.getParams().containsKey(payload)) incoming.removeParam(payload);
        }
        incoming.setParam("status","9");
        // the sync verbs report through status_code
        incoming.setParam("status_code","9");
        incoming.setParam("status_desc","refused: this repository is backed by live-tier row " + repoEngine.getLiveRow()
                + ", read only through the filesystem at this node; filerepo does not serve, write, delete, sync or list it");
        incoming.setParam("refusal","live-backed");
        return incoming;
    }

    /**
     * The refusal of anything in a live-tier root, whatever the repository: those files are the provider's, read only
     * through the filesystem at this node, never served, landed, deleted or synced through filerepo.
     */
    private MsgEvent refuseLiveRoot(MsgEvent incoming, String why) {
        for (String payload : new String[]{"jardata", "filedata", "file_data", "repolistin"}) {
            if (incoming.getParams().containsKey(payload)) incoming.removeParam(payload);
        }
        incoming.setParam("status","9");
        incoming.setParam("status_code","9");
        incoming.setParam("status_desc","refused: " + why + "; files in a live-tier root are read only through the filesystem at this node");
        incoming.setParam("refusal","live-root");
        return incoming;
    }

    /**
     * repostate: what this repository is (LIVE, WORKSPACE_CLEAN, WORKSPACE_DIRTY, MIRROR or ONLY_COPY), from the
     * live-tier provider's cached answers or from filerepo's own catalog; never a walk of the tree.
     */
    private MsgEvent repoState(MsgEvent incoming) {
        try {
            RepoState state = repoEngine.isLiveBacked()
                    ? RepoState.live(repoEngine.getLiveRow(), liveTier)
                    : repoEngine.localState();
            state.into(incoming);
        } catch (Exception ex) {
            logger.error("repostate error", ex);
            incoming.setParam("status","8");
            incoming.setParam("status_desc","repostate error: " + ex.getMessage());
        }
        return incoming;
    }

    /**
     * liverelay: forward a request already signed by its end user, unchanged, through the live-tier provider, and
     * return its reply as it came. Whoever signed the request is the one authorised and audited; filerepo adds nothing
     * and signs nothing.
     */
    private MsgEvent liveRelay(MsgEvent incoming) {
        try {
            String requestJson = incoming.getCompressedParam("request");
            Map<String, String> request = null;
            try {
                if (requestJson != null) request = gson.fromJson(requestJson, stringMapType);
            } catch (Exception unparseable) {
                request = null;
            }
            if (incoming.getParams().containsKey("request")) incoming.removeParam("request");
            if (request == null || request.isEmpty()) {
                incoming.setParam("status","8");
                incoming.setParam("status_desc","liverelay takes request: the signed request, as a compressed JSON object of strings");
                return incoming;
            }
            LiveTierClient.Answer answer = liveTier == null ? null : liveTier.relay(request);
            if (answer == null || !answer.answered()) {
                incoming.setParam("status","11");
                incoming.setParam("status_desc","not relayed: " + (answer == null ? "no live-tier provider admits this bundle" : answer.desc()));
                return incoming;
            }
            // 10 says the provider answered; the answer itself (the core's reply, or the provider's refusal to relay)
            // is in reply, with its own status
            incoming.setCompressedParam("reply", gson.toJson(answer.reply));
            incoming.setParam("reply_status", answer.status());
            incoming.setParam("status","10");
            incoming.setParam("status_desc","answered (reply_status " + answer.status() + ")");
        } catch (Exception ex) {
            logger.error("liverelay error", ex);
            incoming.setParam("status","8");
            incoming.setParam("status_desc","liverelay error: " + ex.getMessage());
        }
        return incoming;
    }

    /**
     * liverealise: realise this live-backed repository's own copy again, as the node itself: copies only, at this node
     * only. Asking again for a copy that is still held keeps it. What is realised comes from the provider's own row, in
     * this JVM: no collection, version or path is taken from the request or put on the reply.
     */
    private MsgEvent liveRealise(MsgEvent incoming) {
        try {
            if (!repoEngine.isLiveBacked()) {
                incoming.setParam("status","9");
                incoming.setParam("status_desc","refused: this repository is not live-backed (live_row is not configured)");
                incoming.setParam("refusal","not-live-backed");
                return incoming;
            }
            String leaseMs = incoming.getParam("lease_ms");
            if (leaseMs != null && !leaseMs.matches("[0-9]{1,18}")) {
                incoming.setParam("status","8");
                incoming.setParam("status_desc","lease_ms is a whole number of milliseconds");
                return incoming;
            }
            if (leaseMs != null && (Long.parseLong(leaseMs) < 1 || Long.parseLong(leaseMs) > liveLeaseMaxMs)) {
                incoming.setParam("status","8");
                incoming.setParam("status_desc","lease_ms is at most live_lease_max_ms (" + liveLeaseMaxMs + ")");
                return incoming;
            }
            String requestId = incoming.getParam("request_id");
            if (requestId == null) {
                requestId = "filerepo-" + UUID.randomUUID().toString().replace("-", "");
            } else if (!requestId.matches("[A-Za-z0-9_-]{1,64}")) {
                incoming.setParam("status","8");
                incoming.setParam("status_desc","request_id is 1-64 letters, digits, - or _");
                return incoming;
            }
            if (liveTier == null) {
                incoming.setParam("status","11");
                incoming.setParam("status_desc","not sent: no live-tier provider admits this bundle");
                return incoming;
            }
            LiveTierClient.Listing listing = liveTier.listRow(repoEngine.getLiveRow());
            LiveTierClient.Answer list = listing.answer;
            if (!list.ok()) {
                incoming.setParam("status","11");
                incoming.setParam("status_desc","not sent: the live-tier provider did not list its rows ("
                        + (list.answered() ? "status " + list.status() : list.desc()) + ")");
                return incoming;
            }
            Map<String, Object> row = listing.row;
            if (row == null) {
                incoming.setParam("status","9");
                incoming.setParam("status_desc","refused: live row " + repoEngine.getLiveRow() + " is not held at this node;"
                        + " a new realise makes a new row, which this repository can then be configured with");
                incoming.setParam("refusal","not-held");
                return incoming;
            }
            if (!"copy".equals(String.valueOf(row.get("mode")))) {
                incoming.setParam("status","9");
                incoming.setParam("status_desc","refused: the node realises copies only; a workspace is kept by its holders");
                incoming.setParam("refusal","workspace");
                return incoming;
            }
            if (row.get("collection") == null || row.get("vid") == null) {
                incoming.setParam("status","9");
                incoming.setParam("status_desc","refused: the provider's row does not name what it holds");
                incoming.setParam("refusal","unnamed");
                return incoming;
            }
            Map<String, String> params = new LinkedHashMap<>();
            params.put("collection", String.valueOf(row.get("collection")));
            params.put("vid", String.valueOf(row.get("vid")));
            Object prefix = row.get("prefix");
            if (prefix != null && !String.valueOf(prefix).isEmpty()) params.put("prefix", String.valueOf(prefix));
            if (leaseMs != null) params.put("lease_ms", leaseMs);
            params.put("request_id", requestId);

            LiveTierClient.Answer answer = liveTier.realise(params);
            if (!answer.answered()) {
                incoming.setParam("status", LiveTierClient.REFUSED.equals(answer.failure) ? "9" : "11");
                incoming.setParam("status_desc","not sent: " + answer.desc());
                return incoming;
            }
            // the reply was sealed between the provider and the core: only its status and job id cross the fabric,
            // never its description, which may name what was realised
            incoming.setParam("reply_status", answer.status());
            Object job = answer.reply.get("job_id");
            if (job != null) incoming.setParam("job_id", String.valueOf(job));
            incoming.setParam("request_id", requestId);
            if (answer.ok()) {
                incoming.setParam("status","10");
                incoming.setParam("status_desc","realise sent");
            } else {
                incoming.setParam("status","9");
                incoming.setParam("status_desc","realise refused (status " + answer.status() + ")");
                incoming.setParam("refusal","realise");
            }
        } catch (Exception ex) {
            logger.error("liverealise error", ex);
            incoming.setParam("status","8");
            incoming.setParam("status_desc","liverealise error: " + ex.getClass().getSimpleName());
        }
        return incoming;
    }

    private MsgEvent removeFile(MsgEvent incoming) {

        try {
            MsgEvent other = refuseOtherRepo(incoming);
            if (other != null) return other;

            if(incoming.paramsContains("file_name")){

                String repoName = incoming.getParam("repo_name");
                String fileName = incoming.getParam("file_name");

                boolean isRemoved = repoEngine.removeFile(repoName, fileName);
                if(isRemoved) {
                    incoming.setParam("status","10");
                    incoming.setParam("status_desc","file removed");
                } else {
                    incoming.setParam("status","9");
                    incoming.setParam("status_desc","file not removed");
                }


            } else {
                logger.error("No file name found");
                incoming.setParam("status","9");
                incoming.setParam("status_desc","No repo or file name name found");
            }

        } catch(Exception ex){
            logger.error("removeFile: " + ex.getMessage());
            incoming.setParam("status","8");
            incoming.setParam("status_desc","remove error: " + ex.getMessage());
        }

        return incoming;
    }

    /**
     * putfiles (FR-D10): every file is verified against its expected content hash (file_hashes) before it lands, and the
     * reply says which were stored, unchanged (already held), skipped (held with other content, overwrite=false) or
     * failed. Only "every file stored or unchanged" is status 10.
     */
    private MsgEvent putFiles(MsgEvent incoming) {

        try {
            List<String> fileList = incoming.getFileList();

            boolean overwrite = false;

            if((fileList != null) && incoming.paramsContains("repo_name")){

                MsgEvent other = refuseOtherRepo(incoming);
                if (other != null) return other;

                // a landing from this agent copies the paths the caller names: none may lie in a live-tier root
                for (String f : fileList) {
                    String conflict = repoEngine.liveRootConflict(Paths.get(f));
                    if (conflict != null) {
                        return refuseLiveRoot(incoming, "a file to land " + conflict);
                    }
                }

                Map<String, String> expected = null;
                String hashesJson = incoming.getCompressedParam("file_hashes");
                if (hashesJson != null) {
                    try {
                        expected = gson.fromJson(hashesJson, stringMapType);
                    } catch (Exception bad) {
                        incoming.setParam("status","8");
                        incoming.setParam("status_desc","file_hashes is not a JSON object of file name to hash");
                        return incoming;
                    }
                }

                try{
                    if(incoming.getParam("overwrite") != null) {
                        overwrite = Boolean.parseBoolean(incoming.getParam("overwrite"));
                    }
                } catch(Exception ex){
                    logger.error("filerepo error", ex);
                }

                RepoEngine.Put put = repoEngine.putFiles(fileList, expected, overwrite, true);
                incoming.setCompressedParam("stored", gson.toJson(put.stored));
                incoming.setCompressedParam("unchanged", gson.toJson(put.unchanged));
                incoming.setCompressedParam("skipped", gson.toJson(put.skipped));
                incoming.setCompressedParam("failed", gson.toJson(put.failed));
                if (put.busy) {
                    incoming.setParam("status","9");
                    incoming.setParam("status_desc","repository busy: a clear holds it");
                    incoming.setParam("refusal","busy");
                } else if (put.allStored()) {
                    if (incoming.getParam("transfer_id") != null) {
                        MsgEvent filesConfirm = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC,incoming.getSrcRegion(),incoming.getSrcAgent(),incoming.getSrcPlugin());
                        filesConfirm.setParam("action", "repoconfirm");
                        filesConfirm.setParam("transfer_id", incoming.getParam("transfer_id"));
                        plugin.msgOut(filesConfirm);
                    }
                    incoming.setParam("status","10");
                    incoming.setParam("status_desc","putfiles stored: " + put.summary());
                } else {
                    logger.warn("putfiles for repo " + incoming.getParam("repo_name") + ": " + put.summary() + " " + put.failed);
                    incoming.setParam("status","9");
                    incoming.setParam("status_desc","putfiles incomplete: " + put.summary());
                }

            } else {
                logger.error("No repo name found");
                incoming.setParam("status","8");
                incoming.setParam("status_desc","no repo_name or file list");
            }

        } catch(Exception ex){
            logger.error("filerepo error", ex);
            incoming.setParam("status","7");
            incoming.setParam("status_desc","putfiles error: " + ex.getMessage());
        }

        if(incoming.getParams().containsKey("filedata")) {
            incoming.removeParam("filedata");
        }

        // return a real reply (was null) so an RPC caller learns the outcome instead of blocking
        // until its timeout; a fire-and-forget (msgOut) caller simply ignores the reply
        return incoming;
    }

    private MsgEvent repoListIn(MsgEvent incoming) {

        try {

            String repoListStringIn = incoming.getCompressedParam("repolistin");
            String deletesIn = incoming.getCompressedParam("repodeletes");
            String transferId = incoming.getParam("transfer_id");

            // the producer is who the verified caller says, not what the message's source fields claim: the files are
            // fetched from it
            String[] from = Callers.plugin(incoming, insecure);
            if (from == null) {
                incoming.setParam("status_code","9");
                incoming.setParam("status_desc","refused: a diff comes from a verified filerepo plugin caller");
            } else if((repoListStringIn != null) && (transferId != null)) {

                RepoEngine.Accepted accepted = repoEngine.acceptDiff(repoListStringIn, deletesIn, transferId,
                        incoming.getParam("sync_gen"), from[0], from[1], from[2]);
                incoming.setParam("status_code", accepted.code);
                incoming.setParam("status_desc", accepted.desc);

            } else {
                incoming.setParam("status_code","9");
                incoming.setParam("status_desc","repoListIn repoListStringIn | repoIn | transferId == NULL!");
                logger.error("repoListStringIn | repoIn == NULL");
            }

        } catch(Exception ex){
            incoming.setParam("status_code","9");
            incoming.setParam("status_desc","repoListIn exception " + ex.getMessage());
            logger.error("filerepo error", ex);
        }
        if(incoming.paramsContains("repolistin")) {
            incoming.removeParam("repolistin");
        }
        if(incoming.paramsContains("repodeletes")) {
            incoming.removeParam("repodeletes");
        }
        return incoming;
    }

}
