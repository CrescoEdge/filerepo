package io.cresco.filerepo;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.TextMessage;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.FileSystemLoopException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public class RepoEngine {

    private PluginBuilder plugin;
    private CLogger logger;
    private Gson gson;

    private Type repoListType;
    private final Type deleteListType = new TypeToken<List<DeleteObject>>() {}.getType();
    private final Type stringListType = new TypeToken<List<String>>() {}.getType();

    /** Files filerepo is still writing (putjar, putfiles, sync) are staged under this name and never scanned or served. */
    static final String STAGING_PREFIX = ".filerepo-";
    static final String STAGING_SUFFIX = ".part";
    /** The owner-only directory, inside the repository, where putjar and putfiles stage what they verify. */
    static final String PRIVATE_STAGING = ".filerepo-staging";
    /** The sync protocol this filerepo speaks: a producer and its subscribers must speak the same one. */
    static final String SYNC_PROTO = "2";

    /** What a producer signs of one offer (repolistin): the payloads by their SHA-256. */
    static String[] offerParts(String name, String producer, String mirror, String transferId, String gen, String filesJson,
                               String deletesJson) {
        return new String[]{"repolistin", name, producer, mirror, transferId, gen, ProducerKey.digest(filesJson),
                ProducerKey.digest(deletesJson)};
    }

    /** What a producer signs of its answer to a subscribe (reposubscribed). */
    static String[] answerParts(String name, String producer, String mirror, String nonce, String gen, String dir, String epoch) {
        return new String[]{"reposubscribed", name, producer, mirror, nonce, gen, dir, epoch};
    }

    // one scan at a time (the scan timer's own re-entry guard)
    private AtomicBoolean inScan = new AtomicBoolean(false);

    // The repository gate. clearrepo holds it exclusively from its only-copy check through its delete; a scan and every
    // landing (putfiles, putjar) hold it shared, so nothing filerepo lands can slip in between a clear's check and its
    // delete, while scans and landings still run side by side as they always have. Fair, so a waiting clear is not
    // starved by a stream of landings. Every claim is bounded and released in a finally.
    private final ReentrantReadWriteLock gate = new ReentrantReadWriteLock(true);
    // bounded wait for the gate (clear and landings); tests shorten it
    volatile long gateWaitMs = 30000L;
    // test hook: runs inside clearRepo between its check and its delete, with the gate held
    volatile Runnable afterClearCheck;
    // test hook: runs as an updater starts on a diff
    volatile Runnable beforeApplyDiff;
    // the device a path is on (mount tracking); null when it cannot be told. Tests replace it.
    volatile Function<Path, Object> devOf = RepoEngine::unixDev;
    // the clock subscriber retention reads (tests move it forward)
    volatile java.util.function.LongSupplier clock = System::currentTimeMillis;

    // Live-tier backing (config live_row): the repository is a live copy or a workspace held at this node, read only
    // through the filesystem. Read once, here; no verb can change it. A live-backed repository runs no scanner and no
    // sync, and ExecutorImpl refuses every verb that would serve, write, delete or sync it.
    private final String liveRow;
    // a subscriber mirror (filerepo_name and repo_dir, no scan_dir): the one role whose contents are held elsewhere
    private final boolean subscriber;
    // has filerepo landed files here directly (putfiles, putjar)? kept in the catalog; null until read
    static final String META_LANDED = "landed";
    private volatile Boolean landed;
    private final Object landedLock = new Object();

    // the live-tier client (may be null): nothing under a live-tier root it knows of is scanned, synced or deleted
    // here, and the roots it learns are remembered in the catalog, so they hold while the provider is away
    private final LiveTierClient liveTier;
    static final String META_LIVE_ROOTS = "live_roots";
    private final Object liveRootsLock = new Object();
    private volatile boolean warnedRepoDirConflict;

    // explicit insecure development mode (broker_security_enabled=false on the agent): an unstamped caller is anonymous
    private final boolean insecure;

    // ---- producer side (sync protocol 2) ----
    // this producer instance: transfer ids are <epoch>.<n>, so a confirm from before a restart never settles a transfer
    // of this instance
    private final String epoch = SyncReceiver.token().substring(0, 16);
    private final AtomicLong transferSeq = new AtomicLong();
    // transferId -> what that offer carried and to whom; consumed by confirmTransfer() (bounded: oldest evicted)
    private final Map<String, Offer> pendingTransferMap =
            Collections.synchronizedMap(new LinkedHashMap<String, Offer>() {
                protected boolean removeEldestEntry(Map.Entry<String, Offer> eldest) {
                    return size() > 4096;
                }
            });
    // subscriber|path (or subscriber|del|path) -> when it was offered: not offered again until confirmed, failed or timed out
    private final Map<String, Long> inFlight = new ConcurrentHashMap<>();
    // subscribers this producer knows (verified by their stamped caller on reposubscribe), with what each still needs
    // kept in the catalog (DBEngine PENDING, PENDINGDEL)
    private final Map<String, ActiveSub> subscribers = new ConcurrentHashMap<>();
    private final DeleteGuard guard;
    static final String META_MOUNTS = "mounts";
    static final String META_ROOT_DEV = "root_dev";
    private volatile String lastHoldLogged;
    // old-protocol peers warned about (bounded), and at most one such WARN a minute
    private final Map<String, Long> oldProtocolWarned = Collections.synchronizedMap(new LinkedHashMap<String, Long>() {
        protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
            return size() > 1024;
        }
    });

    static final int REFILL_BURST = 20;
    /** Names one refill request carries at most. */
    static final int REFILL_PAGE = 1000;

    /** A subscriber as the producer serves it. */
    static final class ActiveSub {
        final String key, region, agent, plugin;
        volatile String gen;
        volatile long lastSeen;
        // false after an offer went unanswered: skipped until it subscribes again
        volatile boolean reachable = true;
        // when it last confirmed a transfer (lag)
        volatile long lastConfirmed;
        // resets (a new generation: everything offered again) allowed: a few at once, then one per sync_reset_interval_ms
        double resetTokens = 3;
        long resetRefilled = System.currentTimeMillis();
        // refills (a mirror asking for files it lost): a burst, then one a second
        double refillTokens = REFILL_BURST;
        long refillRefilled = System.currentTimeMillis();

        synchronized boolean takeRefill(long now) {
            refillTokens = Math.min(REFILL_BURST, refillTokens + (now - refillRefilled) / 1000.0);
            refillRefilled = now;
            if (refillTokens < 1) return false;
            refillTokens -= 1;
            return true;
        }

        ActiveSub(String key, String region, String agent, String plugin, String gen, long lastSeen) {
            this.key = key;
            this.region = region;
            this.agent = agent;
            this.plugin = plugin;
            this.gen = gen;
            this.lastSeen = lastSeen;
            this.lastConfirmed = lastSeen;
        }

        synchronized boolean takeReset(long now, long intervalMs) {
            resetTokens = Math.min(3, resetTokens + (now - resetRefilled) / (double) intervalMs);
            resetRefilled = now;
            if (resetTokens < 1) return false;
            resetTokens -= 1;
            return true;
        }
    }

    // ---- subscriber side ----
    // this repository's sync generation: new when it starts and when a clear starts; a producer that has not served it
    // offers it everything again (paced), and it skips what it holds
    private volatile String syncGen;
    // producers an admin pinned in configuration (sync_producers: region/agent#fingerprint, comma-separated); null: not
    // configured (an admin pins with syncpin); empty: configured, but naming no producer key (nothing syncs)
    private final List<Pin> configuredProducers;
    // sync_role: producer | mirror | none, or "" (from the directories configured: scan_dir a producer, repo_dir a mirror)
    private final String syncRole;
    // the producer an admin pinned with syncpin (catalog meta), when no sync_producers is configured; null: none, and
    // this mirror syncs nothing. Never taken from whoever answers first.
    private volatile Identity adminPin;
    // a pin an earlier build recorded (by directory, possibly taken on first use): it pins nothing; kept only so an
    // admin's syncpin adopt=true can hand what that producer delivered to the producer it pins by key
    private volatile Identity legacyPin;
    static final String META_PRODUCER_PIN = "producer_pin";
    // the admin-provisioned secret of this repository name (sync_key), shared by its producer(s) and mirrors; null: none.
    // A mirror takes a producer only when it proves this key (or an admin pinned it): with neither, it syncs nothing.
    private final byte[] syncKey;
    // producers this repository subscribed to and that answered, verified (stamped caller, and sync_key or the pin), as
    // producers of its filerepo_name speaking this protocol: address -> its stable key and when last heard from
    private final Map<String, KnownProducer> knownProducers = new ConcurrentHashMap<>();
    // subscribe attempts: address -> when sent and the nonce the answer must carry (bounded); an answer is taken only
    // from an attempted producer. Limited per producer, so no flood of discovers for others delays one producer
    private final Map<String, Attempt> attempts = Collections.synchronizedMap(new LinkedHashMap<String, Attempt>() {
        protected boolean removeEldestEntry(Map.Entry<String, Attempt> eldest) {
            return size() > 1024;
        }
    });
    private volatile long lastHolderSweep;
    private volatile long syncOffWarned;
    private volatile long lastOldProtocolWarn;
    // a held rewrite set's id (a digest of the held paths and content), and its size
    private volatile String rewriteHoldId;
    private volatile long rewritesHeld;
    private volatile String lastRewriteHoldLogged;
    // a scan cycle waiting on offers ends at once when the plugin stops
    private volatile boolean stopping;
    // one name at a time: a sync commit or delete, and a landing (putfiles), never interleave on one name
    private final Object nameLock = new Object();
    // the rewrite guard's held set: changed by a scan, or released by an admin, one at a time
    private final Object rewriteLock = new Object();
    // subscribers dropped for lagging (bounded): told so when they come back
    private final Map<String, Long> droppedForLag = Collections.synchronizedMap(new LinkedHashMap<String, Long>() {
        protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
            return size() > 1024;
        }
    });
    private final Map<String, Long> lagAlarmed = new ConcurrentHashMap<>();
    // producers of this name that answered and are not this mirror's pinned producer (bounded): for repostate (the
    // candidates an admin may pin) and warnings; identity -> when last heard from
    private final Map<String, Long> unpinnedSeen = Collections.synchronizedMap(new LinkedHashMap<String, Long>() {
        protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
            return size() > 256;
        }
    });
    private final Map<String, Identity> unpinnedIds = Collections.synchronizedMap(new LinkedHashMap<String, Identity>() {
        protected boolean removeEldestEntry(Map.Entry<String, Identity> eldest) {
            return size() > 256;
        }
    });
    private final Map<String, Long> unpinnedWarned = Collections.synchronizedMap(new LinkedHashMap<String, Long>() {
        protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
            return size() > 256;
        }
    });
    final AtomicLong syncUnpinned = new AtomicLong();
    // this producer's stable key (catalog meta): mirrors record what it delivered under it, so a redeployed plugin
    // (a new plugin id over the same scan dir and catalog) is the same producer to them
    private volatile String producerKey;
    static final String META_PRODUCER_KEY = "producer_key";
    static final String META_SYNC_GEN = "sync_gen";

    /**
     * A producer's identity, as a mirror verifies it: the node it runs on, from the broker's stamp of its caller
     * ({@code <tenant>/<region>/<agent>}, unforgeable by any other node), the repository name, and the fingerprint of the
     * producer's own signing key ({@link ProducerKey}), proven by its signature over the mirror's fresh nonce: no other
     * plugin, on that node or any other, can claim it without the key. Not the plugin id (a producer redeployed over the
     * same directory keeps its key, and is the same producer), and not anything a holder of a shared secret could claim.
     * The directory it reports producing from is kept for display only.
     */
    static final class Identity {
        final String tenant, region, agent, repo;
        /** The producer key's fingerprint (64 hex), or null for a round-3 identity (by directory), which pins nothing. */
        final String key;
        /** The directory the producer reported (display only), or a round-3 identity's directory. */
        final String dir;

        Identity(String tenant, String region, String agent, String repo, String key, String dir) {
            this.tenant = tenant == null ? "-" : tenant;
            this.region = region;
            this.agent = agent;
            this.repo = repo;
            this.key = key;
            this.dir = dir;
        }

        String node() {
            return tenant + "/" + region + "/" + agent;
        }

        /** The whole identity, as a pin and a mirror's claims record it. */
        String full() {
            return node() + "|" + repo + "|" + (key != null ? "fp:" + key : dir);
        }

        /**
         * The key its claims are recorded under in a mirror: by its node, name and key (a mirror hears only producers of
         * its own tenant); a round-3 identity (by directory) keeps the key its claims were recorded under then, so an
         * admin can hand them on (syncpin adopt=true).
         */
        String holder() {
            if (key != null) return "k:" + DBEngine.sha256(region + "/" + agent + "|" + repo + "|" + key).substring(0, 40);
            return "n:" + DBEngine.sha256(full()).substring(0, 40);
        }

        /** As an admin pins it: {@code region/agent#fingerprint}. */
        String pin() {
            return region + "/" + agent + "#" + key;
        }

        static Identity parse(String full) {
            if (full == null) return null;
            String[] a = full.split("\\|", 3);
            if (a.length != 3) return null;
            String[] n = a[0].split("/", 3);
            if (n.length != 3) return null;
            if (a[2].startsWith("fp:")) return new Identity(n[0], n[1], n[2], a[1], a[2].substring(3), null);
            return new Identity(n[0], n[1], n[2], a[1], null, a[2]);
        }

        @Override
        public String toString() {
            return key != null ? pin() + (dir == null ? "" : " (" + dir + ")") : node() + ":" + dir + " (by directory)";
        }
    }

    /** A producer pin: a node ({@code region/agent}, of this tenant) and the fingerprint of its producer key. */
    static final class Pin {
        final String region, agent, key;

        Pin(String region, String agent, String key) {
            this.region = region;
            this.agent = agent;
            this.key = key;
        }

        static final java.util.regex.Pattern FINGERPRINT = java.util.regex.Pattern.compile("[0-9a-f]{64}");

        /** {@code region/agent#fingerprint}; null when malformed (see {@link #why}). */
        static Pin parse(String spec) {
            return why(spec) == null ? make(spec) : null;
        }

        /** Why {@code spec} pins nothing, or null when it is a pin. */
        static String why(String spec) {
            if (spec == null || spec.trim().isEmpty()) return "it is empty";
            spec = spec.trim();
            int hash = spec.indexOf('#');
            if (hash < 0) {
                return "it names no producer key: a pin is region/agent#<the producer's key fingerprint> (the producer's"
                        + " repostate shows producer_pin); a node or a directory is no identity: any plugin there could claim it";
            }
            String node = spec.substring(0, hash), fp = spec.substring(hash + 1).trim().toLowerCase(java.util.Locale.ROOT);
            if (!FINGERPRINT.matcher(fp).matches()) return "the fingerprint is not 64 hex digits (SHA-256 of the producer's public key)";
            String[] parts = node.trim().split("/");
            if (parts.length != 2 || parts[0].isEmpty() || parts[1].isEmpty() || node.indexOf(':') >= 0) {
                return "the node is not region/agent";
            }
            return null;
        }

        private static Pin make(String spec) {
            spec = spec.trim();
            int hash = spec.indexOf('#');
            String[] parts = spec.substring(0, hash).trim().split("/");
            return new Pin(parts[0], parts[1], spec.substring(hash + 1).trim().toLowerCase(java.util.Locale.ROOT));
        }

        boolean matches(Identity id) {
            return id != null && id.key != null && region.equals(id.region) && agent.equals(id.agent) && key.equals(id.key);
        }

        @Override
        public String toString() {
            return region + "/" + agent + "#" + key;
        }
    }

    /** A producer as a mirror knows it: who it is, whether it is this mirror's pinned producer, when last heard from. */
    static final class KnownProducer {
        final Identity id;
        final String plugin;
        final boolean pinned;
        // the key its offers are signed with; null only for a producer a test remembers by its address
        final java.security.PublicKey key;
        volatile long lastSeen;

        KnownProducer(Identity id, String plugin, boolean pinned, java.security.PublicKey key, long lastSeen) {
            this.id = id;
            this.plugin = plugin;
            this.pinned = pinned;
            this.key = key;
            this.lastSeen = lastSeen;
        }

        String holder() {
            return id.holder();
        }
    }

    /** A subscribe this mirror sent: when, and the nonce the producer's answer must echo. */
    static final class Attempt {
        final long at;
        volatile String nonce;

        Attempt(long at, String nonce) {
            this.at = at;
            this.nonce = nonce;
        }
    }

    private final Object lockPeerUpdateStateMap = new Object();
    private final Map<String, Boolean> peerUpdateStateMap = new HashMap<>();

    private final Object lockPeerUpdateQueueMap = new Object();
    // producer -> its diffs waiting for the updater (FR-D5: bounded per producer and in producers)
    private final Map<String, Deque<PendingDiff>> peerUpdateQueueMap = new HashMap<>();

    private Type mapType;

    // volatile: read by putFiles() on dispatch threads to decide inline-catalog vs scanner-owned
    private volatile Timer fileScanTimer;
    private Timer repoBroadcastTimer;

    private String scanDirString;

    private int transferId = -1;

    private  DBEngine dbEngine;

    private final List<String> listenerList = Collections.synchronizedList(new ArrayList<>());
    private String fileRepoName;
    private String repoDir;

    // ---- limits and tunables (config) ----
    private final int scanBatchSize;
    private final int diffPageSize;
    private final int deletePageSize;
    private final int syncPendingPerPeer;
    private final int syncMaxPeers;
    private final long offerTimeoutMs;
    private final long tombstoneTtlMs;
    private final boolean followLinks;
    private final boolean requirePutHashes;
    private final int syncStreamBuffer;
    private final long syncStreamIdleMs;
    private final int syncStreamRounds;
    private final long syncRpcTimeoutMs;
    private final int pagesPerCycle;
    private final long subscribeRetryMs;
    private final long peerTtlMs;
    private final long subscriberTtlMs;
    private final long lagAlarmMs;
    private final long holderTtlMs;
    private final long resetIntervalMs;
    private final int refillBatch;
    private final double rewriteFraction;
    private final int rewriteMin;
    private final double rewriteDayFraction;
    private final double rewriteTotalFraction;
    // what the guards let through (deletes recorded, rewrites offered), kept in the catalog on the guards' own clock
    private final GuardLedger ledger;
    // the budgets that hold the rewrites held now (consumed by their release), kept in the catalog
    static final String META_REWRITE_HOLD_BUDGETS = "rewrite_hold_budgets";

    // Bounded, named, daemon pool for peer-sync updaters: one per producer at a time, at most sync_threads at once,
    // and at most sync_queue_max waiting (FR-D5). A diff that finds both full is refused as busy (status 11) and the
    // producer offers it again next cycle: back-pressure instead of an unbounded backlog.
    private final ThreadPoolExecutor syncPool;
    // Offers go out to subscribers concurrently, at most sync_offer_threads at once: a cycle costs the slowest
    // subscriber's answer, not the sum of all of them, so subscribers that went away (each costs one unanswered RPC,
    // then is skipped until it subscribes again) never stall the scan or the others for more than one RPC timeout.
    private final ThreadPoolExecutor offerPool;

    // ---- sync metrics ----
    final AtomicLong syncRejected = new AtomicLong();
    final AtomicLong syncRefused = new AtomicLong();
    final AtomicLong syncFetched = new AtomicLong();
    final AtomicLong syncFetchFailed = new AtomicLong();
    final AtomicLong syncVerifyFailed = new AtomicLong();
    final AtomicLong syncDeleted = new AtomicLong();
    final AtomicLong syncConflicts = new AtomicLong();
    final AtomicLong syncUnverified = new AtomicLong();
    final AtomicLong subscribersDropped = new AtomicLong();
    final AtomicLong tombstonesRecorded = new AtomicLong();

    public RepoEngine(PluginBuilder pluginBuilder, DBEngine dbEngine) {
        this(pluginBuilder, dbEngine, null);
    }

    public RepoEngine(PluginBuilder pluginBuilder, DBEngine dbEngine, LiveTierClient liveTier) {

        this.plugin = pluginBuilder;
        logger = plugin.getLogger(RepoEngine.class.getName(), CLogger.Level.Info);
        insecure = Callers.insecureMode(plugin);
        this.dbEngine = dbEngine;
        this.liveTier = liveTier;
        this.ledger = new GuardLedger(dbEngine, logger);
        gson = new Gson();

        this.repoListType = new TypeToken<Map<String,FileObject>>() {}.getType();

        this.mapType = new TypeToken<Map<String,String>>() {}.getType();

        scanDirString =  plugin.getConfig().getStringParam("scan_dir");
        fileRepoName =  plugin.getConfig().getStringParam("filerepo_name");
        repoDir =  plugin.getConfig().getStringParam("repo_dir");

        String row = plugin.getConfig().getStringParam("live_row");
        liveRow = (row == null || row.trim().isEmpty()) ? null : row.trim();
        // the same test start() applies to choose the subscriber mode
        subscriber = (liveRow == null) && (scanDirString == null) && (fileRepoName != null) && (repoDir != null);

        scanBatchSize = (int) clamp(longParam("scan_batch_size", 500L), 1, 100_000);
        diffPageSize = (int) clamp(longParam("sync_diff_page", 1000L), 1, 100_000);
        deletePageSize = (int) clamp(longParam("sync_delete_page", 1000L), 1, 100_000);
        syncPendingPerPeer = (int) clamp(longParam("sync_pending_per_peer", 4L), 1, 1024);
        syncMaxPeers = (int) clamp(longParam("sync_max_peers", 256L), 1, 100_000);
        offerTimeoutMs = clamp(longParam("sync_offer_timeout_ms", 600_000L), 1_000L, Long.MAX_VALUE);
        tombstoneTtlMs = clamp(longParam("sync_tombstone_ttl_ms", 7L * 24 * 3600 * 1000), 0L, Long.MAX_VALUE);
        guard = new DeleteGuard(fractionParam("sync_delete_guard_fraction", 0.10),
                (int) clamp(longParam("sync_delete_guard_min", 3L), 1, Integer.MAX_VALUE),
                (int) clamp(longParam("sync_delete_guard_subtree_min", 10L), 1, Integer.MAX_VALUE),
                fractionParam("sync_delete_guard_subtree_fraction", 0.01),
                clamp(longParam("sync_release_ttl_ms", 600_000L), 1_000L, Long.MAX_VALUE),
                clamp(longParam("sync_delete_guard_window_ms", 900_000L), 0L, Long.MAX_VALUE));
        guard.setBudgets(fractionOrZero("sync_delete_guard_day_fraction", 0.25),
                fractionOrZero("sync_delete_guard_total_fraction", 0.5));
        rewriteFraction = fractionParam("sync_rewrite_guard_fraction", 0.25);
        // a day allows at least what one cycle does, and the total at least what a day does (0 turns one off)
        double rd = fractionOrZero("sync_rewrite_guard_day_fraction", 0.25);
        rewriteDayFraction = rd <= 0 ? 0 : Math.max(rd, rewriteFraction);
        double rt = fractionOrZero("sync_rewrite_guard_total_fraction", 0.5);
        rewriteTotalFraction = rt <= 0 ? 0 : Math.max(rt, Math.max(rewriteDayFraction, rewriteFraction));
        rewriteMin = (int) clamp(longParam("sync_rewrite_guard_min", 10L), 1, Integer.MAX_VALUE);
        followLinks = plugin.getConfig().getBooleanParam("scan_follow_links", false);
        requirePutHashes = plugin.getConfig().getBooleanParam("putfiles_require_hashes", true);
        syncStreamBuffer = (int) clamp(longParam("sync_stream_buffer", 262144L), 4096, 786432);
        syncStreamIdleMs = clamp(longParam("sync_stream_idle_ms", 30_000L), 100L, 3_600_000L);
        syncStreamRounds = (int) clamp(longParam("sync_stream_rounds", 3L), 1, 16);
        syncRpcTimeoutMs = clamp(longParam("sync_rpc_timeout_ms", 30_000L), 100L, 600_000L);
        // sync_full_pages_per_cycle is the older name of the same pacing
        pagesPerCycle = (int) clamp(longParam("sync_pages_per_cycle", longParam("sync_full_pages_per_cycle", 8L)), 1, 10_000);
        subscribeRetryMs = clamp(longParam("sync_subscribe_retry_ms", 10_000L), 100L, 3_600_000L);
        peerTtlMs = clamp(longParam("sync_peer_ttl_ms", 3_600_000L), 10_000L, Long.MAX_VALUE);
        subscriberTtlMs = clamp(longParam("sync_subscriber_ttl_ms", 7L * 24 * 3600 * 1000), 60_000L, Long.MAX_VALUE);
        lagAlarmMs = clamp(longParam("sync_lag_alarm_ms", 24L * 3600 * 1000), 60_000L, Long.MAX_VALUE);
        holderTtlMs = clamp(longParam("sync_holder_ttl_ms", 30L * 24 * 3600 * 1000), 60_000L, Long.MAX_VALUE);
        resetIntervalMs = clamp(longParam("sync_reset_interval_ms", 600_000L), 1_000L, Long.MAX_VALUE);
        refillBatch = (int) clamp(longParam("sync_refill_check_batch", 10_000L), 1, 1_000_000);

        String key = plugin.getConfig().getStringParam("sync_key");
        if (key != null && !key.isEmpty() && key.length() < 16) {
            logger.error("sync_key is shorter than 16 characters: ignored (this repository syncs with no producer by key)");
            key = null;
        }
        syncKey = (key == null || key.isEmpty()) ? null : key.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        String pinned = plugin.getConfig().getStringParam("sync_producers");
        if (pinned != null && !pinned.trim().isEmpty()) {
            List<Pin> pins = new ArrayList<>();
            for (String p : pinned.split(",")) {
                if (p.trim().isEmpty()) continue;
                String why = Pin.why(p);
                if (why != null) {
                    logger.error("sync_producers: '" + p.trim() + "' pins nothing: " + why);
                } else {
                    pins.add(Pin.parse(p));
                }
            }
            if (pins.isEmpty()) {
                logger.error("sync_producers names no producer key: this mirror syncs nothing until it does");
            }
            configuredProducers = Collections.unmodifiableList(pins);
        } else {
            configuredProducers = null;
        }
        String role = plugin.getConfig().getStringParam("sync_role");
        role = role == null ? "" : role.trim().toLowerCase(java.util.Locale.ROOT);
        if (!role.isEmpty() && !Set.of("producer", "mirror", "none").contains(role)) {
            logger.error("sync_role '" + role + "' is not producer, mirror or none: this repository syncs nothing");
            role = "none";
        }
        syncRole = role;
        loadSigningKey();

        int syncThreads = (int) clamp(longParam("sync_threads", 8L), 1, 256);
        int syncQueue = (int) clamp(longParam("sync_queue_max", 64L), 1, 100_000);
        syncPool = new ThreadPoolExecutor(syncThreads, syncThreads, 60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(syncQueue),
                r -> { Thread t = new Thread(r, "filerepo-sync"); t.setDaemon(true); return t; },
                new ThreadPoolExecutor.AbortPolicy());
        // let idle sync threads retire (core==max would otherwise keep them alive forever)
        syncPool.allowCoreThreadTimeOut(true);
        int offerThreads = (int) clamp(longParam("sync_offer_threads", 8L), 1, 256);
        // the queue is bounded by the subscribers (at most sync_max_peers): a cycle waits for every offer it started
        offerPool = new ThreadPoolExecutor(offerThreads, offerThreads, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                r -> { Thread t = new Thread(r, "filerepo-offer"); t.setDaemon(true); return t; });
        offerPool.allowCoreThreadTimeOut(true);

        if (liveTier != null) {
            List<String> known = new ArrayList<>();
            // roots the operator names (comma-separated): refused from the start, before any provider has registered
            String configured = plugin.getConfig().getStringParam("live_roots");
            if (configured != null) known.addAll(Arrays.asList(configured.split(",")));
            try {
                String v = dbEngine.getMeta(META_LIVE_ROOTS);
                if (v != null) known.addAll(Arrays.asList(v.split("\n")));
            } catch (Exception ex) {
                logger.error("remembered live-tier roots unreadable: " + ex.getMessage());
            }
            liveTier.rememberRoots(known, this::rememberLiveRoot);
        }

        if (liveRow == null && scanDirString != null && fileRepoName != null) {
            // a producer keeps serving the subscribers it knew before a restart, until they go quiet
            for (DBEngine.Subscriber s : dbEngine.getSubscribers()) {
                subscribers.put(s.key, new ActiveSub(s.key, s.region, s.agent, s.plugin, s.gen, s.lastSeen));
            }
            refreshRewriteHold();
        }
        // this repository's sync generation survives a restart: a plain restart resumes where it left off
        syncGen = loadGen();
        try {
            Identity recorded = Identity.parse(dbEngine.getMeta(META_PRODUCER_PIN));
            if (recorded != null && recorded.key == null) {
                // round 3 pinned by directory, and pinned whoever answered first: not an identity, and not an admin's choice
                legacyPin = recorded;
                if (isMirror()) dbEngine.nameHolder(recorded.holder(), recorded.toString());
            } else {
                adminPin = recorded;
            }
        } catch (Exception ex) {
            adminPin = null;
        }

        if (liveRow == null && scanDirString == null && repoDir == null) {
            migrateDefaultRepository();
            // FR-D9: the old default was the relative path "filerepo", wherever the JVM happened to start
            File legacy = new File("filerepo").getAbsoluteFile();
            if (legacy.isDirectory()) {
                logger.warn("repo_dir is not set: the repository is now " + getRepoDir().getAbsolutePath()
                        + " (it was the relative path 'filerepo'); files landed before are still in "
                        + legacy.getAbsolutePath() + " and are not served until moved");
            }
        } else if (liveRow == null) {
            String configuredDir = scanDirString != null ? scanDirString : repoDir;
            if (configuredDir != null && !new File(configuredDir).isAbsolute()) {
                logger.warn("the repository directory '" + configuredDir + "' is relative: it depends on the agent's"
                        + " working directory; configure an absolute path");
            }
        }
    }

    /** A fraction in [0, 1]: 0 turns that budget off. */
    private double fractionOrZero(String name, double dflt) {
        try {
            Double d = plugin.getConfig().getDoubleParam(name);
            if (d != null && d >= 0 && d <= 1) return d;
        } catch (Exception ignore) { }
        return dflt;
    }

    private double fractionParam(String name, double dflt) {
        try {
            Double d = plugin.getConfig().getDoubleParam(name);
            if (d != null && d > 0 && d <= 1) return d;
        } catch (Exception ignore) { }
        return dflt;
    }

    /** The sync generation from the catalog, or a new one (recorded). */
    private String loadGen() {
        try {
            String g = dbEngine.getMeta(META_SYNC_GEN);
            if (g != null && g.matches("[0-9a-f]{1,64}")) return g;
        } catch (Exception ex) {
            logger.error("sync generation unreadable: " + ex.getMessage());
        }
        return newGen();
    }

    /** A new sync generation, recorded in the catalog before it is used. */
    private String newGen() {
        String g = SyncReceiver.token();
        try {
            dbEngine.setMeta(META_SYNC_GEN, g);
        } catch (Exception ex) {
            logger.error("sync generation not recorded (a restart offers everything again): " + ex.getMessage());
        }
        return g;
    }

    // ---- a mirror's generation, kept with its files too ----
    static final String GEN_MARKER = "generation";
    private volatile long lastMarkerCheck;

    /** Record this mirror's generation inside its directory (the owner-only staging directory, never scanned). */
    void writeGenerationMarker() {
        if (!isMirror()) return;
        try {
            Path m = privateStaging().resolve(GEN_MARKER);
            Path tmp = m.resolveSibling(GEN_MARKER + ".tmp");
            Files.write(tmp, syncGen.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
            Files.move(tmp, m, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception ex) {
            logger.error("the generation marker could not be written in " + getRepoDir() + ": " + ex.getMessage());
        }
    }

    /**
     * A mirror whose directory no longer matches its catalog starts a new generation, so its producer offers it
     * everything again: the marker of its generation inside the directory is missing (a new, replaced or reformatted
     * volume) or names another generation, or the directory is empty though the catalog records what producers
     * delivered (an rm -rf instead of clearrepo). Nothing is deleted. At start, and at most once a minute after.
     */
    void checkGenerationMarker(boolean force) {
        if (!isMirror()) return;
        long now = System.currentTimeMillis();
        if (!force && now - lastMarkerCheck < 60_000L) return;
        lastMarkerCheck = now;
        String why = null;
        try {
            Path m = privateStaging().resolve(GEN_MARKER);
            String onDisk = Files.isRegularFile(m, LinkOption.NOFOLLOW_LINKS)
                    ? new String(Files.readAllBytes(m), java.nio.charset.StandardCharsets.US_ASCII).trim() : null;
            if (onDisk == null) why = "it has no generation marker (a new, replaced or reformatted directory)";
            else if (!onDisk.equals(syncGen)) why = "its generation marker names another generation";
            else if (dbEngine.mirroredCount() > 0 && !hasAnyFile(getRepoDir().toPath())) {
                why = "it is empty, though the catalog records files its producer delivered";
            }
        } catch (IOException ex) {
            logger.error("the generation marker of " + getRepoDir() + " could not be read: " + ex.getMessage());
            return;
        }
        if (why == null) return;
        String before = syncGen;
        syncGen = newGen();
        attempts.clear();
        writeGenerationMarker();
        logger.warn("mirror " + getRepoDir() + ": " + why + ": a new generation " + syncGen + " (was " + before
                + "), so its producer offers it everything again; nothing is deleted");
    }

    // ---- a mirror's refill: what its pinned producer delivered here, gone from the directory, is asked for again ----
    private volatile Timer mirrorCheckTimer;
    // per pinned producer (holder): where the check of its claims goes on from (each run checks a page)
    private final Map<String, String> refillCursor = new ConcurrentHashMap<>();
    // per pinned producer (holder): names found missing and not yet asked for (bounded)
    private final Map<String, Set<String>> refillWanted = new ConcurrentHashMap<>();
    // holder|name -> when last asked for: a name is asked for again at most once per sync_offer_timeout_ms (bounded)
    private final Map<String, Long> refillAsked = Collections.synchronizedMap(new LinkedHashMap<String, Long>() {
        protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
            return size() > 100_000;
        }
    });
    final AtomicLong syncRefillAsked = new AtomicLong();
    final AtomicLong syncRefillOffered = new AtomicLong();

    /**
     * A mirror's scan: the files its pinned producer(s) delivered here (the catalog's claims) that are no longer in the
     * directory (a partial loss: files deleted by hand, a failed disk under one subtree, find -type f -delete) are asked
     * for again from that producer, which offers them again to this mirror alone; they are fetched and verified as any
     * offer. A missing file is never a delete (a mirror produces nothing). Each run checks a page of the claims
     * (sync_refill_check_batch), going on from where the last stopped, so its cost is bounded whatever the size.
     */
    void checkMirroredFiles() {
        if (!isMirror()) return;
        Path repoBase = getRepoDir().toPath().toAbsolutePath().normalize();
        Path realBase;
        try {
            realBase = repoBase.toRealPath();
        } catch (IOException ex) {
            return;
        }
        for (String holder : pinnedHolders()) {
            String cursor = refillCursor.getOrDefault(holder, "");
            List<String[]> page = dbEngine.claimsPage(holder, cursor, refillBatch);
            refillCursor.put(holder, page.size() < refillBatch ? "" : page.get(page.size() - 1)[0]);
            int missing = 0;
            Set<String> wanted = refillWanted.computeIfAbsent(holder, h -> ConcurrentHashMap.newKeySet());
            for (String[] c : page) {
                Target t = target(repoBase, realBase, c[1]);
                if (t.why != null || Files.exists(t.path, LinkOption.NOFOLLOW_LINKS)) continue;
                missing++;
                if (wanted.size() < 100_000) wanted.add(c[1]);
            }
            if (missing > 0) {
                logger.warn("mirror " + getRepoDir() + ": " + missing + " file(s) its producer delivered are gone from the"
                        + " directory: asking the producer to offer them again (nothing is deleted anywhere)");
            }
        }
        requestRefills();
    }

    /** Ask each pinned producer that is known now for the files found missing (pages of REFILL_PAGE names). */
    private void requestRefills() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Set<String>> e : refillWanted.entrySet()) {
            String holder = e.getKey();
            Set<String> wanted = e.getValue();
            if (wanted.isEmpty()) continue;
            String region = null, agent = null, pluginId = null;
            for (Map.Entry<String, KnownProducer> k : knownProducers.entrySet()) {
                KnownProducer kp = k.getValue();
                if (kp.pinned && kp.holder().equals(holder) && now - kp.lastSeen <= peerTtlMs) {
                    region = kp.id.region;
                    agent = kp.id.agent;
                    pluginId = kp.plugin;
                }
            }
            if (pluginId == null) continue;   // asked once it answers
            List<String> names = new ArrayList<>();
            for (String n : new ArrayList<>(wanted)) {
                wanted.remove(n);
                Long asked = refillAsked.get(holder + "|" + n);
                if (asked != null && now - asked < offerTimeoutMs) continue;
                refillAsked.put(holder + "|" + n, now);
                names.add(n);
            }
            for (int from = 0; from < names.size(); from += REFILL_PAGE) {
                List<String> chunk = names.subList(from, Math.min(names.size(), from + REFILL_PAGE));
                MsgEvent ask = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, region, agent, pluginId);
                ask.setParam("action", "reporefill");
                ask.setParam("filerepo_name", fileRepoName);
                ask.setParam("sync_proto", SYNC_PROTO);
                ask.setParam("sync_gen", syncGen);
                ask.setCompressedParam("names", gson.toJson(chunk));
                plugin.msgOut(ask);
                syncRefillAsked.addAndGet(chunk.size());
            }
            if (!names.isEmpty()) {
                logger.info("asked producer " + SyncAuth.addr(region, agent, pluginId) + " for " + names.size()
                        + " file(s) gone from this mirror");
            }
        }
    }

    /**
     * reporefill (a producer's side): a subscriber of this generation asks for files it lost. The names that are
     * cataloged here become pending for that subscriber alone, and are offered (signed) in the next cycle. Bounded: at
     * most REFILL_PAGE names a request, a burst of requests then one a second per subscriber. Never changes what other
     * subscribers are offered, and never a delete.
     */
    MsgEvent onRefill(MsgEvent m) {
        String[] from = Callers.plugin(m, insecure);
        String refusal = null;
        ActiveSub s = from == null ? null : subscribers.get(peerKey(from[0], from[1], from[2]));
        List<String> names = null;
        if (from == null) refusal = "no verified plugin caller";
        else if (!isProducer()) refusal = "not a producer";
        else if (!String.valueOf(fileRepoName).equals(m.getParam("filerepo_name"))) refusal = "a producer of another filerepo_name";
        else if (s == null) refusal = "not a subscriber of this producer";
        else if (!String.valueOf(s.gen).equals(m.getParam("sync_gen"))) refusal = "another generation: subscribe again";
        else if (!s.takeRefill(System.currentTimeMillis())) refusal = "busy: too many refill requests, ask again later";
        else {
            try {
                names = gson.fromJson(m.getCompressedParam("names"), stringListType);
            } catch (Exception bad) {
                names = null;
            }
            if (names == null || names.size() > REFILL_PAGE) refusal = "names: a JSON list of at most " + REFILL_PAGE;
        }
        if (m.getParams().containsKey("names")) m.removeParam("names");
        if (refusal != null) {
            m.setParam("status", "9");
            m.setParam("status_code", "9");
            m.setParam("status_desc", "refused: " + refusal);
            logger.info("reporefill from " + (from == null ? "?" : SyncAuth.addr(from[0], from[1], from[2])) + " refused: " + refusal);
            return m;
        }
        Path root = Paths.get(scanDirString).toAbsolutePath();
        List<String> paths = new ArrayList<>();
        for (String name : names) {
            if (name == null) continue;
            Path p = root.resolve(name).normalize();
            if (!p.startsWith(root) || p.equals(root)) continue;
            String path = p.toString();
            if (dbEngine.getFileInfo(path) == null || offerable(root, path) != null) continue;
            paths.add(path);
        }
        int n = dbEngine.markPendingFor(s.key, paths);
        for (String path : paths) inFlight.remove(s.key + "|" + path);
        s.reachable = true;
        syncRefillOffered.addAndGet(n);
        logger.info("subscriber " + s.key + " lost " + names.size() + " file(s): " + n + " offered to it again");
        m.setParam("status", "10");
        m.setParam("status_code", "10");
        m.setParam("refill", String.valueOf(n));
        return m;
    }

    private static boolean hasAnyFile(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) return false;
        try (java.util.stream.Stream<Path> st = Files.list(dir)) {
            return st.anyMatch(p -> !isStaging(p.getFileName().toString()));
        }
    }

    /** The real path of this producer's scan dir (its identity, with its node, to mirrors). */
    String producerDir() {
        try {
            return Paths.get(scanDirString).toRealPath().toString();
        } catch (Exception ex) {
            return Paths.get(scanDirString).toAbsolutePath().normalize().toString();
        }
    }

    // this producer's signing key (its identity to mirrors, with its node), from its state directory; null when this is
    // not a producer, its catalog is not open (another plugin serves the directory), or the key could not be read
    private volatile ProducerKey signingKey;
    private volatile String signingKeyError;

    /**
     * Load this producer's signing key from the state directory, or make it there once. Only while this plugin holds the
     * directory's lock (its catalog is open): a second plugin over the same directory never reads or makes it. A key that
     * cannot be read is never replaced (the producer's identity is that key): the producer serves no mirror until an
     * admin restores or removes it.
     */
    private void loadSigningKey() {
        if (!isProducer()) return;
        if (dbEngine.openError() != null || dbEngine.stateDir() == null) {
            signingKeyError = "the catalog is not open";
            return;
        }
        try {
            ProducerKey.Loaded l = ProducerKey.loadOrCreate(dbEngine.stateDir());
            signingKey = l.key;
            Path file = dbEngine.stateDir().resolve(ProducerKey.FILE);
            if (l.created) {
                logger.warn("filerepo " + fileRepoName + ": made this producer's signing key " + file + " (owner-only). Pin it"
                        + " on each mirror: sync_producers=" + plugin.getRegion() + "/" + plugin.getAgent() + "#" + l.key.fingerprint
                        + " (or syncpin producer=...). Back it up with the state directory: a producer that loses it is a new"
                        + " producer to its mirrors");
            } else {
                logger.info("filerepo " + fileRepoName + ": producer key " + l.key.fingerprint + " (" + file + ")");
            }
            if (l.tightened) {
                logger.error("filerepo " + fileRepoName + ": the producer key " + file + " was readable by others: made"
                        + " owner-only. If it may have been copied, replace it (remove the file, restart, re-pin every mirror)");
            }
        } catch (Exception ex) {
            signingKeyError = ex.getMessage();
            logger.error("filerepo " + fileRepoName + ": the producer key in " + dbEngine.stateDir() + " cannot be used ("
                    + ex.getMessage() + "): this producer serves no mirror until an admin restores it (or removes it, and"
                    + " re-pins every mirror to the new key)");
        }
    }

    /** This producer's key fingerprint, or null. */
    String signingFingerprint() {
        ProducerKey k = signingKey;
        return k == null ? null : k.fingerprint;
    }

    /** How a mirror pins this producer ({@code region/agent#fingerprint}), or null. */
    String producerPin() {
        ProducerKey k = signingKey;
        return k == null ? null : plugin.getRegion() + "/" + plugin.getAgent() + "#" + k.fingerprint;
    }

    /**
     * A key kept in the catalog (round 2's producer identity). It is no longer anyone's identity: mirrors know a
     * producer by its stamped node and its signing key. Kept for diagnostics.
     */
    String producerKey() {
        String k = producerKey;
        if (k != null) return k;
        synchronized (this) {
            if (producerKey != null) return producerKey;
            try {
                k = dbEngine.getMeta(META_PRODUCER_KEY);
                if (k == null || !k.matches("[0-9a-f]{32}")) {
                    k = SyncReceiver.token();
                    dbEngine.setMeta(META_PRODUCER_KEY, k);
                }
            } catch (Exception ex) {
                logger.error("producer key unreadable: " + ex.getMessage());
                return null;
            }
            producerKey = k;
            return k;
        }
    }

    /**
     * The default repository used to be {@code <plugin data dir>/repo}, which the controller deletes with the plugin:
     * move it to the stable default once (a rename on the same filesystem; never a copy, never a delete) and rewrite
     * the catalog's paths. Journaled in the state directory before the rename, so a crash between the rename and the
     * rewrite is completed at the next start. If both directories hold files, or they are on different filesystems,
     * nothing is moved and an ERROR names both.
     */
    private void migrateDefaultRepository() {
        Path journal = dbEngine.stateDir() == null ? null : dbEngine.stateDir().resolve("migration.journal");
        Path legacy = RepoPaths.legacyDefaultRepo(plugin);
        Path target = repoDirFile().toPath();
        try {
            if (journal != null && Files.isRegularFile(journal, LinkOption.NOFOLLOW_LINKS)) {
                // an interrupted migration: finish it, or start it again
                java.util.Properties j = new java.util.Properties();
                try (java.io.InputStream in = Files.newInputStream(journal)) {
                    j.load(in);
                }
                Path from = Paths.get(j.getProperty("from")), to = Paths.get(j.getProperty("to"));
                if (!Files.exists(from, LinkOption.NOFOLLOW_LINKS) && Files.isDirectory(to, LinkOption.NOFOLLOW_LINKS)) {
                    int rows = dbEngine.rebasePaths(from.toString(), to.toString());
                    Files.delete(journal);
                    logger.warn("the interrupted move of the default repository from " + from + " to " + to
                            + " was completed: " + rows + " catalog row(s) updated");
                    return;
                }
                if (Files.isDirectory(from, LinkOption.NOFOLLOW_LINKS) && Files.exists(to, LinkOption.NOFOLLOW_LINKS)
                        && hasAnyFile(to)) {
                    logger.error("an interrupted move of the default repository left files in both " + from + " and " + to
                            + ": nothing is moved; an admin merges them by hand and removes " + journal);
                    return;
                }
                Files.delete(journal);   // the rename never happened: start over below
            }
            if (legacy.equals(target) || !Files.isDirectory(legacy, LinkOption.NOFOLLOW_LINKS)) return;
            if (Files.exists(target, LinkOption.NOFOLLOW_LINKS) && hasAnyFile(target)) {
                logger.error("the old default repository " + legacy + " and the new " + target + " both hold files: the"
                        + " old one is not served; move its files into the new one by hand");
                return;
            }
            Files.createDirectories(target.getParent());
            if (Files.isDirectory(target, LinkOption.NOFOLLOW_LINKS)) {
                Path staging = target.resolve(PRIVATE_STAGING);
                if (Files.isDirectory(staging, LinkOption.NOFOLLOW_LINKS)) {
                    try (java.util.stream.Stream<Path> st = Files.list(staging)) {
                        for (Path f : (Iterable<Path>) st::iterator) Files.delete(f);   // our own empty-repo staging
                    }
                    Files.delete(staging);
                }
                Files.delete(target);   // empty
            }
            if (journal != null) {
                java.util.Properties j = new java.util.Properties();
                j.setProperty("from", legacy.toString());
                j.setProperty("to", target.toString());
                Path tmp = journal.resolveSibling("migration.journal.tmp");
                try (java.io.OutputStream out = Files.newOutputStream(tmp)) {
                    j.store(out, "filerepo: moving the default repository");
                }
                Files.move(tmp, journal, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            }
            try {
                Files.move(legacy, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.AtomicMoveNotSupportedException otherDevice) {
                if (journal != null) Files.deleteIfExists(journal);
                logger.error("the old default repository " + legacy + " is on another filesystem than " + target
                        + ": it is not copied (a partial copy is never adopted); move its files by hand");
                return;
            }
            int rows = dbEngine.rebasePaths(legacy.toString(), target.toString());
            if (journal != null) Files.deleteIfExists(journal);
            logger.warn("the default repository moved from " + legacy + " (the plugin's data directory, deleted with the"
                    + " plugin) to " + target + "; " + rows + " catalog row(s) updated");
        } catch (Exception ex) {
            logger.error("the default repository " + legacy + " could not be moved to " + target + ": " + ex.getMessage()
                    + "; its files are not served until moved by hand (nothing was deleted)");
        }
    }

    private long longParam(String name, long dflt) {
        try {
            Long v = plugin.getConfig().getLongParam(name, dflt);
            return v == null ? dflt : v;
        } catch (Exception ex) {
            return dflt;
        }
    }

    private static long clamp(long v, long min, long max) {
        return Math.max(min, Math.min(max, v));
    }

    private static Object unixDev(Path p) {
        try {
            return Files.getAttribute(p, "unix:dev", LinkOption.NOFOLLOW_LINKS);
        } catch (Exception ex) {
            return null;
        }
    }

    /** Keep a live-tier root in the catalog, so it is refused even while no provider is registered. */
    private void rememberLiveRoot(String root) {
        synchronized (liveRootsLock) {
            try {
                Set<String> all = new LinkedHashSet<>();
                String v = dbEngine.getMeta(META_LIVE_ROOTS);
                if (v != null) all.addAll(Arrays.asList(v.split("\n")));
                if (all.add(root)) dbEngine.setMeta(META_LIVE_ROOTS, String.join("\n", all));
            } catch (Exception ex) {
                logger.error("could not remember a live-tier root in the catalog: " + ex.getMessage());
            }
        }
    }

    /** Why {@code p} may not be touched through filerepo (it lies in or contains a live-tier root), or null. */
    String liveRootConflict(Path p) {
        return liveTier == null ? null : liveTier.liveRootConflict(p);
    }

    /**
     * Why this repository's own directories (scan_dir, repo_dir) may not be used, or null: they lie in or contain a
     * live-tier root. Such a repository is refused like a live-backed one and scans nothing.
     */
    String repoDirConflict() {
        if (liveTier == null) return null;
        for (String dir : new String[]{scanDirString, repoDirFile().getPath()}) {
            if (dir == null) continue;
            String why = liveTier.liveRootConflict(Paths.get(dir));
            if (why != null) return why;
        }
        return null;
    }

    /** True when the repository is backed by a live-tier row (config live_row). */
    public boolean isLiveBacked() {
        return liveRow != null;
    }

    /** The live-tier row this repository is backed by, or null. */
    public String getLiveRow() {
        return liveRow;
    }

    /** This repository's name (filerepo_name), or null. */
    String getRepoName() {
        return fileRepoName;
    }

    /**
     * True when this repository is a producer: scan_dir and filerepo_name, not live-backed, and sync_role producer or
     * unset. A repository configured as a mirror never acts as a producer to other mirrors.
     */
    boolean isProducer() {
        return liveRow == null && scanDirString != null && fileRepoName != null
                && (syncRole.isEmpty() || "producer".equals(syncRole));
    }

    /** True when this repository is a mirror: filerepo_name and repo_dir, no scan_dir, sync_role mirror or unset. */
    boolean isMirror() {
        return liveRow == null && scanDirString == null && fileRepoName != null && repoDir != null
                && (syncRole.isEmpty() || "mirror".equals(syncRole));
    }

    public void start() {

        if (liveRow != null) {
            // read-only and recreatable: nothing here is scanned, broadcast, offered or subscribed to
            logger.info("live-backed repository (row " + liveRow + "): no scan, no sync");
            return;
        }

        long delay =  plugin.getConfig().getLongParam("scan_delay", 5000L);
        long period =  plugin.getConfig().getLongParam("scan_period", 15000L);

        if ("mirror".equals(syncRole) && scanDirString != null) {
            logger.error("sync_role=mirror with a scan_dir: this repository is not a producer, and serves no mirror");
        } else if ("producer".equals(syncRole) && scanDirString == null) {
            logger.error("sync_role=producer needs a scan_dir: this repository syncs nothing");
        }
        if (isProducer()) {
            logger.info("Starting file scan : " + scanDirString + " filerepo: " + fileRepoName);
            startScan(delay, period);
        } else if (isMirror()) {
            logger.info("Start listening for filerepo: " + fileRepoName + (configuredProducers != null
                    ? " (producers pinned: " + configuredProducers + ")"
                    : adminPin != null ? " (producer pinned: " + adminPin.pin() + ")" : ""));
            if (pinnedProducers() == null) {
                logger.warn("filerepo " + fileRepoName + ": no producer is pinned: this mirror syncs nothing until an admin"
                        + " pins its producer (sync_producers=region/agent#<fingerprint>, or syncpin); repostate lists the"
                        + " producers that answered (candidate_producers)" + (legacyPin == null ? "" : ". The pin an"
                        + " earlier build recorded, " + legacyPin + ", names no key and pins nothing now: pin the producer"
                        + " by its key, with adopt=true to hand it what that one delivered"));
            }
            checkGenerationMarker(true);
            checkMirroredFiles();
            createSubListener(fileRepoName);
            // the mirror's own scan: its generation marker, and the files its producer delivered that are gone
            TimerTask mirrorCheck = new TimerTask() {
                public void run() {
                    try {
                        if (plugin.isActive()) {
                            checkGenerationMarker(false);
                            checkMirroredFiles();
                        }
                    } catch (Exception ex) {
                        logger.error("filerepo mirror check error", ex);
                    }
                }
            };
            mirrorCheckTimer = new Timer("filerepo-mirror-check", true);
            mirrorCheckTimer.scheduleAtFixedRate(mirrorCheck, Math.max(1000L, delay), Math.max(1000L, period));
        }

    }

    public void startScan(long delay, long period) {

        if (liveRow != null) {
            logger.error("startScan refused: a live-backed repository runs no scanner");
            return;
        }

        //stop scan if started
        stopScan();

        //start listening
        logger.info("Creating Repo Listener for: " + fileRepoName);
        createRepoSubListener(fileRepoName);

        //create timer task
        TimerTask fileScanTask = new TimerTask() {
            public void run() {
                try {
                    if(plugin.isActive()) {
                        scanOnce();
                    } else {
                        logger.debug("scan skipped: plugin not active");
                    }
                } catch (Exception ex) {
                    logger.error("filerepo error", ex);
                }
            }
        };

        //create timer task
        TimerTask repoBroadcastTask = new TimerTask() {
            public void run() {
                try {

                    if(plugin.isActive()) {

                        logger.debug("\t\t ***BROADCASTING repo_name: " + fileRepoName + " inScan: " + inScan.get() + " tid:" + transferId);
                        //let everyone know repo exists
                        repoBroadcast(fileRepoName,"discover", transferId);

                    } else {
                        logger.debug("broadcast skipped: plugin not active");
                    }

                } catch (Exception ex) {
                    logger.error("filerepo error", ex);
                }
            }
        };

        fileScanTimer = new Timer("Timer");
        fileScanTimer.scheduleAtFixedRate(fileScanTask, delay, period);
        logger.debug("filescantimer : set : " + period);

        repoBroadcastTimer = new Timer("BroadCastTimer");
        repoBroadcastTimer.scheduleAtFixedRate(repoBroadcastTask, delay, period);
        logger.debug("broadcasttimer : set : " + period);
    }

    /**
     * One scan cycle: catalog what changed, record what was deleted (as the guard allows), offer each subscriber what
     * it still needs. Returns false when the cycle was skipped (another scan running, the scan dir missing, the gate
     * held, the dir over a live root).
     */
    boolean scanOnce() {
        //check file location
        if (!Paths.get(scanDirString).toFile().exists()) {
            logger.warn("scan dir no longer exists: " + scanDirString);
            return false;
        }
        // claim the scan atomically; the finally guarantees release even if the scan/sync throws
        if (!inScan.compareAndSet(false, true)) {
            logger.debug("already in scan");
            return false;
        }
        try {
            String conflict = repoDirConflict();
            if (conflict != null) {
                // a scan dir over live-tier files: catalog and offer none of them
                if (!warnedRepoDirConflict) {
                    logger.error("scan refused: the scan dir " + conflict);
                    warnedRepoDirConflict = true;
                }
                return false;
            }
            // the shared side of the repository gate: a clear holding it skips this cycle
            if (!gate.readLock().tryLock()) {
                logger.debug("scan skipped: a clear holds the repository gate");
                return false;
            }
            try {
                ScanResult result = scan();
                offerAll(result);
                pruneSubscribers();
                return true;
            } finally {
                gate.readLock().unlock();
            }
        } finally {
            inScan.set(false);
        }
    }

    public void shutdown() {
        stopping = true;
        Timer mc = mirrorCheckTimer;
        if (mc != null) {
            mc.cancel();
            mirrorCheckTimer = null;
        }

        if(repoBroadcastTimer != null) {
            repoBroadcastTimer.cancel();
            repoBroadcastTimer = null;
        }
        stopScan();
        List<String> ids;
        synchronized (listenerList) {
            ids = new ArrayList<>(listenerList);
            listenerList.clear();
        }
        for(String listenerid : ids) {
            try {
                plugin.getAgentService().getDataPlaneService().removeMessageListener(listenerid);
            } catch (Exception ex) {
                logger.error("listener removal error", ex);
            }
        }
        try { syncPool.shutdownNow(); } catch (Exception ex) { logger.error("syncPool shutdown error", ex); }
        try {
            // offers still queued are cancelled (a waiting scan cycle sees them end), running ones interrupted
            for (Runnable r : offerPool.shutdownNow()) {
                if (r instanceof Future) ((Future<?>) r).cancel(true);
            }
        } catch (Exception ex) {
            logger.error("offerPool shutdown error", ex);
        }

    }

    /** True while the scan timer is scheduled. */
    boolean isScanScheduled() {
        return fileScanTimer != null;
    }

    public void stopScan() {
        if(fileScanTimer != null) {
            logger.debug("Stopping existing scan");
            fileScanTimer.cancel();
            fileScanTimer = null;
        } else {
            logger.debug("No scan currently active");
        }
    }

    static boolean isStaging(String name) {
        return name != null && (PRIVATE_STAGING.equals(name) || (name.startsWith(STAGING_PREFIX) && name.endsWith(STAGING_SUFFIX)));
    }

    /** A fresh staging file beside {@code destination}: same directory, so the final move is an atomic rename. */
    static Path stagingFor(Path destination) {
        return destination.resolveSibling(STAGING_PREFIX + UUID.randomUUID().toString().replace("-", "") + STAGING_SUFFIX);
    }

    /**
     * The owner-only staging directory inside the repository (same filesystem as every destination in it, so the final
     * move is an atomic rename): nobody but the agent's user can rename a file over one filerepo is verifying.
     */
    Path privateStaging() throws IOException {
        Path dir = getRepoDir().toPath().toAbsolutePath().resolve(PRIVATE_STAGING);
        if (!Files.isDirectory(dir, LinkOption.NOFOLLOW_LINKS)) {
            try {
                Files.createDirectory(dir, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
            } catch (UnsupportedOperationException noPosix) {
                Files.createDirectory(dir);
            } catch (java.nio.file.FileAlreadyExistsException race) {
                // made meanwhile; checked below
            }
        }
        if (Files.isSymbolicLink(dir) || !Files.isDirectory(dir, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("the staging directory is not a directory");
        }
        try {
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwx------"));
        } catch (UnsupportedOperationException noPosix) {
            // not a POSIX filesystem
        }
        return dir;
    }

    /** What a walk found: regular files (as paths under the configured root), directories' devices, completeness. */
    private static final class Walk {
        final List<Path> files = new ArrayList<>();
        // mount points under the root: root-based path -> device
        final Map<String, Object> mounts = new HashMap<>();
        Object rootDev;
        boolean complete = true;
    }

    /**
     * The regular files under {@code root} (recursively or not), skipping staging files and, unless scan_follow_links,
     * every symbolic link below the root. The root itself is followed if it is a link (a scan dir may be one). Records
     * each directory's device, so a mount point under the root is known. An incomplete walk takes nothing for deleted.
     */
    private Walk walk(Path root, boolean recursive) {
        Walk w = new Walk();
        Path walkRoot;
        try {
            walkRoot = root.toRealPath();
        } catch (IOException ex) {
            w.complete = false;
            logger.error("scan dir " + root + " cannot be resolved: " + ex.getMessage());
            return w;
        }
        Function<Path, Object> dev = devOf;
        w.rootDev = dev.apply(walkRoot);
        Deque<Object> parentDevs = new ArrayDeque<>();
        try {
            Files.walkFileTree(walkRoot,
                    followLinks ? EnumSet.of(FileVisitOption.FOLLOW_LINKS) : EnumSet.noneOf(FileVisitOption.class),
                    recursive ? Integer.MAX_VALUE : 1,
                    new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                            if (!dir.equals(walkRoot) && isStaging(dir.getFileName().toString())) {
                                return FileVisitResult.SKIP_SUBTREE;
                            }
                            Object d = dev.apply(dir);
                            Object parent = parentDevs.peek();
                            if (parent != null && d != null && !d.equals(parent)) {
                                w.mounts.put(root.resolve(walkRoot.relativize(dir)).toString(), d);
                            }
                            parentDevs.push(d == null ? (parent == null ? "" : parent) : d);
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                            if (!parentDevs.isEmpty()) parentDevs.pop();
                            if (exc != null) {
                                w.complete = false;
                                logger.warn("scan: could not read " + dir + ": " + exc.getMessage());
                            }
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                            if (attrs.isRegularFile() && !isStaging(file.getFileName().toString())) {
                                w.files.add(root.resolve(walkRoot.relativize(file)));
                            }
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult visitFileFailed(Path file, IOException exc) {
                            if (exc instanceof FileSystemLoopException) {
                                logger.warn("scan: a directory loop at " + file + " was not followed");
                            } else {
                                w.complete = false;
                                logger.warn("scan: could not read " + file + ": " + exc.getMessage());
                            }
                            return FileVisitResult.CONTINUE;
                        }
                    });
        } catch (IOException ex) {
            w.complete = false;
            logger.error("scan walk failed: " + ex.getMessage());
        }
        return w;
    }

    /** What one scan found: new and changed files, and the deletes it recorded. */
    static final class ScanResult {
        final Map<String, FileObject> files;
        final List<DeleteObject> deletes;

        ScanResult(Map<String, FileObject> files, List<DeleteObject> deletes) {
            this.files = files;
            this.deletes = deletes;
        }
    }

    /** What one offer (a page) carried, to whom, for which of its generations: settled by that subscriber's confirm. */
    static final class Offer {
        final String subscriber;
        final String gen;
        final Map<String, FileObject> files;
        final List<DeleteObject> deletes;

        Offer(String subscriber, String gen, Map<String, FileObject> files, List<DeleteObject> deletes) {
            this.subscriber = subscriber;
            this.gen = gen;
            this.files = files;
            this.deletes = deletes;
        }
    }

    /**
     * Scan, then what a subscriber would be offered that no subscriber has confirmed yet (tests and diagnostics): the
     * new and changed files, and every unconfirmed row that can be offered.
     */
    Map<String,FileObject> getFileRepoDiff() {
        Map<String, FileObject> out = new LinkedHashMap<>(scan().files);
        Path root = Paths.get(scanDirString).toAbsolutePath();
        boolean recursive = plugin.getConfig().getBooleanParam("scan_recursive", true);
        for (Map<String, String> row : dbEngine.getFilesNotInSync()) {
            String path = row.get("filepath");
            if (out.containsKey(path) || offerable(root, path) != null) continue;
            try {
                out.put(path, new FileObject(nameUnder(root, Paths.get(path), recursive), row.get("md5"), path,
                        Long.parseLong(row.get("lastmodified")), Long.parseLong(row.get("filesize"))));
            } catch (Exception ignore) { }
        }
        return out;
    }

    private static String nameUnder(Path root, Path p, boolean recursive) {
        return recursive ? root.relativize(p.toAbsolutePath()).toString() : p.getFileName().toString();
    }

    /**
     * Why the cataloged file {@code path} cannot be offered now, or null: outside the scan dir ("outside"), gone, or in
     * a live-tier root ("live").
     */
    private String offerable(Path root, String path) {
        Path p = Paths.get(path);
        if (!p.toAbsolutePath().startsWith(root)) return "outside";
        // a row cataloged before a live-tier root was known is never offered from it
        if (liveRootConflict(p) != null) return "live";
        if (!Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS)) return "gone";
        return null;
    }

    /**
     * Scan the scan dir against the catalog: new and changed files are hashed and cataloged in batches (FR-D8) and become
     * pending for every subscriber; files that are gone become tombstones (FR-D4) as the mass-delete guard allows.
     */
    ScanResult scan() {
        // the guards' clock, recorded so a restart resumes it
        ledger.tick();

        Map<String,FileObject> fileDiffMap = new LinkedHashMap<>();
        List<DeleteObject> deletes = new ArrayList<>();
        try {
            Path root = Paths.get(scanDirString).toAbsolutePath();
            boolean scanRecursive = plugin.getConfig().getBooleanParam("scan_recursive",true);

            // Load the catalog once (path -> md5, lastmodified, filesize) so the per-file freshness check below is an
            // in-memory lookup instead of a DB round-trip per file on every scan cycle.
            Map<String,String[]> catalog = new HashMap<>();
            // the files cataloged under the scan dir, with when each was first cataloged (the guards count originals)
            Map<String, Long> born = new HashMap<>();
            for (Map<String,String> row : dbEngine.getRepoList()) {
                catalog.put(row.get("filepath"), new String[]{row.get("md5"), row.get("lastmodified"), row.get("filesize")});
                if (Paths.get(row.get("filepath")).toAbsolutePath().startsWith(root)) {
                    long c = 0L;
                    try { c = Long.parseLong(row.get("cataloged")); } catch (Exception ignore) { }
                    born.put(row.get("filepath"), c);
                }
            }
            long catalogedNow = ledger.now();

            Walk walk = walk(root, scanRecursive);

            Set<String> seen = new HashSet<>();
            List<DBEngine.Row> adds = new ArrayList<>();
            List<DBEngine.Row> updates = new ArrayList<>();
            List<String> changed = new ArrayList<>();
            // existing files whose content changed: {path, content before, content now}
            List<String[]> rewrites = new ArrayList<>();
            for (Path p : walk.files) {
                String filePath = p.toString();
                seen.add(filePath);
                long lastModified;
                long filesize;
                try {
                    BasicFileAttributes a = Files.readAttributes(p, BasicFileAttributes.class,
                            followLinks ? new LinkOption[0] : new LinkOption[]{LinkOption.NOFOLLOW_LINKS});
                    lastModified = a.lastModifiedTime().toMillis();
                    filesize = a.size();
                } catch (IOException gone) {
                    continue;   // vanished mid-scan: the next cycle sees it
                }
                String[] row = catalog.get(filePath);
                boolean add = row == null;
                boolean update = false;
                if (!add) {
                    long dbMtime = -1, dbSize = -1;
                    try { dbMtime = Long.parseLong(row[1]); } catch (Exception ignore) { }
                    try { dbSize = Long.parseLong(row[2]); } catch (Exception ignore) { }
                    update = dbMtime != lastModified || dbSize != filesize;
                }
                if (!add && !update) continue;

                // a link into a live-tier root is never read, cataloged or offered
                String conflict = liveRootConflict(p);
                if (conflict != null) {
                    logger.warn("skipping a file that " + conflict);
                    continue;
                }
                String MD5hash = Hashes.content(p);
                // Skip files we can't safely catalog: a null hash means the file vanished mid-scan; a path longer than
                // the catalog column would fail the insert. Skipping keeps one bad file from wedging the scan.
                if (MD5hash == null || filePath.length() > 1000) {
                    logger.warn("skipping uncatalogable file (unreadable or path>1000): " + filePath);
                    continue;
                }
                String fileName = nameUnder(root, p, scanRecursive);
                (add ? adds : updates).add(new DBEngine.Row(filePath, MD5hash, lastModified, filesize, catalogedNow));
                if (add) {
                    fileDiffMap.put(filePath, new FileObject(fileName, MD5hash, filePath, lastModified, filesize));
                    changed.add(filePath);
                } else if (!MD5hash.equals(row[0])) {
                    // same content with a new time (a touch) is not a change anyone needs offered
                    fileDiffMap.put(filePath, new FileObject(fileName, MD5hash, filePath, lastModified, filesize));
                    rewrites.add(new String[]{filePath, row[0], MD5hash});
                }
                if (adds.size() + updates.size() >= scanBatchSize) {
                    dbEngine.applyScan(adds, updates, scanBatchSize);
                    adds.clear();
                    updates.clear();
                }
                transferId++;
            }
            dbEngine.applyScan(adds, updates, scanBatchSize);
            // a repository's first scan (or its first in this build): what is cataloged now is what the guards protect
            ledger.initAcks();
            // the rewrite guard: what it holds is offered to nobody until an admin releases exactly that set
            Set<String> held = guardRewrites(rewrites, born);
            for (String[] r : rewrites) {
                if (held.contains(r[0])) fileDiffMap.remove(r[0]);
                else changed.add(r[0]);
            }
            // what changed is pending for every subscriber (and no longer a pending delete)
            dbEngine.markChanged(changed, scanBatchSize);

            // the delete phase: catalog rows under the scan dir whose file is gone
            if (walk.complete) {
                deletes.addAll(recordDeletes(root, scanRecursive, catalog, born, seen, walk));
            } else {
                logger.warn("scan of " + root + " was incomplete: no file is taken for deleted this cycle");
            }
            if (tombstoneTtlMs > 0) {
                int purged = dbEngine.purgeTombstones(System.currentTimeMillis() - tombstoneTtlMs);
                if (purged > 0) logger.debug("purged " + purged + " confirmed tombstone(s)");
            }

        } catch (Exception ex) {
            logger.error("filerepo scan error", ex);
        }
        return new ScanResult(fileDiffMap, deletes);
    }

    /**
     * Turn the catalog rows whose file is gone into tombstones (pending deletes for every subscriber), as the
     * mass-delete guard allows: what it holds stays cataloged and is offered to nobody until an admin releases exactly
     * that set, or the files come back. Remembers the tree's mount points, so a vanished one is told apart.
     */
    private List<DeleteObject> recordDeletes(Path root, boolean recursive, Map<String, String[]> catalog,
                                             Map<String, Long> underRoot, Set<String> seen, Walk walk) {
        long now = System.currentTimeMillis();
        List<DBEngine.Tombstone> gone = new ArrayList<>();
        for (Map.Entry<String, String[]> e : catalog.entrySet()) {
            String path = e.getKey();
            Path p = Paths.get(path);
            // a stale row from elsewhere (an earlier scan_dir) is not this scanner's to delete
            if (!underRoot.containsKey(path)) continue;
            if (seen.contains(path)) continue;
            // still there: a subdirectory file in a non-recursive scan, a link, an unreadable file
            if (Files.exists(p, LinkOption.NOFOLLOW_LINKS)) continue;
            gone.add(new DBEngine.Tombstone(path, nameUnder(root, p, recursive), e.getValue()[0], now));
        }

        // mount points: remembered ones that are no longer mount points (or gone) vanished; the scan dir itself too
        Map<String, String> remembered = readMounts();
        List<String> vanished = new ArrayList<>();
        for (String m : remembered.keySet()) {
            if (!walk.mounts.containsKey(m)) vanished.add(m);
        }
        String rootDevBefore = null;
        try { rootDevBefore = dbEngine.getMeta(META_ROOT_DEV); } catch (Exception ignore) { }
        String rootDevNow = walk.rootDev == null ? null : String.valueOf(walk.rootDev);
        if (rootDevBefore != null && rootDevNow != null && !rootDevBefore.equals(rootDevNow)) vanished.add(root.toString());

        DeleteGuard.Decision d = guard.evaluate(root, underRoot, gone, seen.size(), vanished, ledger);
        ledger.prune(guard.totalOn(), rewriteTotalFraction > 0);
        if (d.released) logger.warn("releasing " + d.held.size() + " held delete(s) recorded now, as an admin asked");
        if (!d.held.isEmpty()) {
            if (!d.holdId.equals(lastHoldLogged)) {
                logger.error("mass-delete guard: " + d.held.size() + " delete(s) held, not offered to subscribers ("
                        + d.why + "); hold " + d.holdId + ". If intended, an admin releases them with releasedeletes");
                lastHoldLogged = d.holdId;
            }
        } else {
            lastHoldLogged = null;
        }

        // remember the mount points seen now, and the vanished ones while deletes under them are held
        Map<String, String> keep = new TreeMap<>();
        for (Map.Entry<String, Object> e : walk.mounts.entrySet()) keep.put(e.getKey(), String.valueOf(e.getValue()));
        for (String m : vanished) {
            if (m.equals(root.toString())) continue;
            boolean holding = false;
            for (DBEngine.Tombstone t : d.held) if (Paths.get(t.filepath).startsWith(Paths.get(m))) holding = true;
            if (holding) keep.put(m, remembered.get(m));
        }
        writeMounts(keep);
        boolean rootHeld = vanished.contains(root.toString()) && !d.held.isEmpty();
        if (rootDevNow != null && !rootHeld) {
            try { dbEngine.setMeta(META_ROOT_DEV, rootDevNow); } catch (Exception ex) { logger.error("root device not recorded: " + ex.getMessage()); }
        }

        List<DeleteObject> recorded = new ArrayList<>();
        if (!d.record.isEmpty()) {
            int n = dbEngine.tombstone(d.record, scanBatchSize);
            List<String> paths = new ArrayList<>();
            for (DBEngine.Tombstone t : d.record) {
                paths.add(t.filepath);
                recorded.add(new DeleteObject(t.filename, t.md5, t.filepath, t.deletedAt));
            }
            dbEngine.markDeleted(paths, scanBatchSize);
            if (rewritesHeld > 0) {
                dbEngine.unholdChanges(paths);
                refreshRewriteHold();
            }
            tombstonesRecorded.addAndGet(n);
            if (n > 0) logger.info("recorded " + n + " deleted file(s) to offer to subscribers");
        }
        return recorded;
    }

    private Map<String, String> readMounts() {
        Map<String, String> out = new HashMap<>();
        try {
            String v = dbEngine.getMeta(META_MOUNTS);
            if (v != null) {
                for (String line : v.split("\n")) {
                    int tab = line.lastIndexOf('\t');
                    if (tab > 0) out.put(line.substring(0, tab), line.substring(tab + 1));
                }
            }
        } catch (Exception ex) {
            logger.error("remembered mount points unreadable: " + ex.getMessage());
        }
        return out;
    }

    private void writeMounts(Map<String, String> mounts) {
        try {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> e : mounts.entrySet()) {
                if (sb.length() > 0) sb.append('\n');
                sb.append(e.getKey()).append('\t').append(e.getValue());
            }
            if (sb.length() > 32000) {
                logger.warn("too many mount points under the scan dir to remember; the guard falls back to subtrees and fractions");
                return;
            }
            if (mounts.isEmpty()) dbEngine.deleteMeta(META_MOUNTS);
            else dbEngine.setMeta(META_MOUNTS, sb.toString());
        } catch (Exception ex) {
            logger.error("mount points not recorded: " + ex.getMessage());
        }
    }

    /**
     * The rewrite guard (the counterpart of the mass-delete guard): a cycle that rewrites at least
     * sync_rewrite_guard_min existing files AND more than sync_rewrite_guard_fraction of the catalog (ransomware, the
     * wrong volume or an old snapshot mounted on the mount point) has its rewrites held, offered to no subscriber, until
     * an admin releases exactly that set (releaserewrites with its hold id). A held file changed again stays held; one
     * changed back to the content the subscribers have is no longer held, and offered to nobody. Returns the paths held
     * now among {@code rewrites}.
     */
    private Set<String> guardRewrites(List<String[]> rewrites, Map<String, Long> born) {
        synchronized (rewriteLock) {
            return guardRewritesLocked(rewrites, born);
        }
    }

    private Set<String> guardRewritesLocked(List<String[]> rewrites, Map<String, Long> born) {
        Set<String> heldNow = new HashSet<>();
        if (rewrites.isEmpty()) return heldNow;
        int catalogSize = born.size();
        // the longer horizons, from the guards' ledger (kept across restarts, on a monotonic clock): originals (files
        // first cataloged before the period began) rewritten, each once, or deleted, within a rolling day and since the
        // last acknowledgement of each budget, against how many originals there were
        boolean cycleLarge = rewrites.size() >= rewriteMin && rewrites.size() > rewriteFraction * catalogSize;
        int today = 0, sinceAck = 0;
        boolean dayLarge = false, totalLarge = false;
        if (!cycleLarge && rewriteDayFraction > 0) {
            long start = ledger.dayStart(GuardLedger.DAY_R);
            List<String> originals = originalsOf(rewrites, born, start);
            if (!originals.isEmpty()) {
                today = ledger.count("DR", start, start) + originals.size() - ledger.known(GuardLedger.REWRITE, originals, start);
                dayLarge = today >= rewriteMin && today > rewriteDayFraction * ledger.originals(born, start);
            }
        }
        if (!cycleLarge && !dayLarge && rewriteTotalFraction > 0) {
            long start = ledger.ack(GuardLedger.TOT_R);
            List<String> originals = originalsOf(rewrites, born, start);
            if (!originals.isEmpty()) {
                sinceAck = ledger.count("DR", start, start) + originals.size() - ledger.known(GuardLedger.REWRITE, originals, start);
                totalLarge = sinceAck >= rewriteMin && sinceAck > rewriteTotalFraction * ledger.originals(born, start);
            }
        }
        boolean mass = cycleLarge || dayLarge || totalLarge;
        List<String[]> hold = new ArrayList<>();
        List<String> restored = new ArrayList<>();
        for (String[] r : rewrites) {
            String[] before = rewritesHeld > 0 ? dbEngine.heldChange(r[0]) : null;
            if (before != null) {
                heldNow.add(r[0]);
                if (r[2].equals(before[0])) restored.add(r[0]);
                else hold.add(new String[]{r[0], before[0], r[2]});
            } else if (mass) {
                heldNow.add(r[0]);
                hold.add(r);
            }
        }
        // what goes through counts toward the day and the totals (each file once)
        Map<String, Long> passed = new LinkedHashMap<>();
        for (String[] r : rewrites) if (!heldNow.contains(r[0])) passed.put(r[0], born.getOrDefault(r[0], 0L));
        ledger.record(GuardLedger.REWRITE, passed);
        if (hold.isEmpty() && restored.isEmpty()) return heldNow;
        if (mass && !hold.isEmpty()) {
            // which budgets hold this set: a release consumes those, and only those
            Set<String> budgets = rewriteHoldBudgets();
            if (cycleLarge) budgets.add("cycle");
            if (dayLarge) budgets.add("day");
            if (totalLarge) budgets.add("total");
            try {
                dbEngine.setMeta(META_REWRITE_HOLD_BUDGETS, String.join(",", budgets));
            } catch (Exception ex) {
                logger.error("the rewrite hold's budgets could not be recorded: " + ex.getMessage());
            }
        }
        dbEngine.holdChanges(hold, scanBatchSize);
        dbEngine.unholdChanges(restored);
        refreshRewriteHold();
        if (rewritesHeld > 0 && !String.valueOf(rewriteHoldId).equals(lastRewriteHoldLogged)) {
            logger.error("rewrite guard: " + rewritesHeld + " rewritten file(s) held, not offered to subscribers ("
                    + rewrites.size() + " of " + catalogSize + " cataloged files rewritten this cycle, " + today
                    + " deleted or rewritten in the last 24 h, " + sinceAck + " since the last acknowledgement); hold "
                    + rewriteHoldId + ". If intended, an admin releases them with releaserewrites");
            lastRewriteHoldLogged = rewriteHoldId;
        }
        return heldNow;
    }

    /** The rewritten paths that are originals of a period starting at {@code start}. */
    private static List<String> originalsOf(List<String[]> rewrites, Map<String, Long> born, long start) {
        List<String> out = new ArrayList<>();
        for (String[] r : rewrites) if (born.getOrDefault(r[0], 0L) <= start) out.add(r[0]);
        return out;
    }

    /** The budgets that hold the rewrites held now (the catalog's record; empty when unreadable). */
    private Set<String> rewriteHoldBudgets() {
        Set<String> out = new LinkedHashSet<>();
        try {
            String v = dbEngine.getMeta(META_REWRITE_HOLD_BUDGETS);
            if (v != null) for (String b : v.split(",")) if (!b.isEmpty()) out.add(b);
        } catch (Exception ex) {
            logger.error("the rewrite hold's budgets are unreadable: " + ex.getMessage());
        }
        return out;
    }

    /** Recompute the held rewrite set's size and id (after it changed). */
    private void refreshRewriteHold() {
        List<String[]> held = dbEngine.heldChanges();
        rewritesHeld = held.size();
        if (held.isEmpty()) {
            rewriteHoldId = null;
            lastRewriteHoldLogged = null;
            try {
                if (dbEngine.getMeta(META_REWRITE_HOLD_BUDGETS) != null) dbEngine.deleteMeta(META_REWRITE_HOLD_BUDGETS);
            } catch (Exception ex) {
                logger.error("the rewrite hold's budgets could not be cleared: " + ex.getMessage());
            }
            return;
        }
        List<DBEngine.Tombstone> asSet = new ArrayList<>(held.size());
        for (String[] h : held) asSet.add(new DBEngine.Tombstone(h[0], h[0], h[1], 0));
        rewriteHoldId = DeleteGuard.digest(asSet);
    }

    long getRewritesHeld() {
        return rewritesHeld;
    }

    String getRewriteHoldId() {
        return rewriteHoldId;
    }

    /**
     * Release the held rewrites (an admin's releaserewrites): they become pending for every subscriber at once, if
     * {@code holdId} names exactly the held set. The count released, 0 when nothing is held, -1 when the id names
     * another set, {@link DeleteGuard#RELEASE_NEEDS_ID} when it names none.
     */
    int releaseHeldRewrites(String holdId) {
        if (holdId == null || holdId.isEmpty()) return DeleteGuard.RELEASE_NEEDS_ID;
        synchronized (rewriteLock) {
            refreshRewriteHold();
            if (rewritesHeld == 0) return 0;
            if (!holdId.equals(rewriteHoldId)) return -1;
            List<String> paths = new ArrayList<>();
            for (String[] h : dbEngine.heldChanges()) paths.add(h[0]);
            Set<String> budgets = rewriteHoldBudgets();
            dbEngine.markChanged(paths, scanBatchSize);
            dbEngine.unholdChanges(paths);
            refreshRewriteHold();
            // the admin has seen them: the budgets that held them start over, and only those (the released rewrites
            // themselves count toward nothing); every other budget, the delete guard's included, keeps counting
            long now = ledger.now();
            if (budgets.contains("day")) ledger.acknowledge(GuardLedger.DAY_R, now);
            if (budgets.contains("total")) ledger.acknowledge(GuardLedger.TOT_R, now);
            try {
                dbEngine.deleteMeta(META_REWRITE_HOLD_BUDGETS);
            } catch (Exception ex) {
                logger.error("the rewrite hold's budgets could not be cleared: " + ex.getMessage());
            }
            logger.warn("releasing " + paths.size() + " held rewrite(s): offered to every subscriber, as an admin asked"
                    + (budgets.isEmpty() ? "" : " (acknowledging the " + String.join(", ", budgets) + " budget(s))"));
            return paths.size();
        }
    }

    // ---- a mirror's held deletes: of content whose producer's claim was orphaned (silent for sync_holder_ttl_ms) ----
    private final Map<String, String[]> mirrorHeld = new ConcurrentHashMap<>();
    private volatile String mirrorHoldId;
    // the held set's size as of its id: published after the id, so a reader that sees a hold sees its id
    private volatile int mirrorHeldCount;

    private synchronized void refreshMirrorHold() {
        List<DBEngine.Tombstone> set = new ArrayList<>();
        for (Map.Entry<String, String[]> e : new TreeMap<>(mirrorHeld).entrySet()) {
            set.add(new DBEngine.Tombstone(e.getKey(), e.getKey(), e.getValue()[0], 0));
        }
        String id = set.isEmpty() ? null : DeleteGuard.digest(set);
        if (id != null && !id.equals(mirrorHoldId)) {
            logger.error("filerepo " + fileRepoName + ": " + set.size() + " delete(s) of orphaned content held (its producer"
                    + " was silent for sync_holder_ttl_ms); hold " + id + ". An admin releases them with releasedeletes");
        }
        mirrorHoldId = id;
        mirrorHeldCount = set.size();
    }

    /**
     * Release this mirror's held deletes (an admin's releasedeletes naming {@code holdId}): each file is deleted if it
     * still holds the content the producer deleted and was not landed here. The count, 0 when nothing is held, -1 when
     * the id names another set, {@link DeleteGuard#RELEASE_NEEDS_ID} when it names none.
     */
    int releaseMirrorHeld(String holdId) {
        if (holdId == null || holdId.isEmpty()) return DeleteGuard.RELEASE_NEEDS_ID;
        if (mirrorHeld.isEmpty()) return 0;
        if (!holdId.equals(mirrorHoldId)) return -1;
        Path repoBase = getRepoDir().toPath().toAbsolutePath().normalize();
        Path realBase;
        try {
            realBase = repoBase.toRealPath();
        } catch (IOException ex) {
            realBase = repoBase;
        }
        int n = 0;
        if (!enterGate()) return 0;
        try {
            synchronized (nameLock) {
                for (Map.Entry<String, String[]> e : new ArrayList<>(mirrorHeld.entrySet())) {
                    String rel = e.getKey(), md5 = e.getValue()[0], producer = e.getValue()[1];
                    Target t = target(repoBase, realBase, rel);
                    try {
                        if (t.why == null && Files.isRegularFile(t.path, LinkOption.NOFOLLOW_LINKS)
                                && dbEngine.getFileInfo(t.path.toFile().getAbsolutePath()) == null
                                && md5.equals(Hashes.content(t.path))) {
                            Files.delete(t.path);
                            syncDeleted.incrementAndGet();
                        }
                    } catch (IOException ex) {
                        logger.warn("released delete of " + rel + " not done: " + ex.getMessage());
                        continue;
                    }
                    dbEngine.forgetMirrored(DBEngine.ORPHAN + producer, rel);
                    dbEngine.forgetMirrored(producer, rel);
                    mirrorHeld.remove(rel);
                    n++;
                }
            }
        } finally {
            exitGate();
        }
        refreshMirrorHold();
        logger.warn("released " + n + " held delete(s) of orphaned content, as an admin asked");
        return n;
    }

    /** How many deletes are held: by the mass-delete guard (a producer), or of orphaned content (a mirror). */
    int getDeletesHeld() {
        return isMirror() ? mirrorHeldCount : guard.heldCount();
    }

    /** The id of the held set (what releasedeletes names), or null. */
    String getHoldId() {
        return isMirror() ? mirrorHoldId : guard.holdId();
    }

    /**
     * Release the deletes now held (an admin's releasedeletes), once: the next scan records them if, and only if, it
     * finds exactly that set held. Returns how many were released, 0 when nothing is held, -1 when {@code holdId} names
     * another set.
     */
    int releaseHeldDeletes(String holdId) {
        if (isMirror()) return releaseMirrorHeld(holdId);
        return guard.release(holdId, ledger.now());
    }

    // =========================================================================== producer: offers

    private static String peerKey(String region, String agent, String pluginId) {
        return region + "_" + agent + "_" + pluginId;
    }

    /** True when the verified caller of {@code m} is a subscriber this producer serves (its fetches are sync). */
    boolean isSubscriberCaller(MsgEvent m) {
        String[] from = Callers.plugin(m, insecure);
        return from != null && subscribers.containsKey(peerKey(from[0], from[1], from[2]));
    }

    /** True when {@code region/agent/pluginId} has subscribed to this repository. */
    boolean isSubscriber(String region, String agent, String pluginId) {
        return subscribers.containsKey(peerKey(region, agent, pluginId));
    }

    private boolean inFlight(String key, long now) {
        Long at = inFlight.get(key);
        if (at == null) return false;
        if (now - at < offerTimeoutMs) return true;
        inFlight.remove(key, at);
        return false;
    }

    /**
     * Offer every reachable subscriber this cycle's changes and deletes, in full, then what else it still needs (a
     * subscriber that is new, cleared or that failed a file), paced.
     */
    private void offerAll(ScanResult result) {
        List<ActiveSub> subs = new ArrayList<>();
        for (ActiveSub s : subscribers.values()) if (s.reachable) subs.add(s);
        if (subs.isEmpty()) return;
        // one task per subscriber, run concurrently; the cycle ends when every one has (each is bounded: a page that is
        // not answered within sync_rpc_timeout_ms ends that subscriber's turn)
        List<Future<?>> turns = new ArrayList<>(subs.size());
        for (ActiveSub s : subs) {
            try {
                turns.add(offerPool.submit(() -> offerOne(s, result)));
            } catch (RejectedExecutionException shutDown) {
                for (Future<?> g : turns) g.cancel(true);
                return;
            }
        }
        // waited for in short steps: a stop (or an interrupt) ends the wait at once, never stranding the scan thread
        for (Future<?> f : turns) {
            while (true) {
                if (stopping) {
                    for (Future<?> g : turns) g.cancel(true);
                    return;
                }
                try {
                    f.get(200, TimeUnit.MILLISECONDS);
                    break;
                } catch (java.util.concurrent.TimeoutException notYet) {
                    // check for a stop, then wait again
                } catch (InterruptedException stop) {
                    for (Future<?> g : turns) g.cancel(true);
                    Thread.currentThread().interrupt();
                    return;
                } catch (CancellationException cancelled) {
                    break;
                } catch (ExecutionException ex) {
                    logger.error("offer failed", ex);
                    break;
                }
            }
        }
    }

    private void offerOne(ActiveSub s, ScanResult result) {
        try {
            if (offerFresh(s, result)) offerTo(s);
        } catch (Exception ex) {
            logger.error("offer to " + s.key + " failed", ex);
        }
    }

    /** This cycle's changes and deletes, every page, to one subscriber. False when it stopped taking pages. */
    private boolean offerFresh(ActiveSub s, ScanResult result) {
        if (result.files.isEmpty() && result.deletes.isEmpty()) return true;
        long now = System.currentTimeMillis();
        List<FileObject> files = new ArrayList<>();
        for (FileObject f : result.files.values()) if (!inFlight(s.key + "|" + f.filePath, now)) files.add(f);
        List<DeleteObject> dels = new ArrayList<>();
        for (DeleteObject d : result.deletes) if (!inFlight(s.key + "|del|" + d.filePath, now)) dels.add(d);
        int pages = Math.max((files.size() + diffPageSize - 1) / diffPageSize, (dels.size() + deletePageSize - 1) / deletePageSize);
        for (int page = 0; page < pages; page++) {
            Map<String, FileObject> pageFiles = new LinkedHashMap<>();
            for (FileObject f : files.subList(Math.min(files.size(), page * diffPageSize), Math.min(files.size(), (page + 1) * diffPageSize))) {
                pageFiles.put(f.filePath, f);
            }
            List<DeleteObject> pageDeletes = new ArrayList<>(dels.subList(Math.min(dels.size(), page * deletePageSize),
                    Math.min(dels.size(), (page + 1) * deletePageSize)));
            if (!sendPage(s, pageFiles, pageDeletes)) return false;
        }
        return true;
    }

    /** Send one page to one subscriber, bound to it and to this instance. False when it was not taken. */
    private boolean sendPage(ActiveSub s, Map<String, FileObject> pageFiles, List<DeleteObject> pageDeletes) {
        long now = System.currentTimeMillis();
        String tid = epoch + "." + transferSeq.incrementAndGet();
        String gen = s.gen;
        pendingTransferMap.put(tid, new Offer(s.key, gen, pageFiles, pageDeletes));
        ProducerKey key = signingKey;
        if (key == null) {
            pendingTransferMap.remove(tid);
            return false;
        }
        MsgEvent req = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, s.region, s.agent, s.plugin);
        req.setParam("action", "repolistin");
        String filesJson = gson.toJson(pageFiles);
        String deletesJson = pageDeletes.isEmpty() ? null : gson.toJson(pageDeletes);
        req.setCompressedParam("repolistin", filesJson);
        if (deletesJson != null) req.setCompressedParam("repodeletes", deletesJson);
        req.setParam("transfer_id", tid);
        req.setParam("sync_proto", SYNC_PROTO);
        // the subscriber's generation this offer is for: a mirror cleared since refuses it
        req.setParam("sync_gen", gen);
        // signed with this producer's key: what it offers and deletes, to whom, for which generation, in which transfer
        req.setParam("sync_sig", key.sign(offerParts(fileRepoName, SyncAuth.addr(plugin.getRegion(), plugin.getAgent(),
                plugin.getPluginID()), SyncAuth.addr(s.region, s.agent, s.plugin), tid, gen, filesJson, deletesJson)));
        MsgEvent reply = plugin.sendRPC(req, syncRpcTimeoutMs);
        if (reply == null) {
            logger.warn("subscriber " + s.key + " did not answer transfer " + tid + "; skipped until it subscribes again");
            s.reachable = false;
            pendingTransferMap.remove(tid);
            return false;
        }
        String code = reply.getParam("status_code");
        if (!"10".equals(code)) {
            logger.warn("subscriber " + s.key + " did not take transfer " + tid + ": status_code " + code + " "
                    + reply.getParam("status_desc"));
            pendingTransferMap.remove(tid);
            return false;
        }
        for (String p : pageFiles.keySet()) inFlight.put(s.key + "|" + p, now);
        for (DeleteObject d : pageDeletes) inFlight.put(s.key + "|del|" + d.filePath, now);
        logger.info("Offered " + pageFiles.size() + " file(s) and " + pageDeletes.size() + " delete(s) to "
                + s.plugin + " (transfer " + tid + ")");
        return true;
    }

    /**
     * Offer one subscriber what it still needs (its pending files and deletes), in pages of sync_diff_page files and
     * sync_delete_page deletes, at most sync_pages_per_cycle pages a cycle. Pages are read by a key cursor over what is
     * pending, so nothing slides past while the catalog changes; what is in flight is skipped until it is confirmed,
     * failed or times out.
     */
    private void offerTo(ActiveSub s) {
        Path root = Paths.get(scanDirString).toAbsolutePath();
        boolean recursive = plugin.getConfig().getBooleanParam("scan_recursive", true);
        String fileCursor = "", delCursor = "";
        boolean filesDone = false, delsDone = false;
        for (int pages = 0; pages < pagesPerCycle && !(filesDone && delsDone); ) {
            long now = System.currentTimeMillis();
            Map<String, FileObject> pageFiles = new LinkedHashMap<>();
            while (!filesDone && pageFiles.size() < diffPageSize) {
                List<String[]> rows = dbEngine.pendingFiles(s.key, fileCursor, diffPageSize);
                if (rows.isEmpty()) {
                    filesDone = true;
                    break;
                }
                for (String[] r : rows) {
                    if (pageFiles.size() >= diffPageSize) break;   // the cursor stays before r: next page starts at it
                    fileCursor = r[0];
                    String path = r[1];
                    if (inFlight(s.key + "|" + path, now)) continue;
                    String why = offerable(root, path);
                    if ("outside".equals(why) || "live".equals(why)) {
                        dbEngine.dropPending(s.key, r[0]);
                        continue;
                    }
                    if (why != null) continue;   // gone: its delete will follow
                    try {
                        pageFiles.put(path, new FileObject(nameUnder(root, Paths.get(path), recursive), r[2], path,
                                Long.parseLong(r[3]), Long.parseLong(r[4])));
                    } catch (Exception bad) {
                        logger.error("malformed catalog row " + path);
                    }
                }
                if (rows.size() < diffPageSize && fileCursor.equals(rows.get(rows.size() - 1)[0])) filesDone = true;
            }
            List<DeleteObject> pageDeletes = new ArrayList<>();
            while (!delsDone && pageDeletes.size() < deletePageSize) {
                List<String[]> rows = dbEngine.pendingDeletes(s.key, delCursor, deletePageSize);
                if (rows.isEmpty()) {
                    delsDone = true;
                    break;
                }
                for (String[] r : rows) {
                    if (pageDeletes.size() >= deletePageSize) break;
                    delCursor = r[0];
                    if (inFlight(s.key + "|del|" + r[1], now)) continue;
                    pageDeletes.add(new DeleteObject(r[2], r[3], r[1], Long.parseLong(r[4])));
                }
                if (rows.size() < deletePageSize && delCursor.equals(rows.get(rows.size() - 1)[0])) delsDone = true;
            }
            if (pageFiles.isEmpty() && pageDeletes.isEmpty()) break;
            if (!sendPage(s, pageFiles, pageDeletes)) return;
            pages++;
        }
    }

    /**
     * Subscribers the producer keeps: everything a subscriber has not confirmed stays pending for it (deletes
     * included: a tombstone is purged only when no kept subscriber still needs it), however long it is away, up to
     * sync_subscriber_ttl_ms. One that has confirmed nothing for sync_lag_alarm_ms is an alarm (an ERROR once a day, and
     * the filerepo.sync.subscribers.lagging gauge); one silent for sync_subscriber_ttl_ms is dropped, with an ERROR: the
     * deletes it never applied are forgotten, so it may hold files deleted here, and is told so (and should be cleared)
     * if it comes back.
     */
    private void pruneSubscribers() {
        long now = clock.getAsLong();
        for (ActiveSub s : new ArrayList<>(subscribers.values())) {
            long quiet = now - Math.max(s.lastSeen, s.lastConfirmed);
            if (quiet > subscriberTtlMs) {
                subscribers.remove(s.key);
                long missed = dbEngine.pendingCount(s.key);
                dbEngine.deleteSubscriber(s.key);
                inFlight.keySet().removeIf(k -> k.startsWith(s.key + "|"));
                droppedForLag.put(s.key, now);
                lagAlarmed.remove(s.key);
                subscribersDropped.incrementAndGet();
                logger.error("subscriber " + s.key + " dropped: silent for " + (quiet / 3_600_000L) + " h (sync_subscriber_ttl_ms);"
                        + " it had " + missed + " file(s) and delete(s) pending. It may hold files deleted here since:"
                        + " clear it (clearrepo) to make it exact");
            } else if (now - s.lastConfirmed > lagAlarmMs) {
                Long alarmed = lagAlarmed.get(s.key);
                if (alarmed == null || now - alarmed > 24L * 3600 * 1000) {
                    long owed = dbEngine.pendingCount(s.key);
                    if (owed > 0) {
                        lagAlarmed.put(s.key, now);
                        logger.error("subscriber " + s.key + " has confirmed nothing for " + ((now - s.lastConfirmed) / 3_600_000L)
                                + " h and has " + owed + " file(s) and delete(s) pending; if silent, it is dropped after "
                                + (subscriberTtlMs / 3_600_000L) + " h (sync_subscriber_ttl_ms)");
                    } else {
                        // nothing owed: it is not lagging
                        lagAlarmed.remove(s.key);
                        s.lastConfirmed = now;
                    }
                }
            } else {
                lagAlarmed.remove(s.key);
            }
        }
    }

    /** Subscribers that owe confirms for longer than sync_lag_alarm_ms. */
    int getLaggingSubscribers() {
        return lagAlarmed.size();
    }

    /**
     * reposubscribe (protocol 2): a subscriber, named by its stamped caller, asks to be served. Taken only by a
     * producer of the same filerepo_name speaking the same protocol, and, when this producer has a sync_key, only from a
     * subscriber that proves it. A subscriber with a new sync generation (new, or cleared) is offered everything again,
     * a few times at once and then once per sync_reset_interval_ms (each reset re-offers the whole catalog). The answer
     * (reposubscribed) goes to the verified caller, never to an address the message names, and carries this producer's
     * stable key and, with a sync_key, its proof.
     */
    void onSubscribe(MsgEvent m) {
        String[] from = Callers.plugin(m, insecure);
        if (from == null) {
            logger.warn("reposubscribe ignored: no verified plugin caller");
            return;
        }
        String key = peerKey(from[0], from[1], from[2]);
        String name = m.getParam("filerepo_name");
        String gen = m.getParam("sync_gen");
        String proto = m.getParam("sync_proto");
        String nonce = m.getParam("sync_nonce");
        String me = SyncAuth.addr(plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
        String them = SyncAuth.addr(from[0], from[1], from[2]);
        String refusal = null;
        if (!isProducer()) refusal = "not a producer";
        else if (!fileRepoName.equals(name)) refusal = "a producer of another filerepo_name";
        else if (signingKey == null) refusal = "this producer has no signing key (" + signingKeyError + "): see its log";
        else if (!SYNC_PROTO.equals(proto)) refusal = "sync protocol " + proto + " is not " + SYNC_PROTO + ": upgrade the subscriber";
        else if (gen == null || !gen.matches("[0-9a-f]{1,64}")) refusal = "no valid sync_gen";
        else if (nonce == null || !nonce.matches("[0-9a-f]{32}")) refusal = "no valid sync_nonce";
        else if (from[0].equals(plugin.getRegion()) && from[1].equals(plugin.getAgent()) && from[2].equals(plugin.getPluginID())) refusal = "itself";
        else if (syncKey != null && !SyncAuth.verify(syncKey, m.getParam("sync_auth"), "reposubscribe", name, them, me, gen, nonce)) {
            syncUnverified.incrementAndGet();
            refusal = "it does not hold this repository's sync_key";
        }
        long now = System.currentTimeMillis();
        boolean wasDropped = false;
        if (refusal == null) {
            ActiveSub s = subscribers.get(key);
            if (s == null) {
                pruneSubscribers();
                if (subscribers.size() >= syncMaxPeers) refusal = "full: " + subscribers.size() + " subscribers (sync_max_peers)";
            } else if (!gen.equals(s.gen) && !s.takeReset(now, resetIntervalMs)) {
                refusal = "a new generation too soon after the last (sync_reset_interval_ms): subscribe again later";
            }
            if (refusal == null) {
                if (s == null || !gen.equals(s.gen)) {
                    // new to this producer, or cleared: everything is pending for it again
                    if (!dbEngine.resetSubscriber(new DBEngine.Subscriber(key, from[0], from[1], from[2], gen, now), scanBatchSize)) {
                        refusal = "the catalog could not record it";
                    } else {
                        inFlight.keySet().removeIf(k -> k.startsWith(key + "|"));
                        // offers made for its earlier generation settle nothing now
                        synchronized (pendingTransferMap) {
                            pendingTransferMap.values().removeIf(o -> o.subscriber.equals(key));
                        }
                        ActiveSub fresh = new ActiveSub(key, from[0], from[1], from[2], gen, now);
                        if (s != null) {
                            fresh.resetTokens = s.resetTokens;
                            fresh.resetRefilled = s.resetRefilled;
                        }
                        subscribers.put(key, fresh);
                        wasDropped = droppedForLag.remove(key) != null;
                        logger.info("subscriber " + key + " (generation " + gen + "): everything is offered to it");
                    }
                } else {
                    s.lastSeen = now;
                    s.reachable = true;
                    dbEngine.touchSubscriber(key, now);
                }
            }
        }
        MsgEvent reply = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, from[0], from[1], from[2]);
        reply.setParam("action", "reposubscribed");
        reply.setParam("filerepo_name", String.valueOf(fileRepoName));
        reply.setParam("sync_proto", SYNC_PROTO);
        reply.setParam("accepted", String.valueOf(refusal == null));
        if (refusal != null) {
            reply.setParam("refusal", refusal);
            logger.info("reposubscribe from " + key + " refused: " + refusal);
        } else {
            String dir = producerDir();
            ProducerKey sk = signingKey;
            reply.setParam("producer_epoch", epoch);
            // the directory this producer produces from (shown to admins); its identity is its node and its key
            reply.setParam("producer_dir", dir);
            reply.setParam("sync_nonce", nonce);
            reply.setParam("producer_pubkey", sk.publicB64);
            reply.setParam("producer_key", sk.fingerprint);
            // the proof: this producer's key signs the mirror's fresh nonce, both ends' stamped addresses and its generation
            reply.setParam("producer_sig", sk.sign(answerParts(name, me, them, nonce, gen, dir, epoch)));
            if (syncKey != null) reply.setParam("sync_auth", SyncAuth.mac(syncKey, "reposubscribed", name, me, them, nonce, dir, gen));
            if (wasDropped) reply.setParam("notice", "dropped-for-lag");
        }
        plugin.msgOut(reply);
    }

    //data transfer
    public void confirmTransfer(String incomingTransferId, String failedFilesJson, String region, String agent, String pluginId) {
        confirmTransfer(incomingTransferId, failedFilesJson, null, null, region, agent, pluginId);
    }

    public void confirmTransfer(String incomingTransferId, String failedFilesJson, String failedDeletesJson,
                                String region, String agent, String pluginId) {
        confirmTransfer(incomingTransferId, failedFilesJson, failedDeletesJson, null, region, agent, pluginId);
    }

    /**
     * A subscriber's confirm: the files it now holds (all offered but {@code failedFiles}) are settled for it where the
     * catalog still holds the offered version, and so are the deletes it applied. Only the subscriber the transfer was
     * offered to settles it, only a transfer of this producer instance (its epoch), and only for the subscriber's
     * generation it was offered for: a confirm from before a restart, for another subscriber's transfer, or from before
     * the subscriber was cleared (its generation renewed) settles nothing. What failed stays pending for that subscriber
     * alone.
     */
    public void confirmTransfer(String incomingTransferId, String failedFilesJson, String failedDeletesJson,
                                String confirmGen, String region, String agent, String pluginId) {
        try {
            String key = peerKey(region, agent, pluginId);
            Offer offered = incomingTransferId == null ? null : pendingTransferMap.get(incomingTransferId);
            if (offered == null || !offered.subscriber.equals(key)) {
                logger.debug("confirm of transfer " + incomingTransferId + " from " + key + " ignored: not its transfer"
                        + " (another subscriber's, an earlier instance's, or expired)");
                return;
            }
            pendingTransferMap.remove(incomingTransferId);
            ActiveSub current = subscribers.get(key);
            if (current == null || !offered.gen.equals(current.gen) || (confirmGen != null && !confirmGen.equals(offered.gen))) {
                logger.info("confirm of transfer " + incomingTransferId + " from " + key + " ignored: it was offered for"
                        + " generation " + offered.gen + ", and the subscriber is now at "
                        + (current == null ? "none" : current.gen) + " (cleared since)");
                return;
            }
            Set<String> failedFiles = parseSet(failedFilesJson, key);
            Set<String> failedDeletes = parseSet(failedDeletesJson, key);

            List<DBEngine.Row> held = new ArrayList<>();
            for (FileObject f : offered.files.values()) {
                inFlight.remove(key + "|" + f.filePath);
                if (!failedFiles.contains(f.filePath)) held.add(new DBEngine.Row(f.filePath, f.MD5, f.lastModified, f.filesize));
            }
            List<DBEngine.Tombstone> applied = new ArrayList<>();
            for (DeleteObject d : offered.deletes) {
                inFlight.remove(key + "|del|" + d.filePath);
                if (!failedDeletes.contains(d.filePath)) applied.add(new DBEngine.Tombstone(d.filePath, d.fileName, d.MD5, d.deletedAt));
            }
            int files = dbEngine.settleFiles(key, held);
            dbEngine.confirmFiles(held);
            int dels = dbEngine.settleDeletes(key, applied);
            dbEngine.confirmTombstones(applied);
            current.lastSeen = System.currentTimeMillis();
            current.lastConfirmed = current.lastSeen;
            dbEngine.touchSubscriber(key, current.lastSeen);
            logger.info("Transfer " + incomingTransferId + " confirmed by " + key + ": " + files + " held, "
                    + failedFiles.size() + " failed, " + dels + " delete(s) applied, " + failedDeletes.size()
                    + " delete(s) failed" + (failedFiles.isEmpty() && failedDeletes.isEmpty() ? "" : " (offered again)"));
        } catch (Exception ex) {
            logger.error("filerepo confirm error", ex);
        }
    }

    private Set<String> parseSet(String json, String repoId) {
        Set<String> out = new HashSet<>();
        if (json == null) return out;
        try {
            List<String> list = gson.fromJson(json, stringListType);
            if (list != null) out.addAll(list);
        } catch (Exception pe) {
            logger.error("confirmTransfer: unparseable list from " + repoId, pe);
        }
        return out;
    }

    // =========================================================================== subscriber

    /**
     * One diff from a producer, waiting for its updater: for this repository's generation, from one holder (null: from a
     * verified producer that is not this mirror's pinned producer, refused whole).
     */
    private static final class PendingDiff {
        final String transferId;
        final String filesJson;
        final String deletesJson;
        final String gen;
        final String holder;

        PendingDiff(String transferId, String filesJson, String deletesJson, String gen, String holder) {
            this.transferId = transferId;
            this.filesJson = filesJson;
            this.deletesJson = deletesJson;
            this.gen = gen;
            this.holder = holder;
        }
    }

    /** How a diff was taken: 10 queued, 9 refused (not a verified producer), 11 busy (bounded queues full; offer again). */
    static final class Accepted {
        final String code;
        final String desc;

        Accepted(String code, String desc) {
            this.code = code;
            this.desc = desc;
        }
    }

    /**
     * Tests only: remember a producer as verified and pinned, under an identity made from its address, with no key (its
     * diffs are taken unsigned). Production knows a producer only from its signed answer ({@link #onSubscribed}), which
     * always carries its key; nothing a message carries reaches this.
     */
    void rememberProducer(String region, String agent, String pluginId) {
        rememberProducer(region, agent, pluginId, new Identity("-", region, agent, fileRepoName, null, "/" + pluginId), true, null);
    }

    /** Remember a verified producer: its identity, its key, and whether it is this mirror's pinned producer. Bounded. */
    void rememberProducer(String region, String agent, String pluginId, Identity id, boolean pinned, java.security.PublicKey key) {
        String addr = peerKey(region, agent, pluginId);
        long now = System.currentTimeMillis();
        if (!knownProducers.containsKey(addr) && knownProducers.size() >= syncMaxPeers) {
            knownProducers.entrySet().removeIf(e -> now - e.getValue().lastSeen > peerTtlMs);
            if (knownProducers.size() >= syncMaxPeers) {
                logger.warn("not taking producer " + addr + ": already " + knownProducers.size() + " producers (sync_max_peers)");
                return;
            }
        }
        knownProducers.put(addr, new KnownProducer(id, pluginId, pinned, key, now));
        // a pinned producer heard from now: its claims are live (an orphaned claim is not taken back by this; see decide)
        if (pinned) {
            dbEngine.touchHolder(id.holder(), now);
            // named, so repostate can say whose content it is if this producer is unpinned later
            if (id.key != null && !id.toString().equals(namedHolders.put(id.holder(), id.toString()))) {
                dbEngine.nameHolder(id.holder(), id.toString());
            }
        }
    }

    // holders named in the catalog this run (each written once)
    private final Map<String, String> namedHolders = new ConcurrentHashMap<>();

    boolean isKnownProducer(String region, String agent, String pluginId) {
        return knownProducer(region, agent, pluginId) != null;
    }

    private KnownProducer knownProducer(String region, String agent, String pluginId) {
        KnownProducer kp = knownProducers.get(peerKey(region, agent, pluginId));
        return kp != null && System.currentTimeMillis() - kp.lastSeen <= peerTtlMs ? kp : null;
    }

    /**
     * Is {@code id} this mirror's producer? With sync_producers, it must match one of the pins; without, the pin an admin
     * set with syncpin. Nothing else: no producer is ever pinned because it answered (first, or at all).
     */
    private boolean isPinnedProducer(Identity id) {
        if (id == null || id.key == null) return false;
        if (configuredProducers != null) {
            for (Pin p : configuredProducers) if (p.matches(id)) return true;
            return false;
        }
        Identity pin = adminPin;
        return pin != null && pin.key != null && pin.region.equals(id.region) && pin.agent.equals(id.agent)
                && pin.key.equals(id.key);
    }

    /** The producer pin(s), for repostate; null when nothing is pinned (this mirror syncs nothing). */
    String pinnedProducers() {
        if (configuredProducers != null) return configuredProducers.isEmpty() ? null : configuredProducers.toString();
        Identity t = adminPin;
        return t == null ? null : t.pin();
    }

    /** The holders (claim keys) of the producers pinned now. */
    private Set<String> pinnedHolders() {
        Set<String> out = new HashSet<>();
        if (configuredProducers != null) {
            for (Pin p : configuredProducers) out.add(new Identity(null, p.region, p.agent, fileRepoName, p.key, null).holder());
        } else if (adminPin != null) {
            out.add(adminPin.holder());
        }
        return out;
    }

    private static final java.util.regex.Pattern HOLDER = java.util.regex.Pattern.compile("~?[kn]:[0-9a-f]{40}");

    /** The holder {@code from} names: a holder as repostate shows it, or a pin ({@code region/agent#fingerprint}). */
    private String holderOf(String from) {
        if (from == null) return null;
        from = from.trim();
        if (HOLDER.matcher(from).matches()) return from.startsWith(DBEngine.ORPHAN) ? from.substring(1) : from;
        Pin p = Pin.parse(from);
        return p == null ? null : new Identity(null, p.region, p.agent, fileRepoName, p.key, null).holder();
    }

    /**
     * syncpin (an admin's). {@code spec} ({@code region/agent#fingerprint}) becomes this mirror's producer; or, with
     * {@code spec} null (reset), the pin is forgotten and this mirror syncs nothing until an admin pins one again (it
     * never falls back to whoever answers). What the previous producer delivered here is orphaned (kept, protected from
     * deletes, changed by nobody but it), unless {@code adopt}: then it becomes the new producer's, from the producer
     * {@code from} names (a pin, or a holder as repostate's orphaned_from shows it), or else from the previous pin. Where
     * sync_producers is configured the pins are not changed here: only an adopt to one of the configured producers is
     * taken (a producer that moved, re-pinned in configuration). A new generation follows: the producer offers
     * everything. Null when it was done, otherwise why not.
     */
    synchronized String syncPin(String spec, boolean adopt, String from) {
        if (from != null && holderOf(from) == null) {
            return "from must be a producer pin (region/agent#<fingerprint>) or a holder as repostate's orphaned_from shows it";
        }
        if (from != null && !adopt) return "from is taken only with adopt=true";
        Identity next = null;
        if (spec != null) {
            String why = Pin.why(spec);
            if (why != null) return "producer must be region/agent#<its key fingerprint>: " + why;
            Pin pin = Pin.parse(spec);
            String tenant = null;
            try {
                tenant = plugin.getAgentService().getTenant();
            } catch (Exception ignore) { }
            next = new Identity(tenant, pin.region, pin.agent, fileRepoName, pin.key, null);
        }
        if (configuredProducers != null) {
            if (next == null || !adopt || from == null) {
                return "this mirror's producers come from sync_producers (configuration); here only adopt=true from=<the"
                        + " old producer> producer=<one of the configured pins> is taken";
            }
            boolean configured = false;
            for (Pin p : configuredProducers) if (p.matches(next)) configured = true;
            if (!configured) return "producer " + next.pin() + " is not one of this mirror's sync_producers";
            String fromHolder = holderOf(from);
            int n = dbEngine.adoptClaims(fromHolder, next.holder());
            dbEngine.nameHolder(next.holder(), next.toString());
            logger.warn("syncpin: " + n + " claim(s) of " + from + " handed to " + next.pin() + ", as an admin asked");
            renewAfterPin();
            return null;
        }
        Identity before = adminPin;
        try {
            if (next == null) dbEngine.deleteMeta(META_PRODUCER_PIN);
            else dbEngine.setMeta(META_PRODUCER_PIN, next.full());
        } catch (Exception ex) {
            return "the pin could not be recorded: " + ex.getMessage();
        }
        adminPin = next;
        if (next != null) dbEngine.nameHolder(next.holder(), next.toString());
        String fromHolder = from != null ? holderOf(from)
                : adopt ? (before != null ? before.holder() : legacyPin != null ? legacyPin.holder() : null) : null;
        if (adopt && next != null && fromHolder != null && !fromHolder.equals(next.holder())) {
            int n = dbEngine.adoptClaims(fromHolder, next.holder());
            logger.warn("syncpin: " + n + " claim(s) of " + (from != null ? from : before != null ? before.pin() : legacyPin)
                    + " handed to " + next.pin() + ", as an admin asked");
        }
        if (before != null && (next == null || !before.holder().equals(next.holder()))
                && !(next != null && adopt && before.holder().equals(fromHolder))) {
            int n = dbEngine.orphanHolder(before.holder());
            logger.warn("syncpin: " + n + " claim(s) of " + before.pin() + " orphaned (kept, protected, frozen; repostate"
                    + " shows them as orphaned content)");
        }
        renewAfterPin();
        logger.warn("syncpin: producer pin " + (next == null ? "cleared: this mirror syncs nothing until an admin pins its"
                + " producer" : "is " + next.pin()) + "; a new generation " + syncGen);
        return null;
    }

    /** After a pin or a hand-over: producers are asked again, and the new generation makes the producer offer everything. */
    private void renewAfterPin() {
        refillWanted.clear();
        refillCursor.clear();
        knownProducers.clear();
        attempts.clear();
        unpinnedSeen.clear();
        unpinnedIds.clear();
        syncGen = newGen();
        writeGenerationMarker();
    }

    /** This repository's sync generation, as its subscribe advertises it (tests). */
    String getSyncGen() {
        return syncGen;
    }

    /**
     * A producer's discover (dataplane, unverified): only an address to ask. Taken by a mirror only; with sync_producers
     * only from a pinned node, and with a sync_key only when the discover carries a fresh proof of it (a forged discover
     * costs nothing and sends nothing). A subscribe (reposubscribe, a MsgEvent, with a nonce and, with a sync_key, its
     * proof) goes to it at most once per sync_subscribe_retry_ms per producer; the producer is known only when it
     * answers, verified by its stamped caller (and its proof), and only the pinned producer may change anything here.
     */
    private void onDiscover(Map<String, String> m) {
        String region = m.get("repo_region_id"), agent = m.get("repo_agent_id"), pluginId = m.get("repo_plugin_id");
        if (region == null || agent == null || pluginId == null) {
            logger.debug("discover without identification");
            return;
        }
        if (region.equals(plugin.getRegion()) && agent.equals(plugin.getAgent()) && pluginId.equals(plugin.getPluginID())) return;
        String key = peerKey(region, agent, pluginId);
        long now = System.currentTimeMillis();
        if (!SYNC_PROTO.equals(m.get("sync_proto"))) {
            warnOldProtocol(key, "producer " + key + " speaks sync protocol " + m.getOrDefault("sync_proto", "1") + ", not "
                    + SYNC_PROTO + ": not syncing from it until it is upgraded");
            return;
        }
        if (!isMirror()) return;
        if (configuredProducers != null) {
            boolean node = false;
            for (Pin p : configuredProducers) if (p.region.equals(region) && p.agent.equals(agent)) node = true;
            if (!node) return;
        }
        if (syncKey != null) {
            long ts;
            try {
                ts = Long.parseLong(m.get("sync_ts"));
            } catch (Exception bad) {
                ts = Long.MIN_VALUE;
            }
            if (ts == Long.MIN_VALUE || Math.abs(now - ts) > SyncAuth.DISCOVER_SKEW_MS
                    || !SyncAuth.verify(syncKey, m.get("sync_auth"), "discover", fileRepoName,
                    SyncAuth.addr(region, agent, pluginId), String.valueOf(ts))) {
                syncUnverified.incrementAndGet();
                return;
            }
        }
        sweepHolders(now);
        checkGenerationMarker(false);
        String nonce = SyncReceiver.token();
        synchronized (attempts) {
            Attempt last = attempts.get(key);
            if (last != null && now - last.at < subscribeRetryMs) return;
            attempts.put(key, new Attempt(now, nonce));
        }
        MsgEvent sub = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, region, agent, pluginId);
        sub.setParam("action", "reposubscribe");
        sub.setParam("filerepo_name", fileRepoName);
        sub.setParam("sync_gen", syncGen);
        sub.setParam("sync_proto", SYNC_PROTO);
        sub.setParam("sync_nonce", nonce);
        if (syncKey != null) {
            sub.setParam("sync_auth", SyncAuth.mac(syncKey, "reposubscribe", fileRepoName,
                    SyncAuth.addr(plugin.getRegion(), plugin.getAgent(), plugin.getPluginID()),
                    SyncAuth.addr(region, agent, pluginId), syncGen, nonce));
        }
        plugin.msgOut(sub);
    }

    /** At most one old-protocol WARN a minute, and at most once an hour per peer (remembered, bounded). */
    private void warnOldProtocol(String who, String message) {
        long now = System.currentTimeMillis();
        Long warned = oldProtocolWarned.get(who);
        if (warned != null && now - warned < 3_600_000L) return;
        oldProtocolWarned.put(who, now);
        if (now - lastOldProtocolWarn < 60_000L) return;
        lastOldProtocolWarn = now;
        logger.warn(message);
    }

    /**
     * Claims of producers not heard from in sync_holder_ttl_ms become orphans: they protect their files from every
     * delete, but no longer block another verified producer of this name from updating them. At most once an hour.
     */
    private void sweepHolders(long now) {
        if (now - lastHolderSweep < 3_600_000L) return;
        lastHolderSweep = now;
        orphanSilentProducers(now - holderTtlMs);
    }

    /** Orphan the claims of producers not heard from since {@code cutoff}; they are asked again at their next discover. */
    List<String> orphanSilentProducers(long cutoff) {
        List<String> stale = dbEngine.orphanStale(cutoff);
        for (String h : stale) {
            logger.warn("producer " + h + " not heard from in " + (holderTtlMs / 3_600_000L) + " h (sync_holder_ttl_ms):"
                    + " what it delivered here is kept, and may be updated by another verified producer of " + fileRepoName);
            for (Map.Entry<String, KnownProducer> e : new ArrayList<>(knownProducers.entrySet())) {
                if (e.getValue().holder().equals(h)) {
                    knownProducers.remove(e.getKey());
                    attempts.remove(e.getKey());
                }
            }
        }
        return stale;
    }

    /**
     * reposubscribed: a producer's answer, from its stamped caller; taken only from a producer this repository asked,
     * echoing the nonce of that ask, and with a sync_key only with its proof. The producer is then known by its
     * identity: the node its caller stamp names, this repository's name, and the directory it reports producing from.
     * Whether it is this mirror's producer is the pin's decision (sync_producers, or the first verified producer).
     */
    void onSubscribed(MsgEvent m) {
        String[] from = Callers.plugin(m, insecure);
        if (from == null) {
            logger.warn("reposubscribed ignored: no verified plugin caller");
            return;
        }
        String key = peerKey(from[0], from[1], from[2]);
        Attempt a = attempts.get(key);
        if (a == null) {
            logger.warn("reposubscribed from " + key + " ignored: this repository did not ask it");
            return;
        }
        if (!isMirror()) return;
        if (!("true".equals(m.getParam("accepted")) && fileRepoName != null && fileRepoName.equals(m.getParam("filerepo_name"))
                && SYNC_PROTO.equals(m.getParam("sync_proto")))) {
            knownProducers.remove(key);
            logger.info("producer " + key + " did not take this subscriber: " + m.getParam("refusal"));
            return;
        }
        String dir = m.getParam("producer_dir");
        String nonce = a.nonce;
        if (dir == null || !dir.startsWith("/") || dir.length() > 1000 || nonce == null || !nonce.equals(m.getParam("sync_nonce"))) {
            syncUnverified.incrementAndGet();
            logger.warn("reposubscribed from " + key + " ignored: no producer directory, or not the answer to this ask");
            return;
        }
        if (syncKey != null && !SyncAuth.verify(syncKey, m.getParam("sync_auth"), "reposubscribed", fileRepoName,
                SyncAuth.addr(from[0], from[1], from[2]),
                SyncAuth.addr(plugin.getRegion(), plugin.getAgent(), plugin.getPluginID()), nonce, dir, syncGen)) {
            syncUnverified.incrementAndGet();
            logger.warn("reposubscribed from " + key + " ignored: it does not hold this repository's sync_key");
            return;
        }
        a.nonce = null;   // one answer per ask
        // the producer's key, proven by its signature over this ask's nonce, both stamped addresses and this
        // generation. An answer without it (another plugin on the producer's agent claiming its directory, a key holder
        // replaying an answer, a filerepo of an earlier build) names no key: it is known, never pinned, and nothing it
        // offers or deletes is taken
        java.security.PublicKey pk = ProducerKey.publicKey(m.getParam("producer_pubkey"));
        boolean signed = pk != null && ProducerKey.verify(pk, m.getParam("producer_sig"), answerParts(fileRepoName,
                SyncAuth.addr(from[0], from[1], from[2]), SyncAuth.addr(plugin.getRegion(), plugin.getAgent(), plugin.getPluginID()),
                nonce, syncGen, dir, m.getParam("producer_epoch")));
        Identity id = new Identity(Callers.tenant(m, insecure), from[0], from[1], fileRepoName,
                signed ? ProducerKey.fingerprint(pk.getEncoded()) : null, dir);
        if (!signed) {
            syncUnverified.incrementAndGet();
            logger.warn("reposubscribed from " + key + " names no producer key (no valid P-384 signature over this ask):"
                    + " it is never this mirror's producer");
        }
        boolean pinned = signed && isPinnedProducer(id);
        if (!pinned) {
            unpinnedSeen.put(id.full(), System.currentTimeMillis());
            unpinnedIds.put(id.full(), id);
            warnUnpinned(id);
        }
        rememberProducer(from[0], from[1], from[2], id, pinned, signed ? pk : null);
        // what this mirror lost of that producer's files, asked for now that it answered
        if (pinned) requestRefills();
        if ("dropped-for-lag".equals(m.getParam("notice"))) {
            logger.error("producer " + key + " had dropped this mirror for lagging: it may hold files deleted at the"
                    + " producer since; clear it (clearrepo) to make it exact");
        }
    }

    /** At most once an hour per producer: a verified producer of this name that is not this mirror's pinned producer. */
    private void warnUnpinned(Identity id) {
        long now = System.currentTimeMillis();
        Long last = unpinnedWarned.get(id.full());
        if (last != null && now - last < 3_600_000L) return;
        unpinnedWarned.put(id.full(), now);
        String pins = pinnedProducers();
        logger.warn("producer " + id + " of " + fileRepoName + " is not this mirror's pinned producer ("
                + (pins == null ? "none is pinned: this mirror syncs nothing" : pins) + "): what it offers is refused, what"
                + " it deletes is ignored" + (id.key == null ? "" : ". An admin who trusts it pins it: syncpin confirm=<this"
                + " mirror's directory> producer=" + id.pin() + " (or sync_producers)"));
    }

    public void getFileRepoDiff(String repoDiffString, String transferId, String region, String agent, String pluginId) {
        acceptDiff(repoDiffString, null, transferId, syncGen, null, region, agent, pluginId);
    }

    /**
     * Take a producer's diff (FR-D5): only from a verified producer this repository subscribed to, at most
     * sync_pending_per_peer waiting per producer and sync_max_peers producers at once, on a bounded pool. Anything over
     * a bound is refused as busy, and the producer offers it again next cycle.
     */
    Accepted acceptDiff(String filesJson, String deletesJson, String transferId, String gen, String sig, String region,
                        String agent, String pluginId) {
        String repoId = peerKey(region, agent, pluginId);
        KnownProducer kp = knownProducer(region, agent, pluginId);
        if (kp == null) {
            syncRefused.incrementAndGet();
            logger.warn("diff " + transferId + " from " + repoId + " refused: not a verified producer this repository subscribed to");
            return new Accepted("9", "refused: not a verified producer this repository subscribed to");
        }
        // signed by the key that producer proved when it answered: what it offers and deletes, to this mirror, for this
        // generation, in this transfer. (A producer that proved no key is never pinned: its offers are refused whole
        // below. Only a test remembers a pinned producer without a key.)
        if (kp.key != null && !ProducerKey.verify(kp.key, sig, offerParts(fileRepoName, SyncAuth.addr(region, agent, pluginId),
                SyncAuth.addr(plugin.getRegion(), plugin.getAgent(), plugin.getPluginID()), transferId, gen, filesJson, deletesJson))) {
            syncRefused.incrementAndGet();
            syncUnverified.incrementAndGet();
            logger.warn("diff " + transferId + " from " + repoId + " refused: not signed by the producer key " + kp.id.key);
            return new Accepted("9", "refused: not signed by the producer's key");
        }
        String current = syncGen;
        if (gen == null || !gen.equals(current)) {
            // offered for an earlier generation (this repository was cleared since): it subscribes again with the new one
            attempts.remove(repoId);
            return new Accepted("9", "refused: offered for generation " + gen + ", this repository is at " + current);
        }
        kp.lastSeen = System.currentTimeMillis();
        // a verified producer that is not this mirror's pinned producer: taken, and answered with every file refused
        // and every delete ignored (its offers settle at it; nothing here changes)
        PendingDiff diff = new PendingDiff(transferId, filesJson, deletesJson, current, kp.pinned ? kp.holder() : null);
        synchronized (lockPeerUpdateQueueMap) {
            Deque<PendingDiff> q = peerUpdateQueueMap.get(repoId);
            if (q == null) {
                if (peerUpdateQueueMap.size() >= syncMaxPeers) {
                    syncRejected.incrementAndGet();
                    return new Accepted("11", "busy: " + peerUpdateQueueMap.size() + " producers syncing (sync_max_peers)");
                }
                q = new ArrayDeque<>();
                peerUpdateQueueMap.put(repoId, q);
            }
            if (q.size() >= syncPendingPerPeer) {
                syncRejected.incrementAndGet();
                return new Accepted("11", "busy: " + q.size() + " diffs from this producer waiting (sync_pending_per_peer)");
            }
            q.add(diff);
        }

        boolean startUpdater = false;
        synchronized (lockPeerUpdateStateMap) {
            // true: an updater IS being started; never a second one while the first drains
            if (!Boolean.TRUE.equals(peerUpdateStateMap.get(repoId))) {
                peerUpdateStateMap.put(repoId, true);
                startUpdater = true;
            }
        }
        if (startUpdater) {
            try {
                syncPool.execute(() -> runUpdater(repoId, region, agent, pluginId));
            } catch (RejectedExecutionException full) {
                synchronized (lockPeerUpdateStateMap) {
                    peerUpdateStateMap.put(repoId, false);
                }
                synchronized (lockPeerUpdateQueueMap) {
                    Deque<PendingDiff> q = peerUpdateQueueMap.get(repoId);
                    if (q != null) {
                        q.remove(diff);
                        if (q.isEmpty()) peerUpdateQueueMap.remove(repoId);
                    }
                }
                syncRejected.incrementAndGet();
                return new Accepted("11", "busy: the sync pool is full (sync_threads, sync_queue_max)");
            }
        }
        return new Accepted("10", "New transferID accepted");
    }

    private void runUpdater(String repoId, String region, String agent, String pluginId) {
        try {
            while (plugin.isActive()) {
                PendingDiff pending;
                synchronized (lockPeerUpdateStateMap) {
                    synchronized (lockPeerUpdateQueueMap) {
                        Deque<PendingDiff> q = peerUpdateQueueMap.get(repoId);
                        pending = q == null ? null : q.poll();
                        if (pending == null) {
                            // only go idle with nothing queued; drop the empty queue so the map stays bounded
                            peerUpdateQueueMap.remove(repoId);
                            peerUpdateStateMap.remove(repoId);
                            return;
                        }
                    }
                }
                applyDiff(pending, pending.holder, region, agent, pluginId);
            }
        } catch (Exception v) {
            logger.error("filerepo sync updater error", v);
        } finally {
            // never leave the state stuck 'active' after a crash or a stop, or it wedges forever
            synchronized (lockPeerUpdateStateMap) {
                if (Boolean.TRUE.equals(peerUpdateStateMap.get(repoId))) peerUpdateStateMap.put(repoId, false);
            }
        }
    }

    /** Where a name from a producer lands in this repository, and why it may not, if it may not. */
    private static final class Target {
        final Path path;
        final String why;

        Target(Path path, String why) {
            this.path = path;
            this.why = why;
        }
    }

    /**
     * The local path of {@code name} (a producer's relative name): contained in the repository directory, never a
     * staging name, never in a live-tier root, and never a case-only collision (on a case-insensitive filesystem,
     * "Model.bin" and "model.bin" are one file: a name whose existing file, or directory on the way, is spelled
     * differently is refused rather than written or deleted).
     */
    private Target target(Path repoBase, Path realBase, String name) {
        Path local = repoBase.resolve(name).normalize();
        if (!local.startsWith(repoBase) || local.equals(repoBase)) return new Target(local, "escapes the repository directory");
        for (Path part : repoBase.relativize(local)) {
            if (isStaging(part.toString())) return new Target(local, "a staging name");
        }
        String conflict = liveRootConflict(local);
        if (conflict != null) return new Target(local, "its destination " + conflict);
        // the deepest part of the path that exists must be spelled, as it exists, exactly as asked
        Path probe = local;
        while (probe != null && !probe.equals(repoBase) && !Files.exists(probe, LinkOption.NOFOLLOW_LINKS)) probe = probe.getParent();
        if (probe != null && !probe.equals(repoBase)) {
            try {
                Path real = probe.toRealPath(LinkOption.NOFOLLOW_LINKS);
                Path expected = realBase.resolve(repoBase.relativize(probe));
                if (!real.toString().equals(expected.toString())) {
                    return new Target(local, "a case-only (or linked) name collision: it exists here as "
                            + realBase.relativize(real));
                }
            } catch (IOException ex) {
                return new Target(local, "unresolvable: " + ex.getMessage());
            }
        }
        return new Target(local, null);
    }

    /** What to do with a producer's offer of a name. */
    private enum Take { HOLD, FETCH, REFUSE }

    private static final class Verdict {
        final Take take;
        final char kind;
        final String why;

        Verdict(Take take, char kind, String why) {
            this.take = take;
            this.kind = kind;
            this.why = why;
        }
    }

    /**
     * Whether producer {@code holder} may put content {@code md5} under {@code rel} here (call with the name lock held):
     * <ul>
     *   <li>never over a file landed here (putfiles, putjar), nor over anything that is not a regular file;</li>
     *   <li>the file here is that content already: held. The producer's claim is "fetched" when it is its own, or when no
     *   other live producer has a fetched claim; otherwise "matched": it protects the file from other producers'
     *   deletes, but never blocks the fetched claim's updates (holding needs bytes fetched from that producer);</li>
     *   <li>other content is replaced only when it is this producer's own earlier content and no other live producer has
     *   a fetched claim on it, or when it is orphaned content (its producer silent for sync_holder_ttl_ms) that no live
     *   producer claims;</li>
     *   <li>a missing file is fetched unless another live producer has a fetched claim on other content there.</li>
     * </ul>
     */
    private Verdict decide(Path localPath, String rel, String md5, String holder) {
        if (dbEngine.getFileInfo(localPath.toFile().getAbsolutePath()) != null) {
            return new Verdict(Take.REFUSE, ' ', "a file landed here holds that name");
        }
        Map<String, DBEngine.Holding> claims = dbEngine.holdings(rel);
        DBEngine.Holding mine = claims.remove(holder);
        // this producer's own claim, orphaned while it was silent: it (and only it) may act on that content again
        DBEngine.Holding myOrphan = claims.remove(DBEngine.ORPHAN + holder);
        if (mine == null) mine = myOrphan;
        boolean otherLive = false, otherLiveFetched = false, otherFetchedDifferent = false, otherOrphanDifferent = false;
        Set<String> otherOrphanMd5s = new HashSet<>();
        for (Map.Entry<String, DBEngine.Holding> e : claims.entrySet()) {
            if (e.getKey().startsWith(DBEngine.ORPHAN)) {
                // another producer's orphaned content: nobody but that producer may replace it
                otherOrphanMd5s.add(e.getValue().md5);
                if (!md5.equals(e.getValue().md5)) otherOrphanDifferent = true;
                continue;
            }
            otherLive = true;
            if (e.getValue().kind == DBEngine.HOLD_FETCHED) {
                otherLiveFetched = true;
                if (!md5.equals(e.getValue().md5)) otherFetchedDifferent = true;
            }
        }
        if (Files.exists(localPath, LinkOption.NOFOLLOW_LINKS)) {
            if (!Files.isRegularFile(localPath, LinkOption.NOFOLLOW_LINKS)) {
                return new Verdict(Take.REFUSE, ' ', "it is not a regular file here");
            }
            String held = Hashes.content(localPath);
            if (held == null) return new Verdict(Take.REFUSE, ' ', "the file here could not be read");
            if (held.equals(md5)) {
                boolean fetched = (mine != null && mine.kind == DBEngine.HOLD_FETCHED)
                        || (!otherLiveFetched && otherOrphanMd5s.isEmpty());
                return new Verdict(Take.HOLD, fetched ? DBEngine.HOLD_FETCHED : DBEngine.HOLD_MATCHED, null);
            }
            boolean ownEarlier = mine != null && held.equals(mine.md5) && !otherLiveFetched && !otherOrphanMd5s.contains(held);
            if (ownEarlier) return new Verdict(Take.FETCH, DBEngine.HOLD_FETCHED, null);
            return new Verdict(Take.REFUSE, ' ', otherOrphanMd5s.contains(held) ? "the file here is the orphaned content of a"
                    + " producer silent for sync_holder_ttl_ms: only it may replace it"
                    : otherLive ? "the file here is another producer's content (" + claims.keySet() + ")"
                    : "the file here holds other content, not this producer's");
        }
        if (otherFetchedDifferent || otherOrphanDifferent) {
            return new Verdict(Take.REFUSE, ' ', "another producer delivered other content under that name");
        }
        return new Verdict(Take.FETCH, DBEngine.HOLD_FETCHED, null);
    }

    /**
     * Apply a producer's diff: its deletes, then its files, each decided ({@link #decide}) and committed under the
     * shared side of the repository gate and the name lock, and only while this repository is still at the generation
     * the diff was accepted for: a clear (which renews the generation first, holding the gate) is never followed by a
     * file or a delete of a transfer from before it. The fetch itself runs outside both. Every outcome is confirmed
     * with that generation (a producer takes no confirm for an earlier one).
     */
    private void applyDiff(PendingDiff pending, String holder, String region, String agent, String pluginId) {
        Runnable hook = beforeApplyDiff;
        if (hook != null) hook.run();
        Map<String,FileObject> remoteRepoFiles = null;
        List<DeleteObject> remoteDeletes = null;
        String repoId = peerKey(region, agent, pluginId);
        try {
            remoteRepoFiles = pending.filesJson == null ? null : gson.fromJson(pending.filesJson, repoListType);
            remoteDeletes = pending.deletesJson == null ? null : gson.fromJson(pending.deletesJson, deleteListType);
        } catch (Exception bad) {
            logger.error("unparseable diff " + pending.transferId + " from " + repoId + ": " + bad.getMessage());
        }
        if (remoteRepoFiles == null) remoteRepoFiles = Collections.emptyMap();
        if (remoteDeletes == null) remoteDeletes = Collections.emptyList();

        logger.debug("UPDATING " + repoId + " transferid: " + pending.transferId);
        List<String> failedFiles = new ArrayList<>();
        List<String> failedDeletes = new ArrayList<>();
        if (holder == null) {
            // not this mirror's producer: no name here is its to add, change or delete
            List<String> refused = new ArrayList<>();
            for (FileObject f : remoteRepoFiles.values()) if (f != null && f.filePath != null) refused.add(f.filePath);
            syncUnpinned.addAndGet(refused.size() + remoteDeletes.size());
            MsgEvent c = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, region, agent, pluginId);
            c.setParam("action", "repoconfirm");
            c.setParam("transfer_id", pending.transferId);
            c.setParam("sync_proto", SYNC_PROTO);
            c.setParam("sync_gen", pending.gen);
            if (!refused.isEmpty()) c.setCompressedParam("refusedfiles", gson.toJson(refused));
            plugin.msgOut(c);
            return;
        }
        Path repoBase = getRepoDir().toPath().toAbsolutePath().normalize();
        Path realBase;
        try {
            realBase = repoBase.toRealPath();
        } catch (IOException ex) {
            realBase = repoBase;
        }

        for (DeleteObject d : remoteDeletes) {
            if (d == null || d.filePath == null) continue;
            if (!applyDelete(d, holder, pending.gen, repoBase, realBase)) failedDeletes.add(d.filePath);
        }

        SyncReceiver rx = null;
        try {
            for (Map.Entry<String, FileObject> diffEntry : remoteRepoFiles.entrySet()) {
                if (!plugin.isActive()) break;
                FileObject fileObject = diffEntry.getValue();

                // reject malformed peer entries (null fields) rather than let an NPE abort the whole batch
                if (fileObject == null || fileObject.fileName == null
                        || fileObject.MD5 == null || fileObject.filePath == null || fileObject.filesize < 0) {
                    logger.warn("skipping malformed peer diff entry: " + diffEntry.getKey());
                    if (fileObject != null && fileObject.filePath != null) {
                        failedFiles.add(fileObject.filePath);
                    }
                    continue;
                }
                String rel = fileObject.fileName;
                Target t = target(repoBase, realBase, rel);
                if (t.why != null) {
                    logger.warn("not syncing [" + rel + "] from " + repoId + ": " + t.why);
                    syncConflicts.incrementAndGet();
                    failedFiles.add(fileObject.filePath);
                    continue;
                }
                Path localPath = t.path;
                String md5 = fileObject.MD5;
                Verdict v = underGate(pending.gen, () -> {
                    Verdict now = decide(localPath, rel, md5, holder);
                    if (now.take == Take.HOLD) {
                        dbEngine.recordMirrored(holder, rel, md5, now.kind);
                        dbEngine.forgetMirrored(DBEngine.ORPHAN + holder, rel);
                    }
                    return now;
                });
                if (v == null) {
                    // cleared meanwhile, or the gate could not be had: nothing of this transfer is taken
                    failedFiles.add(fileObject.filePath);
                    continue;
                }
                if (v.take == Take.HOLD) continue;
                if (v.take == Take.REFUSE) {
                    logger.warn("not syncing [" + rel + "] from " + repoId + ": " + v.why);
                    syncConflicts.incrementAndGet();
                    failedFiles.add(fileObject.filePath);
                    continue;
                }

                // create parent dirs so nested (recursive) repo layouts sync, not just flat top-level files.
                try {
                    Path parent = localPath.getParent();
                    if (parent != null) Files.createDirectories(parent);
                } catch (Exception mkdirEx) {
                    logger.warn("could not create parent dir for " + localPath + ": " + mkdirEx.getMessage());
                    failedFiles.add(fileObject.filePath);
                    continue;
                }
                if (rx == null) {
                    rx = new SyncReceiver(plugin, logger, region, agent, pluginId, syncStreamBuffer, syncStreamIdleMs,
                            syncStreamRounds, syncRpcTimeoutMs).open();
                }
                // committed only if, at that moment, it is still this generation and the same verdict holds
                SyncReceiver.Committer commit = (part, dest) -> {
                    Verdict done = underGate(pending.gen, () -> {
                        Verdict again = decide(dest, rel, md5, holder);
                        if (again.take == Take.HOLD) {
                            dbEngine.recordMirrored(holder, rel, md5, again.kind);
                            dbEngine.forgetMirrored(DBEngine.ORPHAN + holder, rel);
                            return again;
                        }
                        if (again.take != Take.FETCH) return again;
                        Files.move(part, dest, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                        dbEngine.recordMirrored(holder, rel, md5, DBEngine.HOLD_FETCHED);
                        dbEngine.forgetMirrored(DBEngine.ORPHAN + holder, rel);
                        // claims on content that is no longer here go
                        dbEngine.forgetOtherClaims(rel, holder, md5);
                        return again;
                    });
                    return done != null && done.take == Take.FETCH;
                };
                SyncReceiver.Result r = rx.fetch(fileObject.filePath, fileObject.filesize, md5, localPath, commit);
                if (r.ok) {
                    syncFetched.incrementAndGet();
                    logger.debug("Synced " + localPath);
                } else if (Files.isRegularFile(localPath, LinkOption.NOFOLLOW_LINKS) && md5.equals(dbEngine.mirroredMd5(holder, rel))) {
                    // not committed because the same content got here meanwhile: held
                    logger.debug("held " + localPath);
                } else {
                    // one bad/slow file must not abort the batch: it is reported failed and offered again
                    syncFetchFailed.incrementAndGet();
                    if (r.verifyFailed) syncVerifyFailed.incrementAndGet();
                    logger.warn("sync of " + fileObject.filePath + " failed, will retry next cycle: " + r.why);
                    failedFiles.add(fileObject.filePath);
                }
            }
        } catch (Exception ex) {
            logger.error("sync of diff " + pending.transferId + " from " + repoId + " failed", ex);
            // everything not yet fetched is reported failed, so the producer offers it again
            for (FileObject f : remoteRepoFiles.values()) {
                if (f != null && f.filePath != null && !failedFiles.contains(f.filePath)) failedFiles.add(f.filePath);
            }
        } finally {
            if (rx != null) rx.close();
        }

        logger.debug("SENDING UPDATE " + repoId);
        MsgEvent filesConfirm = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC,region,agent,pluginId);
        filesConfirm.setParam("action", "repoconfirm");
        filesConfirm.setParam("transfer_id", pending.transferId);
        filesConfirm.setParam("sync_proto", SYNC_PROTO);
        // the generation this transfer was taken for: confirms from before a clear settle nothing
        filesConfirm.setParam("sync_gen", pending.gen);
        if (!failedFiles.isEmpty()) {
            filesConfirm.setCompressedParam("failedfiles", gson.toJson(failedFiles));
        }
        if (!failedDeletes.isEmpty()) {
            filesConfirm.setCompressedParam("faileddeletes", gson.toJson(failedDeletes));
        }
        plugin.msgOut(filesConfirm);
    }

    /** Work done under the shared side of the repository gate. */
    private interface GateWork<T> {
        T run() throws IOException;
    }

    /**
     * Run {@code work} with the shared side of the repository gate and the name lock held (in that order, as a
     * landing takes them), only while this repository is at generation {@code gen}. Null when the gate could not be
     * had or the generation changed (a clear): nothing was done.
     */
    private <T> T underGate(String gen, GateWork<T> work) throws IOException {
        if (!enterGate()) return null;
        try {
            synchronized (nameLock) {
                if (!gen.equals(syncGen)) return null;
                return work.run();
            }
        } finally {
            exitGate();
        }
    }

    /**
     * Apply a producer's delete: remove the local copy only if this producer's claim is exactly the deleted content,
     * no other producer (live or orphaned) still claims the name, it is not a file landed here, it is exactly the
     * deleted content, and it is not a case-only collision; never outside the repository directory or in a live-tier
     * root, and only while this repository is still at generation {@code gen}. When others still claim it, only this
     * producer's claim goes (and a matched claim on the content here becomes the fetched one). Anything else is left
     * alone and counts as applied (it is not that producer's to delete). False when the delete should be offered again.
     */
    private boolean applyDelete(DeleteObject d, String producer, String gen, Path repoBase, Path realBase) {
        if (d.fileName == null || d.MD5 == null) return false;
        Target t = target(repoBase, realBase, d.fileName);
        if (t.why != null) {
            logger.warn("not applying a delete of [" + d.fileName + "] from " + producer + ": " + t.why);
            return true;
        }
        Path localPath = t.path;
        try {
            Boolean done = underGate(gen, () -> deleteLocked(d, producer, localPath));
            return done != null && done;
        } catch (IOException ex) {
            logger.warn("could not delete " + localPath + ": " + ex.getMessage());
            return false;
        }
    }

    private boolean deleteLocked(DeleteObject d, String producer, Path localPath) throws IOException {
        Map<String, DBEngine.Holding> claims = dbEngine.holdings(d.fileName);
        DBEngine.Holding mine = claims.remove(producer);
        DBEngine.Holding myOrphan = claims.remove(DBEngine.ORPHAN + producer);
        if (mine == null && myOrphan != null && d.MD5.equals(myOrphan.md5)) {
            // content its producer delivered and then went silent for sync_holder_ttl_ms: deleting it needs an admin
            // release with this mirror's hold id; until then the delete is held (the producer offers it again)
            if (mirrorHeld.putIfAbsent(d.fileName, new String[]{d.MD5, producer}) == null) refreshMirrorHold();
            return false;
        }
        if (mine == null) {
            // this producer never delivered that name here: not its to delete
            return true;
        }
        if (!d.MD5.equals(mine.md5)) {
            // it delivered other content since: that is what it holds now
            return true;
        }
        if (!claims.isEmpty()) {
            // another producer still claims it here (or did, and is silent): only this producer's claim goes
            dbEngine.forgetMirrored(producer, d.fileName);
            boolean fetchedLeft = false;
            for (Map.Entry<String, DBEngine.Holding> e : claims.entrySet()) {
                if (!e.getKey().startsWith(DBEngine.ORPHAN) && e.getValue().kind == DBEngine.HOLD_FETCHED) fetchedLeft = true;
            }
            if (!fetchedLeft && Files.isRegularFile(localPath, LinkOption.NOFOLLOW_LINKS)) {
                String here = Hashes.content(localPath);
                for (Map.Entry<String, DBEngine.Holding> e : claims.entrySet()) {
                    if (!e.getKey().startsWith(DBEngine.ORPHAN) && e.getValue().md5 != null && e.getValue().md5.equals(here)) {
                        dbEngine.recordMirrored(e.getKey(), d.fileName, here, DBEngine.HOLD_FETCHED);
                    }
                }
            }
            return true;
        }
        if (!Files.exists(localPath, LinkOption.NOFOLLOW_LINKS)) {
            dbEngine.forgetMirrored(producer, d.fileName);
            return true;
        }
        if (!Files.isRegularFile(localPath, LinkOption.NOFOLLOW_LINKS)
                || dbEngine.getFileInfo(localPath.toFile().getAbsolutePath()) != null) {
            logger.warn("not deleting " + localPath + ": not a regular file, or landed here");
            dbEngine.forgetMirrored(producer, d.fileName);
            return true;
        }
        String held = Hashes.content(localPath);
        if (held == null) return false;
        if (!d.MD5.equals(held)) {
            logger.info("not deleting " + localPath + ": it holds other content than the producer deleted");
            dbEngine.forgetMirrored(producer, d.fileName);
            return true;
        }
        Files.delete(localPath);
        dbEngine.forgetMirrored(producer, d.fileName);
        syncDeleted.incrementAndGet();
        logger.debug("deleted " + localPath + " (deleted at the producer)");
        return true;
    }

    public Map<String,String> getFileInfo(String filePath) {
        return dbEngine.getFileInfo(filePath);
    }

    public String getFileRepoString(String repoName) {
        return getFileRepoString(repoName, 0, 0);
    }

    /** limit <= 0 returns the whole catalog; otherwise a page (Derby OFFSET/FETCH). */
    public String getFileRepoString(String repoName, int limit, int offset) {
        String repoString = null;
        try {
            List<Map<String,String>> repoFileList = dbEngine.getRepoList(limit, offset);
            repoString = gson.toJson(repoFileList);
        } catch (Exception ex) {
            logger.error("getFileRepoString: " + ex.getMessage());
        }
        return repoString;
    }

    public long getRepoCount() {
        return dbEngine.getRepoCount();
    }

    long getTombstoneCount() {
        return dbEngine.getTombstoneCount();
    }

    /** Why the catalog is not open (another plugin serves this directory, or it failed), or null. */
    String catalogOpenError() {
        return dbEngine.openError();
    }

    /** True if the Derby catalog is queryable (distinguishes a dead DB from an empty one). */
    public boolean isCatalogHealthy() {
        return dbEngine.isCatalogHealthy();
    }

    /** Diffs waiting for an updater, over every producer. */
    int getSyncQueued() {
        synchronized (lockPeerUpdateQueueMap) {
            int n = 0;
            for (Deque<PendingDiff> q : peerUpdateQueueMap.values()) n += q.size();
            return n;
        }
    }

    int getSyncActive() {
        return syncPool.getActiveCount();
    }

    int getSubscriberCount() {
        return subscribers.size();
    }

    int getKnownProducerCount() {
        return knownProducers.size();
    }

    /**
     * Content this mirror holds from producers that are not pinned now (re-pinned without adopt, a producer that moved
     * or lost its key, a pin by directory from an earlier build): frozen here, kept, never deleted, and changed by nobody
     * but that producer. repostate says so, by whom, and with the command that hands it to the pinned producer. A few
     * index probes per producer this mirror ever recorded, never a walk.
     */
    private void orphanedFacts(Map<String, String> facts) {
        Set<String> pinned = pinnedHolders();
        long total = 0;
        List<String> from = new ArrayList<>();
        String first = null;
        for (Map.Entry<String, String> e : dbEngine.holderIds().entrySet()) {
            String h = e.getKey();
            if (pinned.contains(h)) continue;
            long n = dbEngine.claimCount(h, 10_001);
            if (n == 0) {
                dbEngine.forgetHolderId(h);
                continue;
            }
            total += n;
            if (first == null) first = h;
            if (from.size() < 4) {
                from.add(h + " (" + (e.getValue() == null ? "a producer recorded by an earlier build" : e.getValue()) + ": "
                        + (n > 10_000 ? "over 10000" : String.valueOf(n)) + " file(s))");
            }
        }
        if (total == 0) return;
        String to = configuredProducers != null && !configuredProducers.isEmpty() ? configuredProducers.get(0).toString()
                : adminPin != null ? adminPin.pin() : "<region/agent#fingerprint of the producer to pin>";
        facts.put("orphaned_files", total > 10_000 ? "over 10000" : String.valueOf(total));
        facts.put("orphaned_from", String.join("; ", from));
        facts.put("orphaned_resolve", "frozen: kept, never deleted, changed by nobody else. To hand it to the pinned"
                + " producer: syncpin confirm=" + repoDirFile().getAbsolutePath() + " adopt=true from=" + first
                + " producer=" + to);
    }

    /**
     * MIRROR or ONLY_COPY, from configuration and the catalog: a subscriber mirror is MIRROR until filerepo lands
     * files in it directly (putfiles, putjar), and every other repository is the origin of what it holds. Costs one
     * catalog lookup and one index probe, whatever the repository's size; never a walk. A subscriber's synced files are
     * not cataloged, so cataloged rows there are files that were landed.
     */
    RepoState localState() {
        Map<String, String> facts = new LinkedHashMap<>();
        facts.put("backing", "local");
        if (!subscriber) {
            facts.put("role", fileScanTimer != null ? "producer" : "on-demand");
            if (isProducer()) {
                // what an admin pins on each mirror (sync_producers, or syncpin producer=...)
                String pin = producerPin();
                if (pin != null) {
                    facts.put("producer_key", signingFingerprint());
                    facts.put("producer_pin", pin);
                } else {
                    facts.put("producer_key_error", String.valueOf(signingKeyError));
                }
            }
            if (getDeletesHeld() > 0) {
                facts.put("deletes_held", String.valueOf(getDeletesHeld()));
                facts.put("deletes_hold_id", String.valueOf(getHoldId()));
            }
            if (getRewritesHeld() > 0) {
                facts.put("rewrites_held", String.valueOf(getRewritesHeld()));
                facts.put("rewrites_hold_id", String.valueOf(getRewriteHoldId()));
            }
            int lagging = getLaggingSubscribers();
            if (lagging > 0) facts.put("subscribers_lagging", String.valueOf(lagging));
            return RepoState.of(RepoState.ONLY_COPY, facts,
                    "the origin of what it holds: no producer refills it");
        }
        facts.put("role", "subscriber");
        String pin = pinnedProducers();
        facts.put("pinned_producer", pin == null ? "none: this mirror syncs nothing until an admin pins its producer"
                + " (sync_producers=region/agent#<fingerprint>, or syncpin producer=...)" : pin);
        if (!unpinnedSeen.isEmpty()) facts.put("unpinned_producers", String.valueOf(unpinnedSeen.size()));
        // the producers that answered and proved a key, most recent first: what an admin may pin
        List<String> candidates = new ArrayList<>();
        synchronized (unpinnedIds) {
            List<Identity> ids = new ArrayList<>(unpinnedIds.values());
            Collections.reverse(ids);
            for (Identity id : ids) {
                if (id.key != null && candidates.size() < 8 && !candidates.contains(id.toString())) candidates.add(id.toString());
            }
        }
        if (!candidates.isEmpty()) facts.put("candidate_producers", String.join(", ", candidates));
        orphanedFacts(facts);
        String mirrorHold = mirrorHoldId;
        int mirrorHolding = mirrorHeldCount;
        if (mirrorHolding > 0 && mirrorHold != null) {
            facts.put("deletes_held", String.valueOf(mirrorHolding));
            facts.put("deletes_hold_id", mirrorHold);
        }
        boolean hasLanded = isLanded() || dbEngine.hasFiles();
        facts.put("landed", String.valueOf(hasLanded));
        if (hasLanded) {
            return RepoState.of(RepoState.ONLY_COPY, facts,
                    "a mirror that files were landed in directly (putfiles, putjar): its producer does not hold them");
        }
        return RepoState.of(RepoState.MIRROR, facts, "a mirror of its producer: what it holds, the producer holds");
    }

    /** What a clear did. */
    static final class Clear {
        static final String CLEARED = "cleared", ONLY_COPY = "only-copy", BUSY = "busy", FAILED = "failed",
                UNCONFIRMED = "unconfirmed", PROTECTED = "protected-dir", LIVE_ROOT = "live-root";
        final String outcome;
        // the state the clear decided on, under the gate; null when the gate was not had
        final RepoState state;
        // files under the repository directory the delete could not remove
        final long remaining;
        final String why;

        Clear(String outcome, RepoState state, long remaining, String why) {
            this.outcome = outcome;
            this.state = state;
            this.remaining = remaining;
            this.why = why;
        }
    }

    /**
     * True when {@code confirm} names this repository's directory exactly: as configured (scan_dir, else repo_dir) or
     * as its absolute path.
     */
    boolean confirms(String confirm) {
        if (confirm == null || confirm.isEmpty()) return false;
        String configured = scanDirString != null ? scanDirString : repoDir;
        if (confirm.equals(configured)) return true;
        File dir = repoDirFile();
        if (confirm.equals(dir.getAbsolutePath())) return true;
        try {
            return confirm.equals(dir.getCanonicalPath());
        } catch (IOException ex) {
            return false;
        }
    }

    /** Directories a clear never wipes: equal to one of these, it is refused. */
    private static final Set<String> SYSTEM_DIRS = new HashSet<>(Arrays.asList(
            "/etc", "/usr", "/bin", "/sbin", "/lib", "/lib32", "/lib64", "/libx32", "/var", "/opt", "/home", "/Users",
            "/root", "/tmp", "/private", "/private/tmp", "/private/var", "/private/etc", "/System", "/Library",
            "/Applications", "/Volumes", "/dev", "/proc", "/sys", "/boot", "/mnt", "/media", "/srv", "/run", "/snap",
            "/usr/local", "/var/tmp", "/var/lib", "/var/log"));

    /**
     * Why {@code dir} may never be wiped, or null: it is the filesystem root, a system directory, or contains (or is)
     * the home directory, the agent's working directory, its data directory, this plugin's data directory, or the JVM.
     */
    String protectedDir(File dir) {
        try {
            Path p = dir.getCanonicalFile().toPath();
            if (p.getNameCount() == 0) return "it is the filesystem root";
            if (SYSTEM_DIRS.contains(p.toString())) return "it is a system directory";
            Map<String, String> own = new LinkedHashMap<>();
            own.put("the home directory", System.getProperty("user.home"));
            own.put("the agent's working directory", System.getProperty("user.dir"));
            own.put("the JVM", System.getProperty("java.home"));
            try {
                own.put("the agent's data directory", plugin.getAgentService().getAgentDataDirectory());
            } catch (Exception ignore) { }
            try {
                own.put("this plugin's data directory", plugin.getPluginDataDirectory());
            } catch (Exception ignore) { }
            try {
                own.put("the repository catalogs", RepoPaths.agentData(plugin).resolve("filerepo").resolve("state").toString());
            } catch (Exception ignore) { }
            for (Map.Entry<String, String> e : own.entrySet()) {
                if (e.getValue() == null || e.getValue().isEmpty()) continue;
                Path q = new File(e.getValue()).getCanonicalFile().toPath();
                if (q.startsWith(p)) return "it is or contains " + e.getKey();
            }
            return null;
        } catch (IOException ex) {
            return "its canonical path cannot be resolved (" + ex.getMessage() + ")";
        }
    }

    /** {@link #clearRepo(String, boolean)} with this repository's own directory as the confirmation (tests, internal). */
    public Clear clearRepo(boolean force) {
        return clearRepo(repoDirFile().getAbsolutePath(), force);
    }

    /**
     * Wipe the repository: every cataloged file and every regular file under the repository directory (FR-D2). It must
     * be confirmed with the directory's exact name, is never done to a protected directory or one that is or holds a
     * live-tier root (force or not), and is refused for an only copy unless {@code force}. Correct for a mirror, whose
     * producer holds the same files: a new sync generation is started before the first file goes, so whatever is
     * deleted, even by a clear that fails partway, is offered again. The repository gate is taken exclusively BEFORE the
     * only-copy check and held through the delete, and every landing takes its shared side, so no file filerepo lands
     * can arrive between the two.
     */
    public Clear clearRepo(String confirm, boolean force) {
        if (!confirms(confirm)) {
            return new Clear(Clear.UNCONFIRMED, null, 0,
                    "confirm must name this repository's directory exactly (its scan_dir, or repo_dir when it has none)");
        }
        File dir = repoDirFile();
        String protectedWhy = protectedDir(dir);
        if (protectedWhy != null) {
            logger.error("clearrepo refused for " + dir + ": " + protectedWhy);
            return new Clear(Clear.PROTECTED, null, 0, "the repository directory is protected: " + protectedWhy);
        }
        String live = repoDirConflict();
        if (live != null) {
            return new Clear(Clear.LIVE_ROOT, null, 0, "this repository's directory " + live);
        }
        boolean acquired = false;
        try {
            // a bounded wait for the gate: a running scan or landing finishes first
            acquired = gate.writeLock().tryLock(gateWaitMs, TimeUnit.MILLISECONDS);
            if (!acquired) {
                logger.warn("clearRepo: timed out waiting for the repository gate");
                return new Clear(Clear.BUSY, null, 0, "the repository gate was held by a scan or a landing for "
                        + gateWaitMs + " ms");
            }

            RepoState state = localState();
            if (RepoState.ONLY_COPY.equals(state.state) && !force) {
                logger.warn("clearrepo refused: " + state.desc + "; force=true clears it anyway");
                return new Clear(Clear.ONLY_COPY, state, 0, state.desc);
            }

            // decided: before anything else, a new generation, so a mirror is refilled whatever happens next (even a
            // clear that fails partway), and the next discover subscribes again at once
            syncGen = newGen();
            attempts.clear();
            writeGenerationMarker();

            Runnable hook = afterClearCheck;
            if (hook != null) hook.run();

            Path base = dir.getCanonicalFile().toPath();
            //List all files recorded in repo and remove them: only inside the repository directory, and never one in a
            //live-tier root (only a drop removes those); a stale row from elsewhere is forgotten, its file left alone
            List<Map<String,String>> repoFileList = dbEngine.getRepoList();
            for(Map<String,String> filerecord : repoFileList) {
                Path recorded = Paths.get(filerecord.get("filepath"));
                dbEngine.deleteFile(filerecord.get("filepath"));
                if (liveRootConflict(recorded) != null) continue;
                try {
                    Path parent = recorded.toAbsolutePath().getParent();
                    Path real = parent == null ? null : parent.toFile().getCanonicalFile().toPath();
                    if (real != null && real.startsWith(base)) {
                        Files.deleteIfExists(recorded);
                    }
                } catch (IOException ex) {
                    logger.warn("clearRepo: " + recorded + " not deleted: " + ex.getMessage());
                }
            }
            //delete any files not recorded in repo dir; links are removed, never followed; an unreadable directory is
            //counted and skipped, never a reason to stop halfway
            final long[] remaining = {0};
            if (dir.isDirectory()) {
                Files.walkFileTree(dir.toPath(), new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path p, BasicFileAttributes attrs) {
                        if (attrs.isRegularFile() || attrs.isSymbolicLink()) {
                            try {
                                Files.delete(p);
                            } catch (IOException ex) {
                                remaining[0]++;
                            }
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFileFailed(Path p, IOException exc) {
                        remaining[0]++;
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
            dbEngine.clearMirrored();
            if (remaining[0] == 0) {
                // nothing landed is left: a mirror is a mirror again
                clearLanded();
            } else {
                logger.warn("clearRepo: " + remaining[0] + " file(s) could not be deleted");
            }
            return new Clear(Clear.CLEARED, state, remaining[0], remaining[0] == 0 ? "cleared"
                    : remaining[0] + " file(s) could not be deleted");

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return new Clear(Clear.FAILED, null, 0, "interrupted");
        } catch (Exception ex) {
            logger.error("clearRepo: " + ex.getMessage());
            return new Clear(Clear.FAILED, null, 0, String.valueOf(ex.getMessage()));
        } finally {
            // ALWAYS release the gate, even on exception, or every later scan, landing and clear waits on it
            if (acquired) gate.writeLock().unlock();
        }
    }

    /**
     * Take the shared side of the repository gate for a landing (putfiles, putjar): it waits, bounded, while a clear
     * holds the gate. False when the gate could not be had, and nothing may be landed; otherwise call exitGate().
     */
    boolean enterGate() {
        try {
            if (gate.readLock().tryLock(gateWaitMs, TimeUnit.MILLISECONDS)) return true;
            logger.warn("landing refused: a clear held the repository gate for " + gateWaitMs + " ms");
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    void exitGate() {
        gate.readLock().unlock();
    }

    /**
     * Record, before the first byte lands, that filerepo landed files here directly. In memory first, so it holds even
     * if the catalog write fails.
     */
    void markLanded() {
        if (Boolean.TRUE.equals(landed)) return;
        synchronized (landedLock) {
            if (Boolean.TRUE.equals(landed)) return;
            landed = Boolean.TRUE;
            try {
                dbEngine.setMeta(META_LANDED, "true");
            } catch (Exception ex) {
                logger.error("could not record a landing in the catalog: " + ex.getMessage());
            }
        }
    }

    private boolean isLanded() {
        Boolean l = landed;
        if (l != null) return l;
        synchronized (landedLock) {
            if (landed != null) return landed;
            try {
                landed = "true".equals(dbEngine.getMeta(META_LANDED));
                return landed;
            } catch (Exception ex) {
                // fail closed (and not cached, so a later read can recover)
                logger.error("landing record unreadable, counted as landed: " + ex.getMessage());
                return true;
            }
        }
    }

    private void clearLanded() {
        synchronized (landedLock) {
            try {
                dbEngine.deleteMeta(META_LANDED);
                landed = Boolean.FALSE;
            } catch (Exception ex) {
                // keep counting it landed: the record still says so
                logger.error("could not clear the landing record: " + ex.getMessage());
            }
        }
    }

    /**
     * Remove one file (file and catalog row), contained to the repository directory and never in a live-tier root. On
     * a producer the removal is recorded as a delete for the subscribers (FR-D4: removefile used to leave every mirror
     * holding the file). Takes the shared side of the repository gate, as a landing does.
     */
    public Boolean removeFile(String fileRepoName, String fileName) {
        if (fileName == null || fileName.isEmpty()) return false;
        if (!enterGate()) return false;
        try {
            File repoRoot = getRepoDir();
            Path base = repoRoot.getCanonicalFile().toPath();
            Path target = repoRoot.toPath().resolve(fileName).normalize();
            Path parentReal = target.toAbsolutePath().getParent() == null ? null
                    : target.toAbsolutePath().getParent().toFile().getCanonicalFile().toPath();
            // path-traversal guard: the resolved target (its directory resolved as the filesystem does) must stay inside
            if (parentReal == null || !parentReal.startsWith(base) || target.getFileName() == null
                    || parentReal.resolve(target.getFileName()).equals(base)) {
                logger.error("path traversal blocked in removeFile: " + fileName);
                return false;
            }
            Path real = parentReal.resolve(target.getFileName());
            if (liveRootConflict(real) != null) {
                logger.error("removeFile refused: " + fileName + " lies in a live-tier root");
                return false;
            }
            if (!Files.isRegularFile(real, LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(real)) {
                return false;
            }
            // the catalog keys a file by the absolute path it was found under
            String abs = real.toFile().getAbsolutePath();
            String cataloged = dbEngine.getFileInfo(abs) != null ? abs
                    : dbEngine.getFileInfo(target.toAbsolutePath().toString()) != null ? target.toAbsolutePath().toString() : null;
            Map<String, String> info = cataloged == null ? null : dbEngine.getFileInfo(cataloged);
            Files.delete(real);
            // a mirror's record of what a producer delivered under that name goes with it
            for (String producer : dbEngine.mirroredBy(fileName).keySet()) dbEngine.forgetMirrored(producer, fileName);
            if (cataloged != null) {
                if (fileScanTimer != null && scanDirString != null && info != null) {
                    Path root = Paths.get(scanDirString).toAbsolutePath();
                    Path p = Paths.get(cataloged);
                    String name = p.startsWith(root) ? root.relativize(p).toString() : p.getFileName().toString();
                    tombstonesRecorded.addAndGet(dbEngine.tombstone(Collections.singletonList(
                            new DBEngine.Tombstone(cataloged, name, info.get("md5"), System.currentTimeMillis())), 1));
                    dbEngine.markDeleted(Collections.singletonList(cataloged), 1);
                } else {
                    dbEngine.deleteFile(cataloged);
                }
            }
            return true;
        } catch (Exception ex) {
            logger.error("removeFile: " + ex.getMessage());
            return false;
        } finally {
            exitGate();
        }
    }

    /** What a putfiles landed: by file name. */
    static final class Put {
        final List<String> stored = new ArrayList<>();
        final List<String> unchanged = new ArrayList<>();
        final List<String> skipped = new ArrayList<>();
        final Map<String, String> failed = new LinkedHashMap<>();
        boolean busy;

        boolean allStored() {
            return !busy && failed.isEmpty() && skipped.isEmpty();
        }

        String summary() {
            return stored.size() + " stored, " + unchanged.size() + " unchanged, " + skipped.size()
                    + " skipped (exist; overwrite=false), " + failed.size() + " failed";
        }
    }

    public Boolean putFiles(List<String> fileList, String repoName, boolean overwrite, boolean isLocal) {
        Map<String, String> hashes = new HashMap<>();
        return putFiles(fileList, hashes, overwrite, false).allStored();
    }

    /**
     * Land pushed files (FR-D10). Each source is COPIED into the owner-only staging directory, its content hash checked
     * against {@code expected} (required unless putfiles_require_hashes=false), and moved into place atomically, then
     * checked once more in place: a partial, corrupt or swapped file is never left under its name. The source is never
     * moved: it is deleted only after it landed, and only when it is a transfer file in the agent's dataplane journal
     * (a fact checked here, never one the message states). A file that exists and is not to be overwritten is reported
     * unchanged when it holds the expected content and skipped when it does not: never counted stored.
     */
    Put putFiles(List<String> fileList, Map<String, String> expected, boolean overwrite, boolean requireHashes) {
        Put put = new Put();
        // the shared side of the repository gate: nothing lands between a clear's check and its delete
        if (!enterGate()) {
            put.busy = true;
            return put;
        }
        try {
            Path repo = getRepoDir().toPath().toAbsolutePath();
            Path journal = journalDir();
            for (String incomingFileName : fileList) {
                Path src = Paths.get(incomingFileName);
                String name = src.getFileName() == null ? null : src.getFileName().toString();
                if (name == null || isStaging(name)) {
                    put.failed.put(String.valueOf(name), "not a file name filerepo lands");
                    continue;
                }
                String want = expected == null ? null : expected.get(name);
                if (want == null && requireHashes && requirePutHashes) {
                    put.failed.put(name, "no expected content hash (file_hashes)");
                    continue;
                }
                logger.info("incoming file: " + src);
                Path dest = repo.resolve(name);
                if (Files.exists(dest, LinkOption.NOFOLLOW_LINKS) && !overwrite) {
                    String held = Files.isRegularFile(dest, LinkOption.NOFOLLOW_LINKS) ? Hashes.content(dest) : null;
                    if (want != null && want.equals(held)) put.unchanged.add(name);
                    else put.skipped.add(name);
                    continue;
                }
                Path part = null;
                try {
                    part = privateStaging().resolve(STAGING_PREFIX + UUID.randomUUID().toString().replace("-", "") + STAGING_SUFFIX);
                    Files.copy(src, part);
                    String got = Hashes.content(part);
                    if (got == null || (want != null && !want.equals(got))) {
                        put.failed.put(name, "content hash mismatch");
                        continue;
                    }
                    // before the first byte lands: from here on, this repository holds files only it has
                    markLanded();
                    // one name at a time: a sync commit of the same name never interleaves with this landing
                    synchronized (nameLock) {
                        Files.move(part, dest, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                        part = null;
                        if (!got.equals(Hashes.content(dest))) {
                            Files.deleteIfExists(dest);
                            put.failed.put(name, "the file changed while it was moved into place");
                            continue;
                        }
                        // landed content is this repository's own, no producer's
                        for (String producer : dbEngine.mirroredBy(name).keySet()) dbEngine.forgetMirrored(producer, name);
                        // Catalog the landed file when no scanner will pick it up (landing-only repos never schedule
                        // the scan timer); a replaced file gets its new content recorded, not a stale row
                        if (fileScanTimer == null) {
                            File saved = dest.toFile();
                            dbEngine.upsertFile(saved.getAbsolutePath(), got, saved.lastModified(), saved.length());
                        }
                    }
                    put.stored.add(name);
                    // a transfer file of the agent's journal has done its job; anything else is the caller's and stays
                    if (journal != null) {
                        try {
                            Path realSrc = src.toRealPath();
                            if (realSrc.startsWith(journal)) Files.deleteIfExists(realSrc);
                        } catch (IOException ignore) { }
                    }
                } catch (Exception ex) {
                    logger.error("putfiles: " + name + " not landed", ex);
                    put.failed.put(name, ex.getClass().getSimpleName());
                } finally {
                    if (part != null) {
                        try { Files.deleteIfExists(part); } catch (Exception ignore) { }
                    }
                }
            }
        } finally {
            exitGate();
        }
        return put;
    }

    /** The agent's dataplane journal (where pushed attachments are reassembled), resolved, or null. */
    private Path journalDir() {
        try {
            Path j = plugin.getAgentService().getDataPlaneService().getJournalPath();
            return j == null ? null : j.toRealPath();
        } catch (Exception ex) {
            return null;
        }
    }


    //sub functions

    /** Handle a subscriber-side dataplane message (a producer's discover broadcast). */
    void onSubscriberMessage(Map<String, String> incomingMap) {
        if (incomingMap == null || !incomingMap.containsKey("action")) {
            logger.error("createSubListener no action in message");
            return;
        }
        if (!incomingMap.containsKey("filerepo_name")) {
            logger.error("action called without filerepo_name");
            return;
        }
        if ("discover".equals(incomingMap.get("action"))) {
            if (fileRepoName != null && fileRepoName.equals(incomingMap.get("filerepo_name"))) onDiscover(incomingMap);
        } else {
            logger.error("unknown actionType: " + incomingMap.get("action"));
        }
    }

    private void createSubListener(String filerepoName) {

        MessageListener ml = new MessageListener() {
            public void onMessage(Message msg) {
                try {
                    if (msg instanceof TextMessage) {
                        logger.debug(" SUB REC MESSAGE:" + ((TextMessage) msg).getText());
                        onSubscriberMessage(gson.fromJson(((TextMessage) msg).getText(), mapType));
                    }
                } catch(Exception ex) {
                    logger.error("filerepo error", ex);
                }
            }
        };

        String queryString = "filerepo_name='" + filerepoName + "' AND broadcast";
        String node_from_listner_id = plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.GLOBAL,ml,queryString);

        listenerList.add(node_from_listner_id);

    }

    //repo functions
    public void repoBroadcast(String filerepoName, String action, int transferId) {

        try {

            Map<String,String> update = new HashMap<>();
            update.put("action",action);
            update.put("filerepo_name",filerepoName);
            update.put("repo_region_id", plugin.getRegion());
            update.put("repo_agent_id",plugin.getAgent());
            update.put("repo_plugin_id",plugin.getPluginID());
            update.put("transfer_id", String.valueOf(transferId));
            update.put("sync_proto", SYNC_PROTO);
            if (syncKey != null) {
                // a proof of the repository's sync_key, bound to this producer's address and the time: mirrors ignore
                // a discover without one, so a forged discover costs them nothing
                String ts = String.valueOf(System.currentTimeMillis());
                update.put("sync_ts", ts);
                update.put("sync_auth", SyncAuth.mac(syncKey, "discover", filerepoName,
                        SyncAuth.addr(plugin.getRegion(), plugin.getAgent(), plugin.getPluginID()), ts));
            }

            TextMessage updateMessage = plugin.getAgentService().getDataPlaneService().createTextMessage();
            updateMessage.setText(gson.toJson(update));
            updateMessage.setStringProperty("filerepo_name",filerepoName);
            updateMessage.setBooleanProperty("broadcast",Boolean.TRUE);

            plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL,updateMessage);
            logger.debug("SENDING MESSAGE: " + update);

        } catch (Exception ex) {
            logger.error("failed to update subscribers");
            logger.error(ex.getMessage());
        }

    }

    /**
     * A producer-side dataplane message: only sync protocol 1 subscribers send these (subscribe and unsubscribe over the
     * dataplane, where anyone can write them). They are never taken: the subscriber is told, in the log, to upgrade.
     */
    void onRepoMessage(Map<String, String> incomingMap) {
        if (incomingMap == null) return;
        String who = incomingMap.get("sub_region_id") + "/" + incomingMap.get("sub_agent_id") + "/" + incomingMap.get("sub_plugin_id");
        warnOldProtocol(who, "ignored a sync protocol 1 '" + incomingMap.get("action") + "' naming " + who
                + " over the dataplane: subscribers of protocol " + SYNC_PROTO + " subscribe with reposubscribe; upgrade it");
    }

    private void createRepoSubListener(String filerepoName) {

        MessageListener ml = new MessageListener() {
            public void onMessage(Message msg) {
                try {
                    if (msg instanceof TextMessage) {
                        onRepoMessage(gson.fromJson(((TextMessage) msg).getText(), mapType));
                    }
                } catch(Exception ex) {
                    logger.error("filerepo error", ex);
                }
            }
        };

        String queryString = "filerepo_name='" + filerepoName + "' AND region_id='" + plugin.getRegion() + "' AND agent_id='" + plugin.getAgent() + "' AND plugin_id='" + plugin.getPluginID() + "'";
        String node_from_listner_id = plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.GLOBAL,ml,queryString);

        listenerList.add(node_from_listner_id);

    }

    //utils
    /**
     * The repository directory as configured, without creating it: scan_dir, else repo_dir, else the default
     * {@code <agent data dir>/filerepo/repos/<filerepo_name or "default">} (outside the plugin's data directory, which
     * the controller deletes with the plugin). See {@link RepoPaths}.
     */
    File repoDirFile() {
        return RepoPaths.repoDir(plugin).toFile();
    }

    /** The repository directory ({@link #repoDirFile()}), created if absent. One answer for every verb. */
    public File getRepoDir() {
        File dir = repoDirFile();
        try {
            if (!dir.isDirectory() && !dir.mkdirs() && !dir.isDirectory()) {
                logger.error("could not create the repository directory " + dir);
            }
        } catch (Exception ex) {
            logger.error("filerepo error", ex);
        }
        return dir;
    }


}
