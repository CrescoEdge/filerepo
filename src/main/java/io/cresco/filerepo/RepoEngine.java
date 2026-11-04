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
    private final boolean insecure = Callers.insecureMode();

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
    private final Map<String, Long> oldProtocolWarned = new ConcurrentHashMap<>();

    /** A subscriber as the producer serves it. */
    static final class ActiveSub {
        final String key, region, agent, plugin;
        volatile String gen;
        volatile long lastSeen;
        // false after an offer went unanswered: skipped until it subscribes again
        volatile boolean reachable = true;

        ActiveSub(String key, String region, String agent, String plugin, String gen, long lastSeen) {
            this.key = key;
            this.region = region;
            this.agent = agent;
            this.plugin = plugin;
            this.gen = gen;
            this.lastSeen = lastSeen;
        }
    }

    // ---- subscriber side ----
    // this repository's sync generation: new when it starts and when a clear starts; a producer that has not served it
    // offers it everything again (paced), and it skips what it holds
    private volatile String syncGen = SyncReceiver.token();
    // producers an admin pinned (sync_producers: region/agent/plugin, comma-separated); null: any that verifies
    private final Set<String> configuredProducers;
    // producers this repository subscribed to and that answered, verified by their stamped caller, as producers of its
    // filerepo_name speaking this sync protocol: producer key -> when last confirmed
    private final Map<String, Long> knownProducers = new ConcurrentHashMap<>();
    // subscribe attempts: producer key -> when sent (bounded); an answer is taken only from an attempted producer
    private final Map<String, Long> attempts = Collections.synchronizedMap(new LinkedHashMap<String, Long>() {
        protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
            return size() > 1024;
        }
    });
    // subscribe attempts allowed: a burst, then a steady rate (a flood of forged discovers sends no flood of messages)
    private double attemptTokens = 512;
    private long attemptRefilled = System.currentTimeMillis();

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
    final AtomicLong tombstonesRecorded = new AtomicLong();

    public RepoEngine(PluginBuilder pluginBuilder, DBEngine dbEngine) {
        this(pluginBuilder, dbEngine, null);
    }

    public RepoEngine(PluginBuilder pluginBuilder, DBEngine dbEngine, LiveTierClient liveTier) {

        this.plugin = pluginBuilder;
        logger = plugin.getLogger(RepoEngine.class.getName(), CLogger.Level.Info);
        this.dbEngine = dbEngine;
        this.liveTier = liveTier;
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
        double fraction = 0.10;
        try {
            Double d = plugin.getConfig().getDoubleParam("sync_delete_guard_fraction");
            if (d != null && d > 0 && d <= 1) fraction = d;
        } catch (Exception ignore) { }
        guard = new DeleteGuard(fraction, (int) clamp(longParam("sync_delete_guard_subtree_min", 2L), 1, Integer.MAX_VALUE),
                clamp(longParam("sync_release_ttl_ms", 600_000L), 1_000L, Long.MAX_VALUE));
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

        String pinned = plugin.getConfig().getStringParam("sync_producers");
        if (pinned != null && !pinned.trim().isEmpty()) {
            Set<String> s = new HashSet<>();
            for (String p : pinned.split(",")) {
                String[] parts = p.trim().split("/");
                if (parts.length == 3) s.add(peerKey(parts[0], parts[1], parts[2]));
                else logger.error("sync_producers: '" + p.trim() + "' is not region/agent/pluginid; ignored");
            }
            configuredProducers = Collections.unmodifiableSet(s);
        } else {
            configuredProducers = null;
        }

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
        }

        if (liveRow == null && scanDirString == null && repoDir == null) {
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

    /** True when this repository is a producer (scan_dir and filerepo_name, not live-backed). */
    boolean isProducer() {
        return liveRow == null && scanDirString != null && fileRepoName != null;
    }

    public void start() {

        if (liveRow != null) {
            // read-only and recreatable: nothing here is scanned, broadcast, offered or subscribed to
            logger.info("live-backed repository (row " + liveRow + "): no scan, no sync");
            return;
        }

        long delay =  plugin.getConfig().getLongParam("scan_delay", 5000L);
        long period =  plugin.getConfig().getLongParam("scan_period", 15000L);

        if((scanDirString != null) && (fileRepoName != null)) {
            logger.info("Starting file scan : " + scanDirString + " filerepo: " + fileRepoName);
            startScan(delay, period);
        } else if((scanDirString == null) && (fileRepoName != null) && (repoDir != null)) {
            logger.info("Start listening for filerepo: " + fileRepoName
                    + (configuredProducers == null ? "" : " (producers pinned: " + configuredProducers + ")"));
            createSubListener(fileRepoName);
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
        try { offerPool.shutdownNow(); } catch (Exception ex) { logger.error("offerPool shutdown error", ex); }

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

    /** What one offer (a page) carried, to whom: settled by that subscriber's confirm. */
    static final class Offer {
        final String subscriber;
        final Map<String, FileObject> files;
        final List<DeleteObject> deletes;

        Offer(String subscriber, Map<String, FileObject> files, List<DeleteObject> deletes) {
            this.subscriber = subscriber;
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

        Map<String,FileObject> fileDiffMap = new LinkedHashMap<>();
        List<DeleteObject> deletes = new ArrayList<>();
        try {
            Path root = Paths.get(scanDirString).toAbsolutePath();
            boolean scanRecursive = plugin.getConfig().getBooleanParam("scan_recursive",true);

            // Load the catalog once (path -> md5, lastmodified, filesize) so the per-file freshness check below is an
            // in-memory lookup instead of a DB round-trip per file on every scan cycle.
            Map<String,String[]> catalog = new HashMap<>();
            for (Map<String,String> row : dbEngine.getRepoList()) {
                catalog.put(row.get("filepath"), new String[]{row.get("md5"), row.get("lastmodified"), row.get("filesize")});
            }

            Walk walk = walk(root, scanRecursive);

            Set<String> seen = new HashSet<>();
            List<DBEngine.Row> adds = new ArrayList<>();
            List<DBEngine.Row> updates = new ArrayList<>();
            List<String> changed = new ArrayList<>();
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
                fileDiffMap.put(filePath, new FileObject(fileName, MD5hash, filePath, lastModified, filesize));
                (add ? adds : updates).add(new DBEngine.Row(filePath, MD5hash, lastModified, filesize));
                changed.add(filePath);
                if (adds.size() + updates.size() >= scanBatchSize) {
                    dbEngine.applyScan(adds, updates, scanBatchSize);
                    adds.clear();
                    updates.clear();
                }
                transferId++;
            }
            dbEngine.applyScan(adds, updates, scanBatchSize);
            // what changed is pending for every subscriber (and no longer a pending delete)
            dbEngine.markChanged(changed, scanBatchSize);

            // the delete phase: catalog rows under the scan dir whose file is gone
            if (walk.complete) {
                deletes.addAll(recordDeletes(root, scanRecursive, catalog, seen, walk));
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
    private List<DeleteObject> recordDeletes(Path root, boolean recursive, Map<String, String[]> catalog, Set<String> seen,
                                             Walk walk) {
        long now = System.currentTimeMillis();
        List<DBEngine.Tombstone> gone = new ArrayList<>();
        List<String> underRoot = new ArrayList<>();
        for (Map.Entry<String, String[]> e : catalog.entrySet()) {
            String path = e.getKey();
            Path p = Paths.get(path);
            // a stale row from elsewhere (an earlier scan_dir) is not this scanner's to delete
            if (!p.toAbsolutePath().startsWith(root)) continue;
            underRoot.add(path);
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

        DeleteGuard.Decision d = guard.evaluate(root, underRoot, gone, seen.size(), vanished, now);
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

    /** How many deletes the mass-delete guard is holding. */
    int getDeletesHeld() {
        return guard.heldCount();
    }

    /** The id of the held set (what releasedeletes names), or null. */
    String getHoldId() {
        return guard.holdId();
    }

    /**
     * Release the deletes now held (an admin's releasedeletes), once: the next scan records them if, and only if, it
     * finds exactly that set held. Returns how many were released, 0 when nothing is held, -1 when {@code holdId} names
     * another set.
     */
    int releaseHeldDeletes(String holdId) {
        return guard.release(holdId, System.currentTimeMillis());
    }

    int releaseHeldDeletes() {
        return releaseHeldDeletes(null);
    }

    // =========================================================================== producer: offers

    private static String peerKey(String region, String agent, String pluginId) {
        return region + "_" + agent + "_" + pluginId;
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
                return;
            }
        }
        for (Future<?> f : turns) {
            try {
                f.get();
            } catch (InterruptedException stop) {
                for (Future<?> g : turns) g.cancel(true);
                Thread.currentThread().interrupt();
                return;
            } catch (ExecutionException | CancellationException ex) {
                logger.error("offer failed", ex);
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
        pendingTransferMap.put(tid, new Offer(s.key, pageFiles, pageDeletes));
        MsgEvent req = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, s.region, s.agent, s.plugin);
        req.setParam("action", "repolistin");
        req.setCompressedParam("repolistin", gson.toJson(pageFiles));
        if (!pageDeletes.isEmpty()) req.setCompressedParam("repodeletes", gson.toJson(pageDeletes));
        req.setParam("transfer_id", tid);
        req.setParam("sync_proto", SYNC_PROTO);
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

    /** Forget subscribers not heard from in sync_peer_ttl_ms, and what they had pending. */
    private void pruneSubscribers() {
        long now = System.currentTimeMillis();
        for (ActiveSub s : new ArrayList<>(subscribers.values())) {
            if (now - s.lastSeen > peerTtlMs) {
                subscribers.remove(s.key);
                dbEngine.deleteSubscriber(s.key);
                inFlight.keySet().removeIf(k -> k.startsWith(s.key + "|"));
                logger.info("subscriber " + s.key + " not heard from in " + peerTtlMs + " ms: forgotten");
            }
        }
    }

    /**
     * reposubscribe (protocol 2): a subscriber, named by its stamped caller, asks to be served. Taken only by a
     * producer of the same filerepo_name speaking the same protocol; a subscriber with a new sync generation (new, or
     * cleared) is offered everything again. The answer (reposubscribed) goes to the verified caller, never to an
     * address the message names.
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
        String refusal = null;
        if (!isProducer()) refusal = "not a producer";
        else if (!fileRepoName.equals(name)) refusal = "a producer of another filerepo_name";
        else if (!SYNC_PROTO.equals(proto)) refusal = "sync protocol " + proto + " is not " + SYNC_PROTO + ": upgrade the subscriber";
        else if (gen == null || !gen.matches("[0-9a-f]{1,64}")) refusal = "no valid sync_gen";
        else if (from[0].equals(plugin.getRegion()) && from[1].equals(plugin.getAgent()) && from[2].equals(plugin.getPluginID())) refusal = "itself";
        long now = System.currentTimeMillis();
        if (refusal == null) {
            ActiveSub s = subscribers.get(key);
            if (s == null) {
                pruneSubscribers();
                if (subscribers.size() >= syncMaxPeers) refusal = "full: " + subscribers.size() + " subscribers (sync_max_peers)";
            }
            if (refusal == null) {
                if (s == null || !gen.equals(s.gen)) {
                    // new to this producer, or cleared: everything is pending for it again
                    if (!dbEngine.resetSubscriber(new DBEngine.Subscriber(key, from[0], from[1], from[2], gen, now), scanBatchSize)) {
                        refusal = "the catalog could not record it";
                    } else {
                        inFlight.keySet().removeIf(k -> k.startsWith(key + "|"));
                        subscribers.put(key, new ActiveSub(key, from[0], from[1], from[2], gen, now));
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
            reply.setParam("producer_epoch", epoch);
        }
        plugin.msgOut(reply);
    }

    //data transfer
    public void confirmTransfer(String incomingTransferId, String failedFilesJson, String region, String agent, String pluginId) {
        confirmTransfer(incomingTransferId, failedFilesJson, null, region, agent, pluginId);
    }

    /**
     * A subscriber's confirm: the files it now holds (all offered but {@code failedFiles}) are settled for it where the
     * catalog still holds the offered version, and so are the deletes it applied. Only the subscriber the transfer was
     * offered to settles it, and only a transfer of this producer instance (its epoch): a confirm from before a restart,
     * or for another subscriber's transfer, settles nothing. What failed stays pending for that subscriber alone.
     */
    public void confirmTransfer(String incomingTransferId, String failedFilesJson, String failedDeletesJson,
                                String region, String agent, String pluginId) {
        try {
            String key = peerKey(region, agent, pluginId);
            Offer offered = incomingTransferId == null ? null : pendingTransferMap.get(incomingTransferId);
            if (offered == null || !offered.subscriber.equals(key)) {
                logger.debug("confirm of transfer " + incomingTransferId + " from " + key + " ignored: not its transfer"
                        + " (another subscriber's, an earlier instance's, or expired)");
                return;
            }
            pendingTransferMap.remove(incomingTransferId);
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
            ActiveSub s = subscribers.get(key);
            if (s != null) {
                s.lastSeen = System.currentTimeMillis();
                dbEngine.touchSubscriber(key, s.lastSeen);
            }
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

    /** One diff from a producer, waiting for its updater. */
    private static final class PendingDiff {
        final String transferId;
        final String filesJson;
        final String deletesJson;

        PendingDiff(String transferId, String filesJson, String deletesJson) {
            this.transferId = transferId;
            this.filesJson = filesJson;
            this.deletesJson = deletesJson;
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

    /** Remember a producer as verified (its answer to reposubscribe; tests). Bounded by sync_max_peers. */
    void rememberProducer(String region, String agent, String pluginId) {
        String key = peerKey(region, agent, pluginId);
        long now = System.currentTimeMillis();
        if (!knownProducers.containsKey(key) && knownProducers.size() >= syncMaxPeers) {
            knownProducers.entrySet().removeIf(e -> now - e.getValue() > peerTtlMs);
            if (knownProducers.size() >= syncMaxPeers) {
                logger.warn("not taking producer " + key + ": already " + knownProducers.size() + " producers (sync_max_peers)");
                return;
            }
        }
        knownProducers.put(key, now);
    }

    boolean isKnownProducer(String region, String agent, String pluginId) {
        Long at = knownProducers.get(peerKey(region, agent, pluginId));
        return at != null && System.currentTimeMillis() - at <= peerTtlMs;
    }

    /** This repository's sync generation, as its subscribe advertises it (tests). */
    String getSyncGen() {
        return syncGen;
    }

    /**
     * A producer's discover (dataplane, unverified): only an address to ask. A subscribe (reposubscribe, a MsgEvent)
     * goes to it, rate limited per producer and overall; the producer is taken only when it answers, verified by its
     * stamped caller, as a producer of this filerepo_name speaking this protocol. With sync_producers set, only those
     * producers are asked.
     */
    private void onDiscover(Map<String, String> m) {
        String region = m.get("repo_region_id"), agent = m.get("repo_agent_id"), pluginId = m.get("repo_plugin_id");
        if (region == null || agent == null || pluginId == null) {
            logger.error("discover without identification");
            return;
        }
        if (region.equals(plugin.getRegion()) && agent.equals(plugin.getAgent()) && pluginId.equals(plugin.getPluginID())) return;
        String key = peerKey(region, agent, pluginId);
        if (!SYNC_PROTO.equals(m.get("sync_proto"))) {
            Long warned = oldProtocolWarned.get(key);
            if (warned == null || System.currentTimeMillis() - warned > 3_600_000L) {
                oldProtocolWarned.put(key, System.currentTimeMillis());
                logger.warn("producer " + key + " speaks sync protocol " + m.getOrDefault("sync_proto", "1") + ", not "
                        + SYNC_PROTO + ": not syncing from it until it is upgraded");
            }
            return;
        }
        if (configuredProducers != null && !configuredProducers.contains(key)) return;
        long now = System.currentTimeMillis();
        synchronized (attempts) {
            Long last = attempts.get(key);
            if (last != null && now - last < subscribeRetryMs) return;
            attemptTokens = Math.min(512, attemptTokens + (now - attemptRefilled) * 64.0 / 1000.0);
            attemptRefilled = now;
            if (attemptTokens < 1) return;
            attemptTokens -= 1;
            attempts.put(key, now);
        }
        MsgEvent sub = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, region, agent, pluginId);
        sub.setParam("action", "reposubscribe");
        sub.setParam("filerepo_name", fileRepoName);
        sub.setParam("sync_gen", syncGen);
        sub.setParam("sync_proto", SYNC_PROTO);
        plugin.msgOut(sub);
    }

    /** reposubscribed: a producer's answer, from its stamped caller; taken only from a producer this repository asked. */
    void onSubscribed(MsgEvent m) {
        String[] from = Callers.plugin(m, insecure);
        if (from == null) {
            logger.warn("reposubscribed ignored: no verified plugin caller");
            return;
        }
        String key = peerKey(from[0], from[1], from[2]);
        if (!attempts.containsKey(key)) {
            logger.warn("reposubscribed from " + key + " ignored: this repository did not ask it");
            return;
        }
        if (configuredProducers != null && !configuredProducers.contains(key)) return;
        if ("true".equals(m.getParam("accepted")) && fileRepoName != null && fileRepoName.equals(m.getParam("filerepo_name"))
                && SYNC_PROTO.equals(m.getParam("sync_proto"))) {
            rememberProducer(from[0], from[1], from[2]);
        } else {
            knownProducers.remove(key);
            logger.info("producer " + key + " did not take this subscriber: " + m.getParam("refusal"));
        }
    }

    public void getFileRepoDiff(String repoDiffString, String transferId, String region, String agent, String pluginId) {
        acceptDiff(repoDiffString, null, transferId, region, agent, pluginId);
    }

    /**
     * Take a producer's diff (FR-D5): only from a verified producer this repository subscribed to, at most
     * sync_pending_per_peer waiting per producer and sync_max_peers producers at once, on a bounded pool. Anything over
     * a bound is refused as busy, and the producer offers it again next cycle.
     */
    Accepted acceptDiff(String filesJson, String deletesJson, String transferId, String region, String agent, String pluginId) {
        String repoId = peerKey(region, agent, pluginId);
        if (!isKnownProducer(region, agent, pluginId)) {
            syncRefused.incrementAndGet();
            logger.warn("diff " + transferId + " from " + repoId + " refused: not a verified producer this repository subscribed to");
            return new Accepted("9", "refused: not a verified producer this repository subscribed to");
        }
        PendingDiff diff = new PendingDiff(transferId, filesJson, deletesJson);
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
                applyDiff(pending, repoId, region, agent, pluginId);
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

    private void applyDiff(PendingDiff pending, String repoId, String region, String agent, String pluginId) {
        Runnable hook = beforeApplyDiff;
        if (hook != null) hook.run();
        Map<String,FileObject> remoteRepoFiles = null;
        List<DeleteObject> remoteDeletes = null;
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
        Path repoBase = getRepoDir().toPath().toAbsolutePath().normalize();
        Path realBase;
        try {
            realBase = repoBase.toRealPath();
        } catch (IOException ex) {
            realBase = repoBase;
        }

        for (DeleteObject d : remoteDeletes) {
            if (d == null || d.filePath == null) continue;
            if (!applyDelete(d, repoId, repoBase, realBase)) failedDeletes.add(d.filePath);
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
                // a file landed here directly (cataloged by this repository) is never overwritten by sync
                if (dbEngine.getFileInfo(localPath.toFile().getAbsolutePath()) != null) {
                    logger.warn("not syncing [" + rel + "] from " + repoId + ": a file landed here holds that name");
                    syncConflicts.incrementAndGet();
                    failedFiles.add(fileObject.filePath);
                    continue;
                }
                Map<String, String> holders = dbEngine.mirroredBy(rel);
                String mine = holders.remove(repoId);
                if (Files.exists(localPath, LinkOption.NOFOLLOW_LINKS)) {
                    String held = Files.isRegularFile(localPath, LinkOption.NOFOLLOW_LINKS) ? Hashes.content(localPath) : null;
                    if (held != null && held.equals(fileObject.MD5)) {
                        // already held, exactly: it is this producer's too
                        dbEngine.recordMirrored(repoId, rel, fileObject.MD5);
                        continue;
                    }
                    // other content: replaced only when it is this producer's own earlier version, held by no other
                    boolean ours = held != null && held.equals(mine) && holders.isEmpty();
                    if (!ours) {
                        logger.warn("not syncing [" + rel + "] from " + repoId + ": the file here holds other content"
                                + (holders.isEmpty() ? " (not this producer's)" : " (another producer's: " + holders.keySet() + ")"));
                        syncConflicts.incrementAndGet();
                        failedFiles.add(fileObject.filePath);
                        continue;
                    }
                } else if (!holders.isEmpty()) {
                    // another producer delivered this name and it is gone here: refetch it from this one is fine only
                    // if they agree on the content
                    for (String other : holders.values()) {
                        if (!fileObject.MD5.equals(other)) {
                            logger.warn("not syncing [" + rel + "] from " + repoId + ": another producer delivered other content under that name");
                            syncConflicts.incrementAndGet();
                            failedFiles.add(fileObject.filePath);
                            localPath = null;
                            break;
                        }
                    }
                    if (localPath == null) continue;
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
                SyncReceiver.Result r = rx.fetch(fileObject.filePath, fileObject.filesize, fileObject.MD5, localPath);
                if (r.ok) {
                    syncFetched.incrementAndGet();
                    dbEngine.recordMirrored(repoId, rel, fileObject.MD5);
                    logger.debug("Synced " + localPath);
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
        if (!failedFiles.isEmpty()) {
            filesConfirm.setCompressedParam("failedfiles", gson.toJson(failedFiles));
        }
        if (!failedDeletes.isEmpty()) {
            filesConfirm.setCompressedParam("faileddeletes", gson.toJson(failedDeletes));
        }
        plugin.msgOut(filesConfirm);
    }

    /**
     * Apply a producer's delete: remove the local copy only if this producer delivered exactly that content under that
     * name, no other producer still holds it here, it is not a file landed here, it is exactly the deleted content, and
     * it is not a case-only collision; never outside the repository directory or in a live-tier root. Anything else is
     * left alone and counts as applied (it is not that producer's to delete). False when the delete should be offered
     * again.
     */
    private boolean applyDelete(DeleteObject d, String producer, Path repoBase, Path realBase) {
        if (d.fileName == null || d.MD5 == null) return false;
        Target t = target(repoBase, realBase, d.fileName);
        if (t.why != null) {
            logger.warn("not applying a delete of [" + d.fileName + "] from " + producer + ": " + t.why);
            return true;
        }
        Path localPath = t.path;
        Map<String, String> holders = dbEngine.mirroredBy(d.fileName);
        String mine = holders.remove(producer);
        if (mine == null) {
            // this producer never delivered that name here: not its to delete
            return true;
        }
        if (!mine.equals(d.MD5)) {
            // it delivered other content since: that is what it holds now
            return true;
        }
        if (!holders.isEmpty()) {
            // another producer still holds it here: only this producer's claim goes
            dbEngine.forgetMirrored(producer, d.fileName);
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
        try {
            Files.delete(localPath);
            dbEngine.forgetMirrored(producer, d.fileName);
            syncDeleted.incrementAndGet();
            logger.debug("deleted " + localPath + " (deleted at the producer)");
            return true;
        } catch (IOException ex) {
            logger.warn("could not delete " + localPath + ": " + ex.getMessage());
            return false;
        }
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
            if (getDeletesHeld() > 0) {
                facts.put("deletes_held", String.valueOf(getDeletesHeld()));
                facts.put("deletes_hold_id", String.valueOf(getHoldId()));
            }
            return RepoState.of(RepoState.ONLY_COPY, facts,
                    "the origin of what it holds: no producer refills it");
        }
        facts.put("role", "subscriber");
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
            syncGen = SyncReceiver.token();
            attempts.clear();

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
                    Files.move(part, dest, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                    part = null;
                    if (!got.equals(Hashes.content(dest))) {
                        Files.deleteIfExists(dest);
                        put.failed.put(name, "the file changed while it was moved into place");
                        continue;
                    }
                    // landed content is this repository's own, no producer's
                    for (String producer : dbEngine.mirroredBy(name).keySet()) dbEngine.forgetMirrored(producer, name);
                    // Catalog the landed file when no scanner will pick it up (landing-only repos never schedule the
                    // scan timer); a replaced file gets its new content recorded, not a stale row
                    if (fileScanTimer == null) {
                        File saved = dest.toFile();
                        dbEngine.upsertFile(saved.getAbsolutePath(), got, saved.lastModified(), saved.length());
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
        Long warned = oldProtocolWarned.get(who);
        if (warned == null || System.currentTimeMillis() - warned > 3_600_000L) {
            if (oldProtocolWarned.size() < 1024) oldProtocolWarned.put(who, System.currentTimeMillis());
            logger.warn("ignored a sync protocol 1 '" + incomingMap.get("action") + "' naming " + who
                    + " over the dataplane: subscribers of protocol " + SYNC_PROTO + " subscribe with reposubscribe; upgrade it");
        }
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
     * The repository directory as configured, without creating it: scan_dir, else repo_dir, else the absolute default
     * {@code <plugin data dir>/repo} (FR-D9; the old default was the relative path "filerepo", wherever the JVM started,
     * and putjar and the scanner disagreed on it).
     */
    File repoDirFile() {
        String configured = scanDirString != null ? scanDirString : repoDir;
        if (configured != null) return new File(configured).getAbsoluteFile();
        return Paths.get(plugin.getPluginDataDirectory(), "repo").toAbsolutePath().toFile();
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
