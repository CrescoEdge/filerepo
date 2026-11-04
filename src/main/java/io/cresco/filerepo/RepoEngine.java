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
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    // transferId -> what that offer carried (files and deletes); consumed by confirmTransfer() so insync=1 is only
    // recorded once a subscriber confirms verified downloads and applied deletes (bounded: oldest evicted)
    private final Map<String, Offer> pendingTransferMap =
            Collections.synchronizedMap(new LinkedHashMap<String, Offer>() {
                protected boolean removeEldestEntry(Map.Entry<String, Offer> eldest) {
                    return size() > 4096;
                }
            });
    // producer side: catalog path (or "del:" + path) -> when it was last offered, so a file still being fetched is not
    // offered again every cycle; cleared by the confirm, expired after sync_offer_timeout_ms
    private final Map<String, Long> offeredAt = new ConcurrentHashMap<>();
    private final AtomicLong transferSeq = new AtomicLong();

    // Sync state is one insync flag per file, so a file any subscriber confirmed is not offered again. A subscriber
    // therefore advertises a sync generation (new when it starts, and after a clear) in its subscribe; for a
    // generation it has not completed, the producer sends that subscriber alone every confirmed file again, paced
    // (sync_full_pages_per_cycle). This is how a subscriber that joins late, or a mirror that was cleared, refills.
    private volatile String syncGen = SyncReceiver.token();
    private final Map<String, FullOffer> fullOffers = new ConcurrentHashMap<>();

    /** A full offer to one subscriber: its generation, the next page, and whether it is done. */
    static final class FullOffer {
        final String gen;
        volatile int offset;
        volatile boolean done;

        FullOffer(String gen) {
            this.gen = gen;
        }
    }

    private final Object lockPeerUpdateStateMap = new Object();
    private final Map<String, Boolean> peerUpdateStateMap = new HashMap<>();

    private final Object lockPeerUpdateQueueMap = new Object();
    // producer -> its diffs waiting for the updater (FR-D5: bounded per producer and in producers)
    private final Map<String, Deque<PendingDiff>> peerUpdateQueueMap = new HashMap<>();
    // subscriber side: the producers this repository subscribed to (from their discover broadcasts); a diff from any
    // other source is refused, so no caller can make this repository fetch from an agent it never chose
    private final Set<String> knownProducers = ConcurrentHashMap.newKeySet();

    private Type mapType;

    // volatile: read by putFiles() on dispatch threads to decide inline-catalog vs scanner-owned
    private volatile Timer fileScanTimer;
    private Timer repoBroadcastTimer;

    private String scanDirString;

    private final Object lockSubscriberMap = new Object();
    private Map<String,Map<String,String>> subscriberMap;

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
    private final int deleteGuardMin;
    private final double deleteGuardFraction;
    private final boolean followLinks;
    private final boolean requirePutHashes;
    private final int syncStreamBuffer;
    private final long syncStreamIdleMs;
    private final int syncStreamRounds;
    private final long syncRpcTimeoutMs;
    private final int fullPagesPerCycle;

    // Bounded, named, daemon pool for peer-sync updaters: one per producer at a time, at most sync_threads at once,
    // and at most sync_queue_max waiting (FR-D5). A diff that finds both full is refused as busy (status 11) and the
    // producer offers it again next cycle: back-pressure instead of an unbounded backlog.
    private final ThreadPoolExecutor syncPool;

    // ---- sync metrics ----
    final AtomicLong syncRejected = new AtomicLong();
    final AtomicLong syncRefused = new AtomicLong();
    final AtomicLong syncFetched = new AtomicLong();
    final AtomicLong syncFetchFailed = new AtomicLong();
    final AtomicLong syncVerifyFailed = new AtomicLong();
    final AtomicLong syncDeleted = new AtomicLong();
    final AtomicLong tombstonesRecorded = new AtomicLong();
    // the deletes the last scan held back (the mass-delete guard), and how many an admin released
    private volatile int deletesHeld;
    private volatile int releaseUpTo;

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

        subscriberMap = Collections.synchronizedMap(new HashMap<>());

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
        deleteGuardMin = (int) clamp(longParam("sync_delete_guard_min", 100L), 1, Integer.MAX_VALUE);
        double fraction = 0.25;
        try {
            Double d = plugin.getConfig().getDoubleParam("sync_delete_guard_fraction");
            if (d != null && d > 0 && d <= 1) fraction = d;
        } catch (Exception ignore) { }
        deleteGuardFraction = fraction;
        followLinks = plugin.getConfig().getBooleanParam("scan_follow_links", false);
        requirePutHashes = plugin.getConfig().getBooleanParam("putfiles_require_hashes", true);
        syncStreamBuffer = (int) clamp(longParam("sync_stream_buffer", 262144L), 4096, 786432);
        syncStreamIdleMs = clamp(longParam("sync_stream_idle_ms", 30_000L), 100L, 3_600_000L);
        syncStreamRounds = (int) clamp(longParam("sync_stream_rounds", 3L), 1, 16);
        syncRpcTimeoutMs = clamp(longParam("sync_rpc_timeout_ms", 30_000L), 100L, 600_000L);
        fullPagesPerCycle = (int) clamp(longParam("sync_full_pages_per_cycle", 8L), 1, 10_000);

        int syncThreads = (int) clamp(longParam("sync_threads", 8L), 1, 256);
        int syncQueue = (int) clamp(longParam("sync_queue_max", 64L), 1, 100_000);
        syncPool = new ThreadPoolExecutor(syncThreads, syncThreads, 60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(syncQueue),
                r -> { Thread t = new Thread(r, "filerepo-sync"); t.setDaemon(true); return t; },
                new ThreadPoolExecutor.AbortPolicy());
        // let idle sync threads retire (core==max would otherwise keep them alive forever)
        syncPool.allowCoreThreadTimeOut(true);

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
            logger.info("Start listening for filerepo: " + fileRepoName);
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
     * One scan cycle: catalog what changed, record what was deleted, offer both to the subscribers. Returns false when
     * the cycle was skipped (another scan running, the scan dir missing, the gate held, the dir over a live root).
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
                logger.debug("\t\t ***STARTING SCAN repo_name: " + fileRepoName + " tid:" + transferId);
                ScanResult result = scan();
                // offers this cycle's changes, and continues any full offer to a subscriber
                syncRegionFiles(result);
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
        return name != null && name.startsWith(STAGING_PREFIX) && name.endsWith(STAGING_SUFFIX);
    }

    /** A fresh staging file beside {@code destination}: same directory, so the final move is an atomic rename. */
    static Path stagingFor(Path destination) {
        return destination.resolveSibling(STAGING_PREFIX + UUID.randomUUID().toString().replace("-", "") + STAGING_SUFFIX);
    }

    /**
     * The regular files under {@code root} (recursively or not), skipping staging files and, unless scan_follow_links,
     * every symbolic link. Returns false when part of the tree could not be read: then no file may be taken for deleted.
     */
    private boolean walk(Path root, boolean recursive, List<Path> out) {
        final boolean[] complete = {true};
        try {
            Files.walkFileTree(root,
                    followLinks ? EnumSet.of(FileVisitOption.FOLLOW_LINKS) : EnumSet.noneOf(FileVisitOption.class),
                    recursive ? Integer.MAX_VALUE : 1,
                    new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                            if (attrs.isRegularFile() && !isStaging(file.getFileName().toString())) out.add(file);
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult visitFileFailed(Path file, IOException exc) {
                            if (exc instanceof FileSystemLoopException) {
                                logger.warn("scan: a directory loop at " + file + " was not followed");
                            } else {
                                complete[0] = false;
                                logger.warn("scan: could not read " + file + ": " + exc.getMessage());
                            }
                            return FileVisitResult.CONTINUE;
                        }
                    });
        } catch (IOException ex) {
            complete[0] = false;
            logger.error("scan walk failed: " + ex.getMessage());
        }
        return complete[0];
    }

    /** What one scan found to offer: new and changed files, and deletes. */
    static final class ScanResult {
        final Map<String, FileObject> files;
        final List<DeleteObject> deletes;

        ScanResult(Map<String, FileObject> files, List<DeleteObject> deletes) {
            this.files = files;
            this.deletes = deletes;
        }
    }

    /** What one offer (a page of a scan) carried, to settle on its confirm. */
    static final class Offer {
        final Map<String, FileObject> files;
        final List<DeleteObject> deletes;
        // a page of a full offer: its files were confirmed already; one that fails is offered again the usual way
        final boolean full;

        Offer(Map<String, FileObject> files, List<DeleteObject> deletes) {
            this(files, deletes, false);
        }

        Offer(Map<String, FileObject> files, List<DeleteObject> deletes, boolean full) {
            this.files = files;
            this.deletes = deletes;
            this.full = full;
        }
    }

    //build and sync (the scan's files to offer; deletes are recorded and kept in the catalog)
    Map<String,FileObject> getFileRepoDiff() {
        return scan().files;
    }

    /**
     * Scan the scan dir against the catalog: new and changed files are hashed and cataloged in batches (FR-D8), files
     * that are gone become tombstones (FR-D4), unless the mass-delete guard holds them, and unconfirmed rows and
     * tombstones are offered again once their last offer has expired.
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

            List<Path> found = new ArrayList<>();
            boolean complete = walk(root, scanRecursive, found);

            Set<String> seen = new HashSet<>();
            List<DBEngine.Row> adds = new ArrayList<>();
            List<DBEngine.Row> updates = new ArrayList<>();
            for (Path p : found) {
                String filePath = p.toFile().getAbsolutePath();
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
                String MD5hash = plugin.getMD5(filePath);
                // Skip files we can't safely catalog: a null md5 means the file vanished mid-scan (getMD5 failed); a
                // path longer than the catalog column would fail the insert. Skipping keeps one bad file from wedging
                // the scan.
                if (MD5hash == null || filePath.length() > 1000) {
                    logger.warn("skipping uncatalogable file (null md5 or path>1000): " + filePath);
                    continue;
                }
                // In recursive mode use the path RELATIVE to scan_dir so nested directory structure is preserved on the
                // consumer (the consumer creates parent dirs); getName() alone would flatten and collide.
                String fileName = scanRecursive ? root.relativize(p.toAbsolutePath()).toString() : p.getFileName().toString();
                fileDiffMap.put(filePath, new FileObject(fileName, MD5hash, filePath, lastModified, filesize));
                (add ? adds : updates).add(new DBEngine.Row(filePath, MD5hash, lastModified, filesize));
                if (adds.size() + updates.size() >= scanBatchSize) {
                    dbEngine.applyScan(adds, updates, scanBatchSize);
                    adds.clear();
                    updates.clear();
                }
                transferId++;
            }
            dbEngine.applyScan(adds, updates, scanBatchSize);

            // the delete phase: catalog rows under the scan dir whose file is gone
            if (complete) {
                recordDeletes(root, scanRecursive, catalog, seen);
            } else {
                logger.warn("scan of " + root + " was incomplete: no file is taken for deleted this cycle");
            }

            long now = System.currentTimeMillis();
            // Re-offer catalog rows no subscriber has confirmed (insync=0): the FS-vs-DB diff above only sees NEW or
            // MODIFIED files, so a failed download would otherwise become a permanent silent gap. A row offered less
            // than sync_offer_timeout_ms ago is still being fetched and is not offered again.
            for (Map<String,String> row : dbEngine.getFilesNotInSync()) {
                String filePath = row.get("filepath");
                if (fileDiffMap.containsKey(filePath) || inFlight(filePath, now)) {
                    continue;
                }
                Path unsynced = Paths.get(filePath);
                if (!unsynced.toAbsolutePath().startsWith(root) || !Files.isRegularFile(unsynced)) {
                    continue;
                }
                // a row cataloged before a live-tier root was known is never offered from it
                if (liveRootConflict(unsynced) != null) {
                    continue;
                }
                String reofferName = scanRecursive ? root.relativize(unsynced.toAbsolutePath()).toString()
                        : unsynced.getFileName().toString();
                try {
                    fileDiffMap.put(filePath, new FileObject(reofferName, row.get("md5"), filePath,
                            Long.parseLong(row.get("lastmodified")), Long.parseLong(row.get("filesize"))));
                } catch (Exception rex) {
                    logger.error("re-offer skipped for malformed catalog row: " + filePath, rex);
                }
            }
            for (DBEngine.Tombstone t : dbEngine.getTombstonesNotInSync(deletePageSize * 16)) {
                if (inFlight("del:" + t.filepath, now)) continue;
                deletes.add(new DeleteObject(t.filename, t.md5, t.filepath, t.deletedAt));
            }
            if (tombstoneTtlMs > 0) {
                int purged = dbEngine.purgeTombstones(now - tombstoneTtlMs);
                if (purged > 0) logger.debug("purged " + purged + " confirmed tombstone(s)");
            }

        } catch (Exception ex) {
            logger.error("filerepo scan error", ex);
        }
        return new ScanResult(fileDiffMap, deletes);
    }

    private boolean inFlight(String key, long now) {
        Long at = offeredAt.get(key);
        if (at == null) return false;
        if (now - at < offerTimeoutMs) return true;
        offeredAt.remove(key, at);
        return false;
    }

    /**
     * Turn the catalog rows whose file is gone into tombstones, unless the mass-delete guard holds them: a scan dir that
     * is suddenly empty (an unmounted volume, a removed directory) or loses more than sync_delete_guard_fraction of the
     * catalog (and at least sync_delete_guard_min files) must not empty every subscriber. Held deletes are reported
     * (repostate, getmetrics, health) until an admin releases them (releasedeletes) or the files come back.
     */
    private void recordDeletes(Path root, boolean recursive, Map<String, String[]> catalog, Set<String> seen) {
        List<DBEngine.Tombstone> gone = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (Map.Entry<String, String[]> e : catalog.entrySet()) {
            String path = e.getKey();
            if (seen.contains(path)) continue;
            Path p = Paths.get(path);
            // a stale row from elsewhere (an earlier scan_dir) is not this scanner's to delete
            if (!p.toAbsolutePath().startsWith(root)) continue;
            // still there: a subdirectory file in a non-recursive scan, a link, an unreadable file
            if (Files.exists(p, LinkOption.NOFOLLOW_LINKS)) continue;
            String name = recursive ? root.relativize(p.toAbsolutePath()).toString() : p.getFileName().toString();
            gone.add(new DBEngine.Tombstone(path, name, e.getValue()[0], now));
        }
        if (gone.isEmpty()) {
            if (deletesHeld != 0) logger.info("the held deletes are gone (the files came back)");
            deletesHeld = 0;
            releaseUpTo = 0;
            return;
        }
        boolean mass = seen.isEmpty()
                || (gone.size() >= deleteGuardMin && gone.size() > deleteGuardFraction * catalog.size());
        if (mass) {
            int released = releaseUpTo;
            if (released > 0 && gone.size() <= released) {
                logger.warn("releasing " + gone.size() + " held delete(s), as an admin asked");
                releaseUpTo = 0;
            } else {
                if (deletesHeld != gone.size()) {
                    logger.error("mass-delete guard: " + gone.size() + " of " + catalog.size() + " cataloged file(s) are"
                            + " gone from " + root + (seen.isEmpty() ? " (the scan dir is empty)" : "")
                            + "; the deletes are held, not offered to subscribers. If this is intended, an admin"
                            + " releases them with releasedeletes");
                }
                deletesHeld = gone.size();
                return;
            }
        }
        deletesHeld = 0;
        int n = dbEngine.tombstone(gone, scanBatchSize);
        tombstonesRecorded.addAndGet(n);
        if (n > 0) logger.info("recorded " + n + " deleted file(s) to offer to subscribers");
    }

    /** How many deletes the mass-delete guard is holding. */
    int getDeletesHeld() {
        return deletesHeld;
    }

    /**
     * Let the next scan record up to the deletes now held (an admin's releasedeletes). Returns how many were released,
     * 0 when nothing is held.
     */
    int releaseHeldDeletes() {
        int held = deletesHeld;
        if (held > 0) releaseUpTo = held;
        return held;
    }

    private void syncRegionFiles(ScanResult result) {
        try {
            List<Map<String,String>> currentSubscriberList;
            synchronized (lockSubscriberMap) {
                currentSubscriberList = new ArrayList<>(subscriberMap.values());
            }
            if (currentSubscriberList.isEmpty()) return;

            // FR-D5/scale: one message per page, never the whole catalog in one control-plane message
            List<FileObject> files = new ArrayList<>(result.files.values());
            List<DeleteObject> deletes = result.deletes;
            int pages = Math.max((files.size() + diffPageSize - 1) / diffPageSize,
                    (deletes.size() + deletePageSize - 1) / deletePageSize);
            long now = System.currentTimeMillis();
            for (int page = 0; page < pages; page++) {
                Map<String, FileObject> pageFiles = new LinkedHashMap<>();
                for (FileObject f : files.subList(Math.min(files.size(), page * diffPageSize),
                        Math.min(files.size(), (page + 1) * diffPageSize))) {
                    pageFiles.put(f.filePath, f);
                }
                List<DeleteObject> pageDeletes = new ArrayList<>(deletes.subList(Math.min(deletes.size(), page * deletePageSize),
                        Math.min(deletes.size(), (page + 1) * deletePageSize)));
                String tid = String.valueOf(transferSeq.incrementAndGet());
                //remember what this transfer offered; insync flips only on subscriber confirm
                pendingTransferMap.put(tid, new Offer(pageFiles, pageDeletes));
                for (String p : pageFiles.keySet()) offeredAt.put(p, now);
                for (DeleteObject d : pageDeletes) offeredAt.put("del:" + d.filePath, now);

                // the page is identical for every subscriber: serialize it once, not once per peer
                String filesJson = gson.toJson(pageFiles);
                String deletesJson = pageDeletes.isEmpty() ? null : gson.toJson(pageDeletes);

                for (Map<String, String> sub : currentSubscriberList) {
                    //This is another filerepo in my region, I need to send it data
                    String region = sub.get("sub_region_id");
                    String agent = sub.get("sub_agent_id");
                    String pluginID = sub.get("sub_plugin_id");

                    MsgEvent fileRepoRequest = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, region, agent, pluginID);
                    fileRepoRequest.setParam("action", "repolistin");
                    fileRepoRequest.setCompressedParam("repolistin", filesJson);
                    if (deletesJson != null) fileRepoRequest.setCompressedParam("repodeletes", deletesJson);
                    fileRepoRequest.setParam("transfer_id", tid);

                    MsgEvent fileRepoResponse = plugin.sendRPC(fileRepoRequest, syncRpcTimeoutMs);
                    if (fileRepoResponse != null) {
                        String code = fileRepoResponse.getParam("status_code");
                        if ("10".equals(code)) {
                            // do NOT mark insync here: status 10 only means the peer accepted the list; rows flip to
                            // insync=1 in confirmTransfer() after the peer reports verified downloads
                            logger.info("Offered " + pageFiles.size() + " file(s) and " + pageDeletes.size()
                                    + " delete(s) to " + pluginID + " (transfer " + tid + ")");
                        } else {
                            logger.warn("Region: " + region + " Agent: " + agent + " pluginId:" + pluginID
                                    + " did not take transfer " + tid + ": status_code " + code + " "
                                    + fileRepoResponse.getParam("status_desc"));
                            // busy or refused: offered again next cycle
                            for (String p : pageFiles.keySet()) offeredAt.remove(p);
                            for (DeleteObject d : pageDeletes) offeredAt.remove("del:" + d.filePath);
                        }
                    } else {
                        logger.error("Host Region: " + region + " Agent: " + agent + " pluginId:" + pluginID + " failed to respond!");
                        logger.error("Removing Host Region: " + region + " Agent: " + agent + " pluginId:" + pluginID);
                        removeSubscribe(sub);
                        // offered again next cycle to whoever subscribes
                        for (String p : pageFiles.keySet()) offeredAt.remove(p);
                        for (DeleteObject d : pageDeletes) offeredAt.remove("del:" + d.filePath);
                    }
                }
            }

            for (Map<String, String> sub : currentSubscriberList) {
                FullOffer full = fullOffers.get(generateSubKey(sub));
                if (full != null && !full.done) continueFullOffer(sub, full);
            }

        } catch (Exception ex) {
            logger.error("filerepo error", ex);
        }
    }

    /**
     * Send one subscriber the files other subscribers already confirmed, a page at a time, at most
     * sync_full_pages_per_cycle pages a cycle. A page it does not take is sent again next cycle.
     */
    private void continueFullOffer(Map<String, String> sub, FullOffer full) {
        String region = sub.get("sub_region_id");
        String agent = sub.get("sub_agent_id");
        String pluginID = sub.get("sub_plugin_id");
        Path root = Paths.get(scanDirString).toAbsolutePath();
        boolean recursive = plugin.getConfig().getBooleanParam("scan_recursive", true);
        for (int sent = 0; sent < fullPagesPerCycle && !full.done; sent++) {
            List<Map<String, String>> rows = dbEngine.getSyncedRows(diffPageSize, full.offset);
            if (rows.isEmpty()) {
                full.done = true;
                logger.info("full offer to " + pluginID + " (generation " + full.gen + ") complete");
                return;
            }
            Map<String, FileObject> page = new LinkedHashMap<>();
            for (Map<String, String> row : rows) {
                Path p = Paths.get(row.get("filepath"));
                if (!p.toAbsolutePath().startsWith(root) || !Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS)
                        || liveRootConflict(p) != null) continue;
                try {
                    String name = recursive ? root.relativize(p.toAbsolutePath()).toString() : p.getFileName().toString();
                    page.put(row.get("filepath"), new FileObject(name, row.get("md5"), row.get("filepath"),
                            Long.parseLong(row.get("lastmodified")), Long.parseLong(row.get("filesize"))));
                } catch (Exception bad) {
                    logger.error("full offer: malformed catalog row " + row.get("filepath"));
                }
            }
            if (!page.isEmpty()) {
                String tid = String.valueOf(transferSeq.incrementAndGet());
                pendingTransferMap.put(tid, new Offer(page, Collections.emptyList(), true));
                MsgEvent req = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, region, agent, pluginID);
                req.setParam("action", "repolistin");
                req.setCompressedParam("repolistin", gson.toJson(page));
                req.setParam("transfer_id", tid);
                MsgEvent reply = plugin.sendRPC(req, syncRpcTimeoutMs);
                if (reply == null) {
                    logger.error("full offer: " + pluginID + " failed to respond; removing it");
                    removeSubscribe(sub);
                    return;
                }
                if (!"10".equals(reply.getParam("status_code"))) {
                    logger.debug("full offer: " + pluginID + " did not take a page (" + reply.getParam("status_code")
                            + "); again next cycle");
                    return;
                }
                logger.info("Offered " + page.size() + " confirmed file(s) to " + pluginID + " (full offer, transfer " + tid + ")");
            }
            full.offset += rows.size();
            if (rows.size() < diffPageSize) {
                full.done = true;
                logger.info("full offer to " + pluginID + " (generation " + full.gen + ") complete");
            }
        }
    }

    /** This repository's sync generation, as its subscribe advertises it (tests). */
    String getSyncGen() {
        return syncGen;
    }

    private static String peerKey(String region, String agent, String pluginId) {
        return region + "_" + agent + "_" + pluginId;
    }

    /** True when {@code region/agent/pluginId} has subscribed to this repository. */
    boolean isSubscriber(String region, String agent, String pluginId) {
        synchronized (lockSubscriberMap) {
            return subscriberMap.containsKey(peerKey(region, agent, pluginId));
        }
    }

    //data transfer
    public void confirmTransfer(String incomingTransferId, String failedFilesJson, String region, String agent, String pluginId) {
        confirmTransfer(incomingTransferId, failedFilesJson, null, region, agent, pluginId);
    }

    /**
     * A subscriber's confirm: the files it now holds (all offered but {@code failedFiles}) flip to insync=1, where the
     * catalog still holds the offered content, and so do the deletes it applied. Only a subscriber of this repository
     * may confirm: any other caller could otherwise mark files synced that no subscriber holds.
     */
    public void confirmTransfer(String incomingTransferId, String failedFilesJson, String failedDeletesJson,
                                String region, String agent, String pluginId) {
        try {
            String repoId = region + "-" + agent + "-" + pluginId;
            if (!isSubscriber(region, agent, pluginId)) {
                logger.warn("confirm of transfer " + incomingTransferId + " from " + repoId + " ignored: not a subscriber");
                return;
            }

            Set<String> failedFiles = parseSet(failedFilesJson, repoId);
            Set<String> failedDeletes = parseSet(failedDeletesJson, repoId);

            Offer offered = incomingTransferId == null ? null : pendingTransferMap.get(incomingTransferId);
            if (offered != null) {
                List<DBEngine.Row> confirmed = new ArrayList<>();
                for (FileObject fileObject : offered.files.values()) {
                    offeredAt.remove(fileObject.filePath);
                    if (!failedFiles.contains(fileObject.filePath)) {
                        confirmed.add(new DBEngine.Row(fileObject.filePath, fileObject.MD5, fileObject.lastModified, fileObject.filesize));
                    }
                }
                List<DBEngine.Tombstone> applied = new ArrayList<>();
                for (DeleteObject d : offered.deletes) {
                    offeredAt.remove("del:" + d.filePath);
                    if (!failedDeletes.contains(d.filePath)) {
                        applied.add(new DBEngine.Tombstone(d.filePath, d.fileName, d.MD5, d.deletedAt));
                    }
                }
                int files = dbEngine.confirmFiles(confirmed);
                int dels = dbEngine.confirmTombstones(applied);
                if (offered.full && !failedFiles.isEmpty()) {
                    // they were confirmed by another subscriber: offer them again the usual way, to everyone
                    Set<String> again = new HashSet<>(failedFiles);
                    again.retainAll(offered.files.keySet());
                    dbEngine.markUnsynced(again);
                }
                logger.info("Transfer " + incomingTransferId + " confirmed by " + repoId + ": "
                        + files + " in sync, " + failedFiles.size() + " failed, " + dels + " delete(s) applied, "
                        + failedDeletes.size() + " delete(s) failed"
                        + (failedFiles.isEmpty() && failedDeletes.isEmpty() ? "" : " (will re-offer)"));
            } else {
                logger.debug("confirmTransfer: unknown/expired transfer_id " + incomingTransferId + " from " + repoId);
            }

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

    /** How a diff was taken: 10 queued, 9 refused (unknown producer), 11 busy (bounded queues full; offer again). */
    static final class Accepted {
        final String code;
        final String desc;

        Accepted(String code, String desc) {
            this.code = code;
            this.desc = desc;
        }
    }

    public void getFileRepoDiff(String repoDiffString, String transferId, String region, String agent, String pluginId) {
        acceptDiff(repoDiffString, null, transferId, region, agent, pluginId);
    }

    /** Remember a producer this repository subscribed to (its discover broadcast). Bounded by sync_max_peers. */
    void rememberProducer(String region, String agent, String pluginId) {
        String key = peerKey(region, agent, pluginId);
        if (knownProducers.contains(key)) return;
        if (knownProducers.size() >= syncMaxPeers) {
            logger.warn("not subscribing to " + key + ": already " + knownProducers.size() + " producers (sync_max_peers)");
            return;
        }
        knownProducers.add(key);
    }

    boolean isKnownProducer(String region, String agent, String pluginId) {
        return knownProducers.contains(peerKey(region, agent, pluginId));
    }

    /**
     * Take a producer's diff (FR-D5): only from a producer this repository subscribed to, at most
     * sync_pending_per_peer waiting per producer and sync_max_peers producers at once, on a bounded pool. Anything over
     * a bound is refused as busy, and the producer offers it again next cycle.
     */
    Accepted acceptDiff(String filesJson, String deletesJson, String transferId, String region, String agent, String pluginId) {
        String repoId = peerKey(region, agent, pluginId);
        if (!knownProducers.contains(repoId)) {
            syncRefused.incrementAndGet();
            logger.warn("diff " + transferId + " from " + repoId + " refused: not a producer this repository subscribed to");
            return new Accepted("9", "refused: not a producer this repository subscribed to");
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

        for (DeleteObject d : remoteDeletes) {
            if (d == null || d.filePath == null) continue;
            if (!applyDelete(d, repoBase)) failedDeletes.add(d.filePath);
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

                // CONTAINMENT: fileName comes from the peer's listing and may carry subdirs (recursive repos), but must
                // never escape the repo dir via ".." or an absolute path (resolve() returns an absolute arg as-is)
                Path localPath = repoBase.resolve(fileObject.fileName).normalize();
                if (!localPath.startsWith(repoBase) || localPath.equals(repoBase)
                        || isStaging(localPath.getFileName().toString())) {
                    logger.error("rejecting peer file name escaping repo dir: [" + fileObject.fileName + "]");
                    failedFiles.add(fileObject.filePath);
                    continue;
                }
                // nor may it lead, through a link, into a live-tier root
                String conflict = liveRootConflict(localPath);
                if (conflict != null) {
                    logger.error("rejecting peer file: its destination " + conflict);
                    failedFiles.add(fileObject.filePath);
                    continue;
                }
                //check that file exists
                if (Files.isRegularFile(localPath, LinkOption.NOFOLLOW_LINKS)) {
                    try {
                        if (Files.size(localPath) == fileObject.filesize
                                && fileObject.MD5.equals(plugin.getMD5(localPath.toString()))) {
                            continue;   // already held, verified
                        }
                    } catch (IOException ignore) { }
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
        if (!failedFiles.isEmpty()) {
            filesConfirm.setCompressedParam("failedfiles", gson.toJson(failedFiles));
        }
        if (!failedDeletes.isEmpty()) {
            filesConfirm.setCompressedParam("faileddeletes", gson.toJson(failedDeletes));
        }
        plugin.msgOut(filesConfirm);
    }

    /**
     * Apply a producer's delete: remove the local copy only if it is exactly the content the producer deleted, never
     * outside the repository directory or in a live-tier root. A copy that is absent, or holds other content (landed
     * here, or newer), is left alone and counts as applied. False when the delete should be offered again.
     */
    private boolean applyDelete(DeleteObject d, Path repoBase) {
        if (d.fileName == null || d.MD5 == null) return false;
        Path localPath = repoBase.resolve(d.fileName).normalize();
        if (!localPath.startsWith(repoBase) || localPath.equals(repoBase)) {
            logger.error("rejecting a delete escaping the repo dir: [" + d.fileName + "]");
            return true;    // never applicable here: do not have it offered forever
        }
        String conflict = liveRootConflict(localPath);
        if (conflict != null) {
            logger.error("rejecting a delete: its target " + conflict);
            return true;
        }
        if (!Files.exists(localPath, LinkOption.NOFOLLOW_LINKS)) return true;
        if (!Files.isRegularFile(localPath, LinkOption.NOFOLLOW_LINKS)) {
            logger.warn("not deleting " + localPath + ": not a regular file");
            return true;
        }
        String held = plugin.getMD5(localPath.toString());
        if (held == null) return false;
        if (!d.MD5.equals(held)) {
            logger.info("not deleting " + localPath + ": it holds other content than the producer deleted");
            return true;
        }
        try {
            Files.delete(localPath);
            dbEngine.deleteFile(localPath.toFile().getAbsolutePath());
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
            if (deletesHeld > 0) facts.put("deletes_held", String.valueOf(deletesHeld));
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
     * producer holds the same files. The repository gate is taken exclusively BEFORE the only-copy check and held through
     * the delete, and every landing takes its shared side, so no file filerepo lands can arrive between the two.
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
            //delete any files not recorded in repo dir (close the walk stream); links are removed, never followed
            long remaining = 0;
            if (dir.isDirectory()) {
                try (java.util.stream.Stream<Path> walk = Files.walk(dir.toPath())) {
                    for (Path p : (Iterable<Path>) walk::iterator) {
                        if (Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(p)) {
                            try {
                                Files.delete(p);
                            } catch (IOException ex) {
                                remaining++;
                            }
                        }
                    }
                }
            }
            // a cleared subscriber asks its producer for everything again
            syncGen = SyncReceiver.token();
            if (remaining == 0) {
                // nothing landed is left: a mirror is a mirror again
                clearLanded();
            } else {
                logger.warn("clearRepo: " + remaining + " file(s) could not be deleted");
            }
            return new Clear(Clear.CLEARED, state, remaining, remaining == 0 ? "cleared"
                    : remaining + " file(s) could not be deleted");

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
            if (cataloged != null) {
                if (fileScanTimer != null && scanDirString != null && info != null) {
                    Path root = Paths.get(scanDirString).toAbsolutePath();
                    Path p = Paths.get(cataloged);
                    String name = p.startsWith(root) ? root.relativize(p).toString() : p.getFileName().toString();
                    tombstonesRecorded.addAndGet(dbEngine.tombstone(Collections.singletonList(
                            new DBEngine.Tombstone(cataloged, name, info.get("md5"), System.currentTimeMillis())), 1));
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
        return putFiles(fileList, hashes, overwrite, isLocal, false).allStored();
    }

    /**
     * Land pushed files (FR-D10). Each file is copied (local) or moved (remote) to a staging file beside its
     * destination, its content hash checked against {@code expected} (required unless putfiles_require_hashes=false),
     * then moved into place atomically: a partial or corrupt file is never visible under its name. A file that exists
     * and is not to be overwritten is reported unchanged when it holds the expected content and skipped when it does
     * not: never counted stored.
     */
    Put putFiles(List<String> fileList, Map<String, String> expected, boolean overwrite, boolean isLocal, boolean requireHashes) {
        Put put = new Put();
        // the shared side of the repository gate: nothing lands between a clear's check and its delete
        if (!enterGate()) {
            put.busy = true;
            return put;
        }
        try {
            Path repo = getRepoDir().toPath().toAbsolutePath();
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
                    String held = Files.isRegularFile(dest, LinkOption.NOFOLLOW_LINKS) ? plugin.getMD5(dest.toString()) : null;
                    if (want != null && want.equals(held)) put.unchanged.add(name);
                    else put.skipped.add(name);
                    continue;
                }
                // before the first byte lands: from here on, this repository holds files only it has
                markLanded();
                Path part = stagingFor(dest);
                try {
                    //if local copy, if remote move temp to correct
                    if (isLocal) {
                        Files.copy(src, part);
                    } else {
                        try {
                            Files.move(src, part);
                        } catch (IOException crossDevice) {
                            Files.copy(src, part);
                            Files.deleteIfExists(src);
                        }
                    }
                    String got = plugin.getMD5(part.toString());
                    if (got == null || (want != null && !want.equals(got))) {
                        put.failed.put(name, "content hash mismatch");
                        continue;
                    }
                    Files.move(part, dest, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                    // Catalog the landed file when no scanner will pick it up (landing-only repos never schedule the
                    // scan timer); a replaced file gets its new content recorded, not a stale row
                    if (fileScanTimer == null) {
                        File saved = dest.toFile();
                        dbEngine.upsertFile(saved.getAbsolutePath(), got, saved.lastModified(), saved.length());
                    }
                    put.stored.add(name);
                } catch (Exception ex) {
                    logger.error("putfiles: " + name + " not landed", ex);
                    put.failed.put(name, ex.getClass().getSimpleName());
                } finally {
                    try { Files.deleteIfExists(part); } catch (Exception ignore) { }
                }
            }
        } finally {
            exitGate();
        }
        return put;
    }


    //sub functions
    private void updateSubscribe(Map<String, String> incomingMap) {

        try {
            if ((incomingMap.containsKey("repo_region_id")) && (incomingMap.containsKey("repo_agent_id")) && (incomingMap.containsKey("repo_plugin_id"))) {

                //don't include self
                if (!((plugin.getRegion().equals(incomingMap.get("repo_region_id"))) && (plugin.getAgent().equals(incomingMap.get("repo_agent_id"))) && (plugin.getPluginID().equals(incomingMap.get("repo_plugin_id"))))) {
                    rememberProducer(incomingMap.get("repo_region_id"), incomingMap.get("repo_agent_id"), incomingMap.get("repo_plugin_id"));
                    if (isKnownProducer(incomingMap.get("repo_region_id"), incomingMap.get("repo_agent_id"), incomingMap.get("repo_plugin_id"))) {
                        subMessage(fileRepoName, incomingMap.get("repo_region_id"), incomingMap.get("repo_agent_id"), incomingMap.get("repo_plugin_id"), "subscribe");
                    }
                }

            } else {
                logger.error("not agent identification provided");
            }
        } catch (Exception ex) {
            logger.error("Failed to subscribe");
            logger.error(ex.getMessage());
        }

    }

    /** Handle a subscriber-side dataplane message (the producer's discover broadcast). */
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
            updateSubscribe(incomingMap);
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

    public void subMessage(String filerepoName, String regionId, String agentId, String pluginId, String action) {

        try {

            Map<String,String> update = new HashMap<>();
            update.put("action",action);
            update.put("filerepo_name",filerepoName);
            update.put("sub_region_id", plugin.getRegion());
            update.put("sub_agent_id",plugin.getAgent());
            update.put("sub_plugin_id",plugin.getPluginID());
            update.put("sync_gen", syncGen);

            TextMessage updateMessage = plugin.getAgentService().getDataPlaneService().createTextMessage();
            updateMessage.setText(gson.toJson(update));
            updateMessage.setStringProperty("filerepo_name",filerepoName);
            updateMessage.setStringProperty("region_id",regionId);
            updateMessage.setStringProperty("agent_id",agentId);
            updateMessage.setStringProperty("plugin_id",pluginId);

            plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL,updateMessage);


        } catch (Exception ex) {
            logger.error("failed to update subscribers");
            logger.error(ex.getMessage());
        }

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

    /** Handle a producer-side dataplane message (a subscriber's subscribe or unsubscribe). */
    void onRepoMessage(Map<String, String> incomingMap) {
        if (incomingMap == null || !incomingMap.containsKey("action")) {
            logger.error("createRepoSubListener no action in message");
            return;
        }
        if (!incomingMap.containsKey("filerepo_name")) {
            logger.error("action called without filerepo_name");
            return;
        }
        switch (incomingMap.get("action")) {
            case "subscribe":
                addSubscribe(incomingMap);
                break;
            case "unsubscribe":
                removeSubscribe(incomingMap);
                break;
            default:
                logger.error("unknown actionType: " + incomingMap.get("action"));
                break;
        }
    }

    private void createRepoSubListener(String filerepoName) {

        MessageListener ml = new MessageListener() {
            public void onMessage(Message msg) {
                try {
                    if (msg instanceof TextMessage) {
                        logger.debug(" REPO REC MESSAGE:" + ((TextMessage) msg).getText());
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

    private String generateSubKey(Map<String, String> incomingMap) {
        if ((incomingMap.containsKey("sub_region_id")) && (incomingMap.containsKey("sub_agent_id")) && (incomingMap.containsKey("sub_plugin_id"))) {
            return peerKey(incomingMap.get("sub_region_id"), incomingMap.get("sub_agent_id"), incomingMap.get("sub_plugin_id"));
        }
        return null;
    }

    private void addSubscribe(Map<String, String> incomingMap) {

        try {
            if ((incomingMap.containsKey("sub_region_id")) && (incomingMap.containsKey("sub_agent_id")) && (incomingMap.containsKey("sub_plugin_id"))) {

                    //don't include self
                    if (!((plugin.getRegion().equals(incomingMap.get("sub_region_id"))) && (plugin.getAgent().equals(incomingMap.get("sub_agent_id"))) && (plugin.getPluginID().equals(incomingMap.get("sub_plugin_id"))))) {
                        String subKey = generateSubKey(incomingMap);
                        if(subKey != null) {
                            synchronized (lockSubscriberMap) {
                                if (subscriberMap.containsKey(subKey)) {
                                    subscriberMap.get(subKey).put("ts", String.valueOf(System.currentTimeMillis()));
                                } else if (subscriberMap.size() >= syncMaxPeers) {
                                    logger.warn("subscriber " + subKey + " not added: already " + subscriberMap.size()
                                            + " subscribers (sync_max_peers)");
                                    return;
                                } else {
                                    Map<String, String> sub = new HashMap<>(incomingMap);
                                    sub.put("ts", String.valueOf(System.currentTimeMillis()));
                                    subscriberMap.put(subKey, sub);
                                }
                                String gen = incomingMap.get("sync_gen");
                                FullOffer current = fullOffers.get(subKey);
                                if (gen != null && gen.matches("[0-9a-f]{1,64}") && (current == null || !current.gen.equals(gen))) {
                                    // a subscriber new to this producer (or cleared): everything again, to it; and
                                    // what is in flight to others is offered again too, so it is not missed
                                    fullOffers.put(subKey, new FullOffer(gen));
                                    offeredAt.clear();
                                    logger.info("subscriber " + subKey + " (generation " + gen + "): a full offer follows");
                                }
                            }
                        }
                    }

            } else {
                logger.error("not agent identification provided");
            }
        } catch (Exception ex) {
            logger.error("Failed to subscribe");
            logger.error(ex.getMessage());
        }


    }

    private void removeSubscribe(Map<String, String> incomingMap) {

        try {

            if ((incomingMap.containsKey("sub_region_id")) && (incomingMap.containsKey("sub_agent_id")) && (incomingMap.containsKey("sub_plugin_id"))) {

                //don't include self
                if (!((plugin.getRegion().equals(incomingMap.get("sub_region_id"))) && (plugin.getAgent().equals(incomingMap.get("sub_agent_id"))) && (plugin.getPluginID().equals(incomingMap.get("sub_plugin_id"))))) {
                    String subKey = generateSubKey(incomingMap);
                    if(subKey != null) {
                        synchronized (lockSubscriberMap) {

                            subscriberMap.remove(subKey);
                            fullOffers.remove(subKey);

                        }
                    }
                }

            } else {
                logger.error("not agent identification provided");
            }

        } catch (Exception ex) {
            logger.error("Failed to unsubscribe");
            logger.error(ex.getMessage());
        }

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
