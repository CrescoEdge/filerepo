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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RepoEngine {

    private PluginBuilder plugin;
    private CLogger logger;
    private Gson gson;

    private Type repoListType;

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

    private AtomicBoolean lockFileMap = new AtomicBoolean();
    private Map<String, Map<String,FileObject>> fileMap;

    private AtomicBoolean lockPeerVersionMap = new AtomicBoolean();
    private Map<String, String> peerVersionMap;
    // transferId -> the files offered in that transfer; consumed by confirmTransfer() so insync=1
    // is only recorded once a subscriber confirms verified downloads (bounded: oldest evicted)
    private final Map<String, Map<String, FileObject>> pendingTransferMap =
            Collections.synchronizedMap(new LinkedHashMap<String, Map<String, FileObject>>() {
                protected boolean removeEldestEntry(Map.Entry<String, Map<String, FileObject>> eldest) {
                    return size() > 64;
                }
            });

    private AtomicBoolean lockPeerUpdateStateMap = new AtomicBoolean();
    private Map<String, Boolean> peerUpdateStateMap;

    private AtomicBoolean lockPeerUpdateQueueMap = new AtomicBoolean();
    private Map<String, Queue<Map<String,String>>> peerUpdateQueueMap;

    private Type mapType;

    // volatile: read by putFiles() on dispatch threads to decide inline-catalog vs scanner-owned
    private volatile Timer fileScanTimer;
    private Timer repoBroadcastTimer;

    private String scanDirString;

    private AtomicBoolean lockSubscriberMap = new AtomicBoolean();
    private Map<String,Map<String,String>> subscriberMap;

    private int transferId = -1;

    private  DBEngine dbEngine;

    private List<String> listenerList;
    private String fileRepoName;
    private String repoDir;

    // Bounded, named, daemon pool for peer-sync downloader threads (replaces unbounded new Thread()).
    // core==max so up to N peer syncs run concurrently: with core<max and an unbounded queue,
    // ThreadPoolExecutor never starts more than core threads, so this used to pin at 1.
    private final ThreadPoolExecutor syncPool = new ThreadPoolExecutor(8, 8, 60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            r -> { Thread t = new Thread(r, "filerepo-sync"); t.setDaemon(true); return t; });

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
        listenerList = new ArrayList<>();

        fileMap = Collections.synchronizedMap(new HashMap<>());
        peerVersionMap = Collections.synchronizedMap(new HashMap<>());
        peerUpdateStateMap = Collections.synchronizedMap(new HashMap<>());
        peerUpdateQueueMap = Collections.synchronizedMap(new HashMap<>());

        scanDirString =  plugin.getConfig().getStringParam("scan_dir");
        fileRepoName =  plugin.getConfig().getStringParam("filerepo_name");
        repoDir =  plugin.getConfig().getStringParam("repo_dir");

        String row = plugin.getConfig().getStringParam("live_row");
        liveRow = (row == null || row.trim().isEmpty()) ? null : row.trim();
        // the same test start() applies to choose the subscriber mode
        subscriber = (liveRow == null) && (scanDirString == null) && (fileRepoName != null) && (repoDir != null);

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

        // let idle sync threads retire (core==max would otherwise keep 8 daemons alive forever)
        syncPool.allowCoreThreadTimeOut(true);
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
        for (String dir : new String[]{scanDirString, plugin.getConfig().getStringParam("repo_dir", "filerepo")}) {
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

                        //check file location
                        if(Paths.get(scanDirString).toFile().exists()) {

                            // claim the scan atomically; the finally guarantees release even if the scan/sync throws
                            if (inScan.compareAndSet(false, true)) {
                                try {
                                    String conflict = repoDirConflict();
                                    if (conflict != null) {
                                        // a scan dir over live-tier files: catalog and offer none of them
                                        if (!warnedRepoDirConflict) {
                                            logger.error("scan refused: the scan dir " + conflict);
                                            warnedRepoDirConflict = true;
                                        }
                                    // the shared side of the repository gate: a clear holding it skips this cycle
                                    } else if (gate.readLock().tryLock()) {
                                        try {
                                            logger.debug("\t\t ***STARTING SCAN repo_name: " + fileRepoName + " inScan: " + inScan.get() + " tid:" + transferId);

                                            //build file list
                                            Map<String, FileObject> diffList = getFileRepoDiff();
                                            if (diffList.size() > 0) {

                                                logger.debug("SYNC Files");
                                                syncRegionFiles(diffList);
                                            }
                                        } finally {
                                            gate.readLock().unlock();
                                        }
                                    } else {
                                        logger.debug("scan skipped: a clear holds the repository gate");
                                    }
                                } finally {
                                    inScan.set(false);
                                }

                            } else {
                                logger.debug("already in scan");
                            }
                        } else {
                            logger.warn("scan dir no longer exists: " + scanDirString);
                        }

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

    public void shutdown() {

        if(repoBroadcastTimer != null) {
            repoBroadcastTimer.cancel();
            repoBroadcastTimer = null;
        }
        stopScan();
        //if(fileRepoName != null) {
        //    repoBroadcast(fileRepoName,"shutdown");
        //}
        for(String listenerid : listenerList) {
            plugin.getAgentService().getDataPlaneService().removeMessageListener(listenerid);
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

    private List<File> getFileNames(List<File> fileNames, Path dir) {
        try(DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path path : stream) {
                if(path.toFile().isDirectory()) {
                    getFileNames(fileNames, path);
                } else {
                    fileNames.add(path.toFile());
                }
            }
        } catch(Exception e) {
            logger.error("filerepo error", e);
        }
        return fileNames;
    }

    //build and sync
    Map<String,FileObject> getFileRepoDiff() {

        Map<String,FileObject> fileDiffMap = null;
        try {

            fileDiffMap = new HashMap<>();

            // Load the catalog once (path -> lastmodified) so the per-file freshness check below is
            // an in-memory lookup instead of a DB round-trip per file on every scan cycle.
            Map<String,Long> catalogMtime = new HashMap<>();
            for (Map<String,String> row : dbEngine.getRepoList()) {
                try { catalogMtime.put(row.get("filepath"), Long.parseLong(row.get("lastmodified"))); }
                catch (Exception ignore) { /* skip malformed row */ }
            }

            File[] listOfFiles = null;
            boolean scanRecursive = plugin.getConfig().getBooleanParam("scan_recursive",true);
            if(scanRecursive) {
                logger.debug("SCAN RECURSIVE");
                List<File> tp = new ArrayList<>();
                List<File> fn = getFileNames(tp,Paths.get(scanDirString));
                for(File f : fn) {
                    logger.debug("File: " + f.getAbsolutePath());
                }


                listOfFiles = new File[fn.size()];
                listOfFiles = fn.toArray(listOfFiles);

            } else {
                logger.debug("NOT SCAN RECURSIVE");
                //get all files in the scan directory
                File folder = new File(scanDirString);
                listOfFiles = folder.listFiles();
            }

            if(listOfFiles != null) {
                for (int i = 0; i < listOfFiles.length; i++) {
                    if (listOfFiles[i].isFile()) {
                        // In recursive mode use the path RELATIVE to scan_dir so nested directory
                        // structure is preserved on the consumer (the consumer creates parent dirs);
                        // getName() alone would flatten every subdir file to its basename and collide.
                        String fileName;
                        if (scanRecursive) {
                            fileName = Paths.get(scanDirString).toAbsolutePath()
                                    .relativize(listOfFiles[i].toPath().toAbsolutePath()).toString();
                        } else {
                            fileName = listOfFiles[i].getName();
                        }
                        String filePath = listOfFiles[i].getAbsolutePath();
                        long lastModified = listOfFiles[i].lastModified();
                        long filesize = listOfFiles[i].length();

                        boolean add = false;
                        boolean update = false;

                        //see if file is in the database (in-memory lookup from the pre-loaded catalog)
                        long lastModifiedDb = catalogMtime.getOrDefault(filePath, -1L);
                        logger.trace("found file: " + filePath + " lastmodified: " + lastModified + " dblastmodified: " + lastModifiedDb);
                        if (lastModifiedDb == -1) {
                            add = true;
                            logger.trace("add file: " + filePath);
                        } else if (lastModifiedDb < lastModified) {
                            update = true;
                            logger.trace("add file: " + filePath);
                        } else if (lastModifiedDb > lastModified) {
                            logger.error("How can an older file be recored in DB? lastModifiedDb > lastModified ");
                            update = true;
                            logger.trace("update file: " + filePath);
                        }

                        String MD5hash = null;
                        if (add || update) {
                            // a link into a live-tier root is never read, cataloged or offered
                            String conflict = liveRootConflict(listOfFiles[i].toPath());
                            if (conflict != null) {
                                logger.warn("skipping a file that " + conflict);
                                continue;
                            }
                            MD5hash = plugin.getMD5(filePath);
                            // Skip files we can't safely catalog: a null md5 means the file vanished
                            // mid-scan (getMD5 failed); a path longer than the catalog column would
                            // fail the insert. Skipping keeps one bad file from wedging the scan.
                            if (MD5hash == null || filePath.length() > 1000) {
                                logger.warn("skipping uncatalogable file (null md5 or path>1000): " + filePath);
                                continue;
                            }
                            logger.trace("generate MD5 for fileName:" + filePath + " MD5:" + MD5hash + " filepath:" + filePath);
                            FileObject fileObject = new FileObject(fileName, MD5hash, filePath, lastModified, filesize);
                            fileDiffMap.put(filePath, fileObject);
                        }

                        if (add) {
                            dbEngine.addFile(filePath, MD5hash, lastModified, filesize);
                            logger.trace("DB insert fileName:" + filePath + " MD5:" + MD5hash + " filepath:" + filePath);
                        }

                        if (update) {
                            dbEngine.updateFile(filePath, MD5hash, 0, lastModified, filesize);
                            logger.trace("DB update fileName:" + filePath + " MD5:" + MD5hash + " filepath:" + filePath);
                        }

                        if (add || update) {
                            //start sync
                            transferId++;
                            //find other repos
                        }

                    }
                }
            }

            // Re-offer catalog rows no subscriber has confirmed (insync=0): the FS-vs-DB diff
            // above only sees NEW or MODIFIED files, so a failed download would otherwise become
            // a permanent silent gap (receivers md5-skip files they already hold, so this is cheap).
            boolean reofferRecursive = plugin.getConfig().getBooleanParam("scan_recursive",true);
            for (Map<String,String> row : dbEngine.getFilesNotInSync()) {
                String filePath = row.get("filepath");
                if (fileDiffMap.containsKey(filePath)) {
                    continue;
                }
                File unsyncedFile = new File(filePath);
                if (!unsyncedFile.isFile()) {
                    continue;
                }
                // a row cataloged before a live-tier root was known is never offered from it
                if (liveRootConflict(unsyncedFile.toPath()) != null) {
                    continue;
                }
                String reofferName;
                if (reofferRecursive) {
                    reofferName = Paths.get(scanDirString).toAbsolutePath()
                            .relativize(unsyncedFile.toPath().toAbsolutePath()).toString();
                } else {
                    reofferName = unsyncedFile.getName();
                }
                try {
                    fileDiffMap.put(filePath, new FileObject(reofferName, row.get("md5"), filePath,
                            Long.parseLong(row.get("lastmodified")), Long.parseLong(row.get("filesize"))));
                } catch (Exception rex) {
                    logger.error("re-offer skipped for malformed catalog row: " + filePath, rex);
                }
            }

        }catch (Exception ex) {
            logger.error(ex.getMessage());
            logger.error("filerepo error", ex);
        }
        return fileDiffMap;
    }

    private void syncRegionFiles(Map<String,FileObject> fileDiffMap) {
        String returnString = null;
        try {
            int subscriberCount = 0;

            List<Map<String,String>> currentSubscriberList = null;

            synchronized (lockSubscriberMap) {
                subscriberCount = subscriberMap.size();
                if (subscriberCount >0) {
                    currentSubscriberList = new ArrayList<>();
                    for (Map<String,String> subscriber : subscriberMap.values()) {
                        currentSubscriberList.add(subscriber);
                    }
                }
            }

            if(subscriberCount > 0) {

                //remember what this transfer offered; insync flips only on subscriber confirm
                pendingTransferMap.put(String.valueOf(transferId), new HashMap<>(fileDiffMap));

                // the diff is identical for every subscriber: serialize it once, not once per peer
                String repoListStringIn = gson.toJson(fileDiffMap);

                for (Map<String, String> subscriberMap : currentSubscriberList) {

                    //This is another filerepo in my region, I need to send it data
                    String region = subscriberMap.get("sub_region_id");
                    String agent = subscriberMap.get("sub_agent_id");
                    String pluginID = subscriberMap.get("sub_plugin_id");

                    logger.debug("SEND :" + region + " " + agent + " " + pluginID + " data");

                    MsgEvent fileRepoRequest = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, region, agent, pluginID);
                    fileRepoRequest.setParam("action", "repolistin");
                    fileRepoRequest.setCompressedParam("repolistin", repoListStringIn);
                    fileRepoRequest.setParam("transfer_id", String.valueOf(transferId));

                    logger.debug("repoListStringIn: " + repoListStringIn);

                    MsgEvent fileRepoResponse = plugin.sendRPC(fileRepoRequest);

                    if (fileRepoResponse != null) {

                        logger.debug("Host Region: " + region + " Agent: " + agent + " pluginId:" + pluginID + " responded");

                        if (fileRepoResponse.paramsContains("status_code") && fileRepoResponse.paramsContains("status_desc")) {
                            int status_code = Integer.parseInt(fileRepoResponse.getParam("status_code"));
                            String status_desc = fileRepoResponse.getParam("status_code");
                            if (status_code != 10) {
                                logger.error("Region: " + region + " Agent: " + agent + " pluginId:" + pluginID + " filerepo update failed status_code: " + status_code + " status_desc:" + status_desc);
                            } else {
                                // do NOT mark insync here: status 10 only means the peer accepted
                                // the list; rows flip to insync=1 in confirmTransfer() after the
                                // peer reports verified downloads (failed files stay 0 -> re-offer)
                                logger.info("Offered " + fileDiffMap.size() + " files to " + pluginID);
                            }
                        }


                    } else {
                        logger.error("Host Region: " + region + " Agent: " + agent + " pluginId:" + pluginID + " failed to respond!");
                        logger.error("Removing Host Region: " + region + " Agent: " + agent + " pluginId:" + pluginID);
                        removeSubscribe(subscriberMap);
                    }

                }
            }

        }catch (Exception ex) {
            logger.error("filerepo error", ex);
        }
    }

    //data transfer
    public void confirmTransfer(String incomingTransferId, String failedFilesJson, String region, String agent, String pluginId) {
        try{

            String repoId = region + "-" + agent + "-" + pluginId;
            synchronized (lockPeerVersionMap) {
                peerVersionMap.put(repoId,incomingTransferId);
            }

            Set<String> failedFiles = new HashSet<>();
            if (failedFilesJson != null) {
                try {
                    List<String> failedList = gson.fromJson(failedFilesJson,
                            new com.google.gson.reflect.TypeToken<List<String>>(){}.getType());
                    if (failedList != null) failedFiles.addAll(failedList);
                } catch (Exception pe) {
                    logger.error("confirmTransfer: unparseable failedfiles from " + repoId, pe);
                }
            }

            Map<String, FileObject> offered = pendingTransferMap.get(incomingTransferId);
            if (offered != null) {
                int confirmed = 0;
                for (FileObject fileObject : offered.values()) {
                    if (!failedFiles.contains(fileObject.filePath)) {
                        dbEngine.updateFile(fileObject.filePath, fileObject.MD5, 1, fileObject.lastModified, fileObject.filesize);
                        confirmed++;
                    }
                }
                logger.info("Transfer " + incomingTransferId + " confirmed by " + repoId + ": "
                        + confirmed + " in sync, " + failedFiles.size() + " failed"
                        + (failedFiles.isEmpty() ? "" : " (will re-offer)"));
            } else {
                logger.debug("confirmTransfer: unknown/expired transfer_id " + incomingTransferId + " from " + repoId);
            }

        } catch (Exception ex) {
            logger.error(ex.getMessage());
            logger.error("filerepo error", ex);
        }

    }

    public void getFileRepoDiff(String repoDiffString, String transferId, String region, String agent, String pluginId) {
        try {

            String repoId = region + "-" + agent + "-" + pluginId;
            Map<String,String> update = new HashMap<>();
            update.put(transferId,repoDiffString);

            synchronized (lockPeerUpdateQueueMap) {
                if(!peerUpdateQueueMap.containsKey(repoId)) {
                    peerUpdateQueueMap.put(repoId,new LinkedList());

                }
                logger.debug("getFileRepoDiff() adding transfer_id: " + transferId + " to queueMap");
                peerUpdateQueueMap.get(repoId).add(update);
            }

            //if updater for specific id is not active, activate it

            boolean startUpdater = false;
            synchronized (lockPeerUpdateStateMap) {

                if(!peerUpdateStateMap.containsKey(repoId)) {
                    // true: an updater IS being started; false here spawned a duplicate updater
                    // for every diff that arrived while the first was still draining
                    peerUpdateStateMap.put(repoId,true);
                    startUpdater = true;
                } else {
                    if(!peerUpdateStateMap.get(repoId)) {
                        peerUpdateStateMap.put(repoId,true);
                        startUpdater = true;
                    }
                }
            }

            if(startUpdater) {
                logger.debug("starting new updater thread for repoId: " + repoId + " transfer id: " + transferId );
                syncPool.submit(() -> {
                        try {

                            boolean workExist = true;
                            while(workExist && plugin.isActive()) {

                                Map<String, String> pendingUpdate = null;

                                synchronized (lockPeerUpdateQueueMap) {
                                    pendingUpdate = peerUpdateQueueMap.get(repoId).poll();
                                }

                                if(pendingUpdate == null) {

                                    // only go idle if nothing raced in between poll() and here;
                                    // otherwise that update would sit unserviced until the next diff
                                    synchronized (lockPeerUpdateStateMap) {
                                        synchronized (lockPeerUpdateQueueMap) {
                                            if (peerUpdateQueueMap.get(repoId).isEmpty()) {
                                                peerUpdateStateMap.put(repoId, false);
                                                workExist = false;
                                            }
                                        }
                                    }

                                } else {

                                    //get the update
                                    Map.Entry<String, String> entry = pendingUpdate.entrySet().iterator().next();
                                    String currentTransferId = entry.getKey();
                                    String diffJson = entry.getValue();

                                    //extract file objects
                                    Map<String,FileObject> remoteRepoFiles = gson.fromJson(diffJson, repoListType);

                                    logger.debug("UPDATING " + repoId + " transferid: " + currentTransferId);

                                    List<String> failedFiles = new ArrayList<>();

                                    for (Map.Entry<String, FileObject> diffEntry : remoteRepoFiles .entrySet()) {
                                        FileObject fileObject = diffEntry.getValue();

                                        // reject malformed peer entries (null fields) rather than let an
                                        // NPE abort the whole batch and drop every good file in this diff
                                        if (fileObject == null || fileObject.fileName == null
                                                || fileObject.MD5 == null || fileObject.filePath == null) {
                                            logger.warn("skipping malformed peer diff entry: " + diffEntry.getKey());
                                            if (fileObject != null && fileObject.filePath != null) {
                                                failedFiles.add(fileObject.filePath);
                                            }
                                            continue;
                                        }

                                        //public Path downloadRemoteFile(String remoteRegion, String remoteAgent, String remoteFilePath, String localFilePath) {
                                        File localDir = getRepoDir();
                                        logger.debug("localDir: " + localDir.getAbsolutePath());
                                        // CONTAINMENT: fileName comes from the peer's listing and may carry
                                        // subdirs (recursive repos), but must never escape the repo dir via
                                        // ".." or an absolute path (resolve() returns an absolute arg as-is)
                                        Path repoBase = localDir.toPath().toAbsolutePath().normalize();
                                        Path localPath = repoBase.resolve(fileObject.fileName).normalize();
                                        if (!localPath.startsWith(repoBase)) {
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
                                        logger.debug("localFilePath: " + localPath.toFile().getAbsolutePath());
                                        //check that file exists
                                        boolean downloadFile = true;
                                        if(localPath.toFile().exists()) {
                                            if(localPath.toFile().length() == fileObject.filesize) {
                                                if(fileObject.MD5.equals(plugin.getMD5(localPath.toAbsolutePath().toString()))) {
                                                    downloadFile = false;
                                                }
                                            }
                                        }

                                        if(downloadFile) {
                                            // create parent dirs so nested (recursive) repo layouts sync,
                                            // not just flat top-level files.
                                            try {
                                                Path parent = localPath.toAbsolutePath().getParent();
                                                if (parent != null) Files.createDirectories(parent);
                                            } catch (Exception mkdirEx) {
                                                logger.warn("could not create parent dir for " + localPath + ": " + mkdirEx.getMessage());
                                            }
                                            Path tmpFile = plugin.getAgentService().getDataPlaneService().downloadRemoteFile(region, agent, fileObject.filePath, localPath.toFile().getAbsolutePath());
                                            // A null return means this one file's transfer failed. Skip it and keep
                                            // going: one bad/slow file must not NPE-abort the whole batch (which would
                                            // wedge sync forever). Failed files are reported in the confirm so the
                                            // sender keeps them insync=0 and re-offers them next cycle.
                                            if (tmpFile == null) {
                                                logger.warn("filerepo download returned null, will retry next cycle: " + fileObject.filePath);
                                                failedFiles.add(fileObject.filePath);
                                            } else if (!fileObject.MD5.equals(plugin.getMD5(tmpFile.toFile().getAbsolutePath()))) {
                                                // verify what actually landed; keep a corrupt file and it would
                                                // wrongly count as synced forever
                                                logger.warn("filerepo download md5 mismatch, deleting + retry next cycle: " + fileObject.filePath);
                                                tmpFile.toFile().delete();
                                                failedFiles.add(fileObject.filePath);
                                            } else {
                                                logger.debug("Synced " + tmpFile.toFile().getAbsolutePath());
                                            }
                                        }

                                    }

                                    logger.debug("SENDING UPDATE " + repoId);

                                    MsgEvent filesConfirm = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC,region,agent,pluginId);
                                    filesConfirm.setParam("action", "repoconfirm");
                                    filesConfirm.setParam("transfer_id", currentTransferId);
                                    if (!failedFiles.isEmpty()) {
                                        filesConfirm.setCompressedParam("failedfiles", gson.toJson(failedFiles));
                                    }
                                    plugin.msgOut(filesConfirm);

                                }
                            }

                        } catch (Exception v) {
                            logger.error("filerepo sync updater error", v);
                            // never leave the state stuck 'active' after a crash or it wedges forever
                            synchronized (lockPeerUpdateStateMap) {
                                peerUpdateStateMap.put(repoId,false);
                            }
                        }
                });
            }


        } catch (Exception ex) {
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logger.error("getFileRepoDiff() " + errors.toString());

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

    /** True if the Derby catalog is queryable (distinguishes a dead DB from an empty one). */
    public boolean isCatalogHealthy() {
        return dbEngine.isCatalogHealthy();
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
        static final String CLEARED = "cleared", ONLY_COPY = "only-copy", BUSY = "busy", FAILED = "failed";
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
     * Wipe the repository: every cataloged file and every regular file under the repository directory. Correct for a
     * mirror, whose producer holds the same files; unrecoverable for an only copy, which is refused unless
     * {@code force}. The repository gate is taken exclusively BEFORE the check and held through the delete, and every
     * landing takes its shared side, so no file filerepo lands can arrive between the check and the delete.
     */
    public Clear clearRepo(boolean force) {
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

            //List all files recorded in repo and remove them (never one in a live-tier root: only a drop removes those)
            List<Map<String,String>> repoFileList = dbEngine.getRepoList();
            for(Map<String,String> filerecord : repoFileList) {
                File removeFile = Paths.get(filerecord.get("filepath")).toFile();
                dbEngine.deleteFile(removeFile.getAbsolutePath());
                if (liveRootConflict(removeFile.toPath()) == null) {
                    removeFile.delete();
                }
            }
            //delete any files or directories not recorded in repo dir (close the walk stream)
            long remaining = 0;
            try (java.util.stream.Stream<Path> walk = Files.walk(Paths.get(getRepoDir().getAbsolutePath()))) {
                for (Path p : (Iterable<Path>) walk::iterator) {
                    if (Files.isRegularFile(p) && !p.toFile().delete()) remaining++;
                }
            }
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

    public Boolean removeFile(String fileRepoName, String fileName) {
        boolean isRemoved = false;
        try {

            File repoRoot = getRepoDir();
            File checkFile = new File(repoRoot, fileName).getCanonicalFile();
            // path-traversal guard: the resolved target must stay inside the repo directory
            if (!checkFile.toPath().startsWith(repoRoot.getCanonicalFile().toPath())) {
                logger.error("path traversal blocked in removeFile: " + fileName);
                return false;
            }
            if(checkFile.exists()) {
                int deleteStatus = dbEngine.deleteFile(checkFile.getAbsolutePath());
                logger.debug("delete status: " + deleteStatus);
                isRemoved = checkFile.delete();
            }

        } catch (Exception ex) {
            logger.error("removeFile: " + ex.getMessage());
            isRemoved = false;
        }
        return isRemoved;
    }

    public Boolean putFiles(List<String> fileList, String repoName, boolean overwrite, boolean isLocal) {

        boolean isUploaded = false;
        // the shared side of the repository gate: nothing lands between a clear's check and its delete
        if (!enterGate()) {
            return false;
        }
        try {

            boolean isFault = false;

            for(String incomingFileName : fileList) {

                Path tmpFilePath = Paths.get(incomingFileName);

                logger.info("incoming file: " + tmpFilePath);

                String fileSavePath = getRepoDir().getAbsolutePath() + "/" + tmpFilePath.getFileName();
                File checkFile = new File(fileSavePath);

                if ((checkFile.exists() && overwrite) || (!checkFile.exists())) {

                    // before the first byte lands: from here on, this repository holds files only it has
                    markLanded();

                    File fileSaved = new File(fileSavePath);

                    //move file from temp to requested location
                    if(fileSaved.exists()) {
                        fileSaved.delete();
                    }

                    //if local copy, if remote move temp to correct
                    if(isLocal) {
                        Files.copy(tmpFilePath, fileSaved.toPath());
                    } else {
                        Files.move(tmpFilePath, fileSaved.toPath());
                    }

                    if (fileSaved.isFile()) {

                        // Catalog the landed file when no scanner will pick it up. Keying on
                        // enable_scan (default true) meant landing-only repos (which never schedule
                        // the scan timer) never indexed received files -> getfile returned empty.
                        if(fileScanTimer == null) {
                            String filePath = fileSaved.getAbsolutePath();
                            String MD5hash = plugin.getMD5(fileSavePath);
                            long lastModified = fileSaved.lastModified();
                            long filesize = fileSaved.length();

                            dbEngine.addFile(filePath, MD5hash, lastModified, filesize);
                        }

                    } else {
                        isFault = true;
                    }
                } else {
                    logger.info("file " + checkFile.getAbsolutePath() + " exist : " + checkFile.exists() + " overwrite=" + overwrite);
                }
            }

            if(!isFault) {
                isUploaded = true;
            }


        } catch(Exception ex){
            logger.error("filerepo error", ex);
        } finally {
            exitGate();
        }

        return isUploaded;
    }


    //sub functions
    private void updateSubscribe(Map<String, String> incomingMap) {

        try {
            if ((incomingMap.containsKey("repo_region_id")) && (incomingMap.containsKey("repo_agent_id")) && (incomingMap.containsKey("repo_plugin_id"))) {

                //don't include self
                if (!((plugin.getRegion().equals(incomingMap.get("repo_region_id"))) && (plugin.getAgent().equals(incomingMap.get("repo_agent_id"))) && (plugin.getPluginID().equals(incomingMap.get("repo_plugin_id"))))) {
                    subMessage(fileRepoName, incomingMap.get("repo_region_id"), incomingMap.get("repo_agent_id"), incomingMap.get("repo_plugin_id"), "subscribe");
                }

            } else {
                logger.error("not agent identification provided");
            }
        } catch (Exception ex) {
            logger.error("Failed to subscribe");
            logger.error(ex.getMessage());
        }

    }

    private void createSubListener(String filerepoName) {


        MessageListener ml = new MessageListener() {
            public void onMessage(Message msg) {
                try {

                    if (msg instanceof TextMessage) {
                        logger.debug(" SUB REC MESSAGE:" + ((TextMessage) msg).getText());
                        Map<String, String> incomingMap = gson.fromJson(((TextMessage) msg).getText(), mapType);
                        if(incomingMap.containsKey("action")) {
                            if(incomingMap.containsKey("filerepo_name")) {

                                String actionType = incomingMap.get("action");
                                switch (actionType) {
                                    case "discover":
                                        updateSubscribe(incomingMap);
                                        break;

                                    default:
                                        logger.error("unknown actionType: " + actionType);
                                        break;
                                }


                            } else {
                                logger.error("action called without filerepo_name");
                            }
                        } else {
                            logger.error("createSubListener no action in message");
                            logger.error(incomingMap.toString());
                        }

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

    private void createRepoSubListener(String filerepoName) {

        MessageListener ml = new MessageListener() {
            public void onMessage(Message msg) {
                try {

                    if (msg instanceof TextMessage) {
                        logger.debug(" REPO REC MESSAGE:" + ((TextMessage) msg).getText());
                        Map<String, String> incomingMap = gson.fromJson(((TextMessage) msg).getText(), mapType);
                        if(incomingMap.containsKey("action")) {
                            if(incomingMap.containsKey("filerepo_name")) {

                                String actionType = incomingMap.get("action");
                                switch (actionType) {
                                    case "subscribe":
                                        addSubscribe(incomingMap);
                                        break;
                                    case "unsubscribe":
                                        removeSubscribe(incomingMap);
                                        break;

                                    default:
                                        logger.error("unknown actionType: " + actionType);
                                        break;
                                }


                            } else {
                                logger.error("action called without filerepo_name");
                            }
                        } else {
                            logger.error("createRepoSubListener no action in message");
                        }

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
        String subKey = null;
        try {
                if ((incomingMap.containsKey("sub_region_id")) && (incomingMap.containsKey("sub_agent_id")) && (incomingMap.containsKey("sub_plugin_id"))) {

                    subKey = incomingMap.get("sub_region_id") + "_" + incomingMap.get("sub_agent_id") + "_" + incomingMap.get("sub_plugin_id");

                }

            } catch (Exception ex) {
            logger.error("could not generate sub key");
            logger.error(ex.getMessage());
        }

        return subKey;
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
                                } else {
                                    incomingMap.put("ts", String.valueOf(System.currentTimeMillis()));
                                    subscriberMap.put(subKey, incomingMap);
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
    private File getRootRepoDir() {
        File repoDir = null;
        try {

            String repoDirString =  plugin.getConfig().getStringParam("repo_dir", "filerepo");


            File tmpRepo = new File(repoDirString);
            if(tmpRepo.isDirectory()) {
                repoDir = tmpRepo;
            } else {
                tmpRepo.mkdir();
                repoDir = tmpRepo;
            }

        } catch(Exception ex) {
            logger.error("filerepo error", ex);
        }
        return repoDir;
    }

    public File getRepoDir() {
        File repoDir = null;
        try {

            String rootRepo = plugin.getConfig().getStringParam("scan_dir");
            if(rootRepo == null) {
                rootRepo = getRootRepoDir().getAbsolutePath();
            }
            File tmpRepo = new File(rootRepo);
            if(tmpRepo.isDirectory()) {
                repoDir = tmpRepo;
            } else {
                tmpRepo.mkdir();
                repoDir = tmpRepo;
            }

        } catch(Exception ex) {
            logger.error("filerepo error", ex);
        }
        return repoDir;
    }


}
