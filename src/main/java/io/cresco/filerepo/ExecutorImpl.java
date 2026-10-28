package io.cresco.filerepo;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
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
import java.io.RandomAccessFile;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
                  @CrescoParam(name = "action_pluginmd5", required = true, description = "plugin md5")},
        returns = @CrescoReturn(name = "jardata", type = "bytes", description = "the jar bytes")),
    @CrescoAction(name = "putjar", type = "EXEC",
        summary = "Write a plugin jar (inline bytes) into this repo, md5-verified.", why = "Publish a plugin bundle.",
        params = {@CrescoParam(name = "pluginname", required = true), @CrescoParam(name = "md5", required = true),
                  @CrescoParam(name = "jarfile", required = true), @CrescoParam(name = "version", required = true),
                  @CrescoParam(name = "jardata", required = true, type = "bytes", description = "jar bytes")},
        returns = @CrescoReturn(name = "uploaded", description = "plugin name on success")),
    @CrescoAction(name = "putfiles", type = "EXEC",
        summary = "Land pushed files (MsgEvent attachments) into a repo.", why = "Receiving side of a file push.",
        params = {@CrescoParam(name = "repo_name", required = true), @CrescoParam(name = "overwrite", type = "boolean")}),
    @CrescoAction(name = "putfilesremote", type = "EXEC",
        summary = "Push a set of files from this repo to another agent's repo.", why = "Distribute files across the mesh.",
        params = {@CrescoParam(name = "file_list", required = true, compressed = true, type = "object", description = "JSON list of paths"),
                  @CrescoParam(name = "dst_region", required = true), @CrescoParam(name = "dst_agent", required = true),
                  @CrescoParam(name = "dst_plugin", required = true), @CrescoParam(name = "repo_name", required = true)},
        returns = @CrescoReturn(name = "status", description = "10 request sent")),
    @CrescoAction(name = "streamfile", type = "EXEC",
        summary = "Stream a byte-range of a file over the dataplane in chunks.", why = "Move large artifacts efficiently.",
        params = {@CrescoParam(name = "file_path", required = true), @CrescoParam(name = "start_byte", required = true, type = "long"),
                  @CrescoParam(name = "byte_length", required = true, type = "long"), @CrescoParam(name = "transfer_id", required = true),
                  @CrescoParam(name = "ident_key", required = true), @CrescoParam(name = "ident_id", required = true),
                  @CrescoParam(name = "buffer_size", type = "int", description = "chunk size, default 32768")},
        returns = @CrescoReturn(name = "status", description = "10 accepted")),
    @CrescoAction(name = "streamfilecancel", type = "EXEC",
        summary = "Cancel an in-progress streamfile transfer.", why = "Stop a large transfer.",
        params = @CrescoParam(name = "transfer_id", required = true),
        returns = @CrescoReturn(name = "status_code", description = "10 cancelled")),
    @CrescoAction(name = "getscandir", type = "EXEC",
        summary = "Return this repo's configured scan directory.", why = "Discover where the repo watches.",
        returns = @CrescoReturn(name = "scan_dir", description = "the scan directory")),
    @CrescoAction(name = "removefile", type = "EXEC",
        summary = "Remove a file from a repo (file + catalog row).", why = "Delete an artifact.",
        params = {@CrescoParam(name = "repo_name", required = true), @CrescoParam(name = "file_name", required = true)},
        returns = @CrescoReturn(name = "status", description = "10 removed")),
    @CrescoAction(name = "clearrepo", type = "EXEC",
        summary = "Delete all files in a repo. An only copy (repostate ONLY_COPY) is refused unless force=true.",
        why = "Wipe a repo.",
        params = {@CrescoParam(name = "repo_name", required = true),
                  @CrescoParam(name = "force", type = "boolean", description = "clear an only copy anyway")},
        returns = @CrescoReturn(name = "status", description = "10 cleared; 9 refused (see refusal) or not cleared")),
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
    @CrescoAction(name = "liverealise", type = "EXEC",
        summary = "Keep a live-backed repo's copy live: realise it again at this node as the node itself (copies only).",
        why = "Keep the copy a live-backed repo is configured with available at this node.",
        params = {@CrescoParam(name = "lease_ms", type = "long", description = "requested lease"),
                  @CrescoParam(name = "request_id", description = "idempotency key: 1-64 letters, digits, - or _")},
        returns = {@CrescoReturn(name = "job_id", description = "the realise job"),
                   @CrescoReturn(name = "reply_status", description = "the realise reply's own status"),
                   @CrescoReturn(name = "status", description = "10 realise accepted; 8 bad params; 9 refused (see refusal); 11 the live-tier provider did not answer")}),
    @CrescoAction(name = "repolistin", type = "EXEC",
        summary = "Receive a producer's file-diff and pull changed files (sync consumer side).",
        why = "Directory-sync receiving half.",
        params = {@CrescoParam(name = "repolistin", required = true, compressed = true, type = "object", description = "JSON diff"),
                  @CrescoParam(name = "transfer_id", required = true)},
        returns = @CrescoReturn(name = "status_code", description = "10 accepted")),
    @CrescoAction(name = "repoconfirm", type = "EXEC",
        summary = "Acknowledge a peer received a transfer generation.", why = "Sync handshake.",
        params = @CrescoParam(name = "transfer_id", required = true)),
    @CrescoAction(name = "getmetrics", type = "EXEC",
        summary = "Return filerepo metrics (cataloged file count, in-flight transfers) as MeasurementEngine gauges JSON.",
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

    private PluginBuilder plugin;
    private CLogger logger;
    private Gson gson;
    private RepoEngine repoEngine;
    private Type listType;
    private final Type stringMapType = new com.google.gson.reflect.TypeToken<Map<String, String>>(){}.getType();
    // the live-tier client; may be null (no provider, ever)
    private final LiveTierClient liveTier;

    private final AtomicBoolean transferLock = new AtomicBoolean();

    private Map<String,StreamObject> transferStreams;

    // B-2 unified metrics: filerepo catalog size / in-flight transfers as MeasurementEngine gauges,
    // exposed via the standard getmetrics EXEC so they fold into the controller's metric inventory.
    private MeasurementEngine metricEngine;
    // set under `this` monitor in cleanup(); blocks a racing getmetrics from recreating the engine
    private boolean closed = false;

    // Bounded, named, daemon pool for byte-range streamers (replaces unbounded `new Thread()` per
    // transfer). Safeguards: inline reads (getfile/getjar) are capped to keep them off the heap for
    // large artifacts; anything bigger must use streamfile. Buffer size is tunable.
    private final ExecutorService transferPool;
    private final long maxInlineBytes;
    private final int defaultStreamBuffer;
    // Hard ceiling on a streamfile chunk: it + its framing header must fit one dataplane frame.
    private static final int MAX_STREAM_BUFFER = 786432; // 768 KB

    public ExecutorImpl(PluginBuilder pluginBuilder, RepoEngine repoEngine, LiveTierClient liveTier) {
        this.plugin = pluginBuilder;
        logger = plugin.getLogger(ExecutorImpl.class.getName(), CLogger.Level.Info);
        gson = new Gson();
        this.repoEngine = repoEngine;
        this.liveTier = liveTier;
        listType = new TypeToken<ArrayList<String>>(){}.getType();
        transferStreams = Collections.synchronizedMap(new HashMap<>());

        int maxThreads = (int) Math.max(2L, plugin.getConfig().getLongParam("transfer_threads", 8L));
        // core==max so up to maxThreads transfers run concurrently. With core<max and an unbounded
        // queue, ThreadPoolExecutor never starts more than core threads, so transfer_threads was
        // dead config and every streamfile serialized behind the one running transfer.
        ThreadPoolExecutor tp = new ThreadPoolExecutor(maxThreads, maxThreads, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                r -> { Thread t = new Thread(r, "filerepo-transfer"); t.setDaemon(true); return t; });
        tp.allowCoreThreadTimeOut(true);
        this.transferPool = tp;
        // Inline getfile returns the whole file in ONE control-plane RPC message, which the wss
        // frame caps at ~1MB (base64-inflated). Default the cap BELOW that so getfile can never
        // produce an oversized frame; anything larger returns status 5 -> use streamfile (which
        // has no size limit — it byte-ranges over the dataplane in chunks). Raise this only if the
        // wsapi frame limit is also raised.
        this.maxInlineBytes = plugin.getConfig().getLongParam("max_inline_bytes", 524288L); // 512 KB
        this.defaultStreamBuffer = (int)(long) plugin.getConfig().getLongParam("stream_buffer_size", 262144L); // 256 KB
    }

    /** Shut the transfer pool down; called from Plugin.isStopped(). */
    public void cleanup() {
        try {
            transferPool.shutdownNow();
        } catch (Exception ex) {
            logger.error("transferPool shutdown error", ex);
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
                metricEngine.setGauge("filerepo.active.transfers", "in-flight streamfile transfers", "filerepo", CMetric.MeasureClass.GAUGE_INT);
            }
            long fileCount = 0L;
            try { fileCount = repoEngine.getRepoCount(); } catch (Exception ignore) {}
            int active;
            synchronized (transferLock) { active = transferStreams.size(); }
            metricEngine.updateLongGauge("filerepo.files.count", fileCount);
            metricEngine.updateIntGauge("filerepo.active.transfers", active);
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
        long startTime = System.currentTimeMillis();
        //logger.error("return getrepofilelist start");

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
        //logger.error("return getrepofilelist end: time = " + (System.currentTimeMillis() - startTime));
        return msg;

    }

    private MsgEvent repoList(MsgEvent msg) {

        try {
            Map<String, List<Map<String, String>>> repoMap = new HashMap<>();
            List<Map<String, String>> pluginInventory = null;
            File repoDir = getRepoDir();
            if (repoDir != null) {
                pluginInventory = plugin.getPluginInventory(repoDir.getAbsolutePath());
            }

            repoMap.put("plugins", pluginInventory);

            List<Map<String, String>> repoInfo = getRepoInfo();
            repoMap.put("server", repoInfo);

            msg.setCompressedParam("repolist", gson.toJson(repoMap));
        }catch (Exception ex) {
            logger.error(ex.getMessage());
        }

        return msg;

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

    private File getRepoDir() {
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

    private MsgEvent putPluginJar(MsgEvent incoming) {

        File jarFileSaved = null;
        try {

            String pluginName = incoming.getParam("pluginname");
            String pluginMD5 = incoming.getParam("md5");
            String pluginJarFile = incoming.getParam("jarfile");
            String pluginVersion = incoming.getParam("version");
            byte[] jarData = incoming.getDataParam("jardata");
            // copy the (large, base64) payload out and drop it from the message early
            if(incoming.getParams().containsKey("jardata")) incoming.removeParam("jardata");

            if((pluginName != null) && (pluginMD5 != null) && (pluginJarFile != null) && (pluginVersion != null) && (jarData != null)) {

                jarFileSaved = safeRepoFile(getRepoDir(), pluginJarFile);
                if (jarFileSaved != null) {
                    // the shared side of the repository gate, as for putfiles: a jar never lands mid-clear
                    if (!repoEngine.enterGate()) {
                        jarFileSaved = null;
                        incoming.setParam("status","9");
                        incoming.setParam("status_desc","repository busy: a clear holds it");
                    } else {
                        try {
                            repoEngine.markLanded();
                            Files.write(jarFileSaved.toPath(), jarData);
                            if (jarFileSaved.isFile()) {
                                String md5 = plugin.getMD5(jarFileSaved.getAbsolutePath());
                                if (pluginMD5.equals(md5)) {
                                    incoming.setParam("uploaded", pluginName);
                                    incoming.setParam("status","10");
                                    incoming.setParam("status_desc","uploaded");
                                } else {
                                    // integrity check failed: drop the bad/partial file rather than serve it
                                    jarFileSaved.delete();
                                    incoming.setParam("status","9");
                                    incoming.setParam("status_desc","md5 mismatch, upload rejected");
                                }
                            } else {
                                incoming.setParam("status","9");
                                incoming.setParam("status_desc","write did not produce a file");
                            }
                        } finally {
                            repoEngine.exitGate();
                        }
                    }
                } else {
                    incoming.setParam("status","8");
                    incoming.setParam("status_desc","invalid jarfile name");
                }
            } else {
                incoming.setParam("status","7");
                incoming.setParam("status_desc","missing required putjar params");
            }

        } catch(Exception ex){
            logger.error("putPluginJar error", ex);
            // a partial/truncated jar must not be left where the inventory would advertise+serve it
            try { if (jarFileSaved != null) jarFileSaved.delete(); } catch (Exception ignore) {}
            incoming.setParam("status","6");
            incoming.setParam("status_desc","putjar error: " + ex.getMessage());
        }

        if(incoming.getParams().containsKey("jardata")) {
            incoming.removeParam("jardata");
        }

        return incoming;
    }

    private MsgEvent getPluginJar(MsgEvent incoming) {

        try {
            if ((incoming.getParam("action_pluginname") != null) && (incoming.getParam("action_pluginmd5") != null)) {
                String requestPluginName = incoming.getParam("action_pluginname");
                String requestPluginMD5 = incoming.getParam("action_pluginmd5");

                File repoDir = getRepoDir();
                if (repoDir != null) {

                    List<Map<String, String>> pluginInventory = plugin.getPluginInventory(repoDir.getAbsolutePath());
                    for (Map<String, String> repoMap : pluginInventory) {

                        if (repoMap.containsKey("pluginname") && repoMap.containsKey("md5") && repoMap.containsKey("jarfile")) {
                            String pluginName = repoMap.get("pluginname");
                            String pluginMD5 = repoMap.get("md5");
                            String pluginJarFile = repoMap.get("jarfile");

                            if (pluginName.equals(requestPluginName) && pluginMD5.equals(requestPluginMD5)) {

                                File jarFile = safeRepoFile(repoDir, pluginJarFile);
                                if (jarFile != null && jarFile.isFile() && jarFile.length() <= maxInlineBytes) {
                                    incoming.setDataParam("jardata", Files.readAllBytes(jarFile.toPath()));
                                } else if (jarFile != null && jarFile.length() > maxInlineBytes) {
                                    incoming.setParam("status_desc","jar " + jarFile.length()
                                            + " bytes exceeds max_inline_bytes; use streamfile");
                                }
                                // requested jar found+handled: stop scanning the rest of the inventory
                                break;

                            }
                        }

                    }

                }
            }
        } catch(Exception ex) {
            logger.error("getPluginJar error", ex);
        }
        return incoming;
    }

    /**
     * Resolve a child name against repoDir and guarantee the result stays inside repoDir
     * (path-traversal guard for names like "../../etc/x"). Returns null if it escapes.
     */
    private File safeRepoFile(File repoDir, String childName) {
        try {
            if (childName == null) return null;
            Path base = repoDir.getCanonicalFile().toPath();
            File candidate = new File(repoDir, childName).getCanonicalFile();
            if (candidate.toPath().startsWith(base)) {
                return candidate;
            }
            logger.error("path traversal blocked: '" + childName + "' escapes repo dir " + base);
        } catch (Exception ex) {
            logger.error("safeRepoFile error for " + childName, ex);
        }
        return null;
    }

    private MsgEvent getFile(MsgEvent incoming) {

        try {

            if(incoming.getParam("file_path") != null){
                String filePath = incoming.getParam("file_path");
                String conflict = repoEngine.liveRootConflict(Paths.get(filePath));
                if (conflict != null) {
                    return refuseLiveRoot(incoming, "the file " + conflict);
                }
                //logger.error("file_name: " + filename);

                ///opt/cresco/filerepo/extract_dump_7e90b402-8076-11ec-bc89-0242ac11001a.json
                //Path filePath = Paths.get(repoEngine.getRepoDir().getAbsolutePath(), filename);
                //incoming.setParam("file_path",filePath.toFile().getAbsolutePath());
                //logger.error("file_path: " + filePath.toFile().getAbsolutePath());
                Map<String,String> fileInfo = repoEngine.getFileInfo(filePath);

                if(fileInfo != null) {
                    File f = new File(filePath);
                    if (!f.isFile()) {
                        incoming.setParam("status","6");
                        incoming.setParam("status_desc","file missing on disk");
                    } else if (f.length() > maxInlineBytes) {
                        // Memory safeguard: never slurp a huge file onto the heap for an inline reply.
                        incoming.setParam("status","5");
                        incoming.setParam("status_desc","file " + f.length() + " bytes exceeds max_inline_bytes "
                                + maxInlineBytes + "; use streamfile");
                    } else {
                        incoming.setCompressedParam("file_metadata",gson.toJson(fileInfo));
                        incoming.setDataParam("file_data", Files.readAllBytes(f.toPath()));
                        incoming.setParam("status","10");
                        incoming.setParam("status_desc","found list");
                    }
                } else {
                    incoming.setParam("status","9");
                    incoming.setParam("status_desc","fileInfo == null");
                }
            } else {
                incoming.setParam("status","8");
                incoming.setParam("status_desc","no filename parameter");
            }

        } catch(Exception ex) {
            incoming.setParam("status","7");
            incoming.setParam("status_desc","getFile error " + ex.getMessage());
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

    private void streamFile(Map<String,String> transferInfo) {
        String transferId = transferInfo.get("transfer_id");
        int BUFFER_SIZE = Integer.parseInt(transferInfo.get("buffer_size"));
        //logger.info("transferid: " + transferId + " BUFFER_SIZE: " + BUFFER_SIZE + " TRANSFERINFO: " + transferInfo);

        try {

            transferPool.submit(() -> {
                try {

                    boolean alerted = false;
                    long startByte = Long.parseLong(transferInfo.get("start_byte"));
                    long byteLength = Long.parseLong(transferInfo.get("byte_length"));
                    String filePath = transferInfo.get("file_path");
                    StreamObject streamObject = new StreamObject(transferId, filePath, startByte, byteLength);
                    synchronized (transferLock) {
                        transferStreams.put(transferId, streamObject);
                    }

                    // try-with-resources: the RAF is closed even if the loop throws (previously it
                    // leaked the descriptor on any exception mid-transfer).
                    try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
                        raf.seek(startByte);

                        int seqNum = 0;
                        byte[] buffer = new byte[BUFFER_SIZE];
                        int read;
                        boolean isActive = true;
                        while((byteLength > 0) && (isActive)){
                            BytesMessage updateMessage = plugin.getAgentService().getDataPlaneService().createBytesMessage();
                            read = (int) Math.min((long) BUFFER_SIZE, byteLength);
                            // Honor the ACTUAL bytes read: RandomAccessFile.read() may return a short
                            // read (< requested); writing the requested length shipped stale bytes.
                            int got = raf.read(buffer, 0, read);
                            if (got <= 0) { break; }
                            updateMessage.writeBytes(buffer, 0, got);
                            updateMessage.setStringProperty(transferInfo.get("ident_key"), transferInfo.get("ident_id"));
                            updateMessage.setStringProperty("transfer_id", transferId);
                            updateMessage.setStringProperty("seq_num", String.valueOf(seqNum));
                            plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL,updateMessage, DeliveryMode.NON_PERSISTENT, 0, 0);
                            byteLength = byteLength - got;
                            seqNum += 1;

                            synchronized (transferLock) {
                                if(transferStreams.containsKey(transferId)) {
                                    transferStreams.get(transferId).setBytesTransfered(transferStreams.get(transferId).getBytesTransfered() + got);
                                    isActive = transferStreams.get(transferId).isActive();
                                    if(!alerted) {
                                        if (transferStreams.get(transferId).getBytesTransfered() > (1024)) {
                                            logger.debug("streamFile transferId: " + transferId + " bytesTransfered: " + transferStreams.get(transferId).getBytesTransfered());
                                            alerted = true;
                                        }
                                    }
                                } else {
                                    logger.error("streamFile transferId: " + transferId + " not found in transferStreams");
                                }
                            }
                        }
                    }

                } catch(Exception ex) {
                    logger.error("streamFile error: " + ex.getMessage());
                    synchronized (transferLock) {
                        if(transferStreams.containsKey(transferId)) {
                            transferStreams.get(transferId).setActive(false);
                        }
                    }
                } finally {
                    // Remove the finished/failed transfer so the map doesn't grow unbounded
                    // (this cleanup was previously commented out -> a memory leak per transfer).
                    synchronized (transferLock) {
                        transferStreams.remove(transferId);
                    }
                }
            });


        } catch (Exception ex) {
            logger.error("Error streamFile(Map<String,String> transferInfo)", ex);
        }

    }
    private MsgEvent streamFile(MsgEvent incoming) {

        try {

            if(incoming.getParam("file_path") != null) {
                String filePath = incoming.getParam("file_path");
                String conflict = repoEngine.liveRootConflict(Paths.get(filePath));
                if (conflict != null) {
                    return refuseLiveRoot(incoming, "the file " + conflict);
                }

                File file = new File(filePath);
                if(file.exists()) {
                    long startByte = Long.parseLong(incoming.getParam("start_byte"));
                    long byteLength = Long.parseLong(incoming.getParam("byte_length"));

                    // overflow-safe range validation: reject a bad range up-front instead of replying
                    // "10 accepted" and then failing silently on the async streamer (start+len could
                    // also overflow to a negative endByte that slips past a <= file.length() check)
                    if(startByte >= 0 && byteLength > 0 && startByte <= file.length()
                            && byteLength <= file.length() - startByte) {

                        Map<String, String> fileInfo = repoEngine.getFileInfo(filePath);
                        if (fileInfo != null) {
                            fileInfo.put("transfer_id", incoming.getParam("transfer_id"));
                            fileInfo.put("file_path", incoming.getParam("file_path"));
                            fileInfo.put("start_byte", incoming.getParam("start_byte"));
                            fileInfo.put("byte_length", incoming.getParam("byte_length"));
                            fileInfo.put("ident_key", incoming.getParam("ident_key"));
                            fileInfo.put("ident_id", incoming.getParam("ident_id"));

                            // Clamp the chunk size: each chunk carries a small framing header and must
                            // stay under the dataplane frame limit, else chunks are silently dropped.
                            // A caller passing an oversized buffer_size must not be able to break the
                            // transfer — cap it (and floor it) to a safe range.
                            int reqBuf = defaultStreamBuffer;
                            try { if (incoming.getParam("buffer_size") != null) reqBuf = Integer.parseInt(incoming.getParam("buffer_size")); }
                            catch (Exception ignore) { }
                            if (reqBuf > MAX_STREAM_BUFFER) reqBuf = MAX_STREAM_BUFFER;
                            if (reqBuf < 4096) reqBuf = 4096;
                            fileInfo.put("buffer_size", String.valueOf(reqBuf));

                            //transfer in new thread, send recept
                            streamFile(fileInfo);
                            //logger.error("transferid: " + incoming.getParam("transfer_id") + " START");

                            incoming.setParam("status", "10");
                            incoming.setParam("status_desc", "transfer accepted");
                        } else {
                            incoming.setParam("status", "9");
                            incoming.setParam("status_desc", "file not in catalog");
                        }
                    } else {
                        incoming.setParam("status", "8");
                        incoming.setParam("status_desc", "invalid byte range");
                    }
                } else {
                    incoming.setParam("status","7");
                    incoming.setParam("status_desc","no filename parameter");
                }
            } else {
                incoming.setParam("status","6");
                incoming.setParam("status_desc","file does not exists on OS");
            }

        } catch(Exception ex) {
            incoming.setParam("status","5");
            incoming.setParam("status_desc","getFile error " + ex.getMessage());
            logger.error("filerepo error", ex);
        }
        return incoming;
    }

    private MsgEvent streamFileCancel(MsgEvent incoming) {
        try {
            incoming.setParam("status_code","9");
            logger.debug("streamFileCancel transferid: " + incoming.getParam("transfer_id"));

            if(incoming.paramsContains("transfer_id")) {
                String transferId = incoming.getParam("transfer_id");
                synchronized (transferLock) {
                    if(transferStreams.containsKey(transferId)) {
                        transferStreams.get(transferId).setActive(false);
                        incoming.setParam("status_code","10");
                        logger.info("streamFileCancel transferId: " + transferId + " set canceled transfered " + transferStreams.get(transferId).getBytesTransfered() + " bytes." );
                    } else {
                        logger.error("streamFileCancel transferId: " + transferId + " not found in transferStreams!");
                    }
                }
            } else {
                logger.error("streamFileCancel transferId not found in MsgEvent!");
            }

        } catch(Exception ex) {
            incoming.setParam("status","7");
            incoming.setParam("status_desc","getFile error " + ex.getMessage());
            logger.error("filerepo error", ex);
        }
        return incoming;
    }


    private void confirmTransfer(MsgEvent incoming) {
        repoEngine.confirmTransfer(incoming.getParam("transfer_id"), incoming.getCompressedParam("failedfiles"), incoming.getSrcRegion(), incoming.getSrcAgent(), incoming.getSrcPlugin());
    }

    private MsgEvent putFileRemote(MsgEvent incoming) {

        try {

            if(incoming.paramsContains("file_list") && incoming.paramsContains("dst_region")
                    && incoming.paramsContains("dst_agent") && incoming.paramsContains("dst_plugin")
                    && incoming.paramsContains("repo_name") )  {

                String fileListString = incoming.getCompressedParam("file_list");
                List<String> fileList = gson.fromJson(fileListString, listType);
                // these are any paths the caller names: none may lie in a live-tier root
                for (String f : fileList == null ? Collections.<String>emptyList() : fileList) {
                    String conflict = repoEngine.liveRootConflict(Paths.get(f));
                    if (conflict != null) {
                        return refuseLiveRoot(incoming, "a listed file " + conflict);
                    }
                }
                String dst_region = incoming.getParam("dst_region");
                String dst_agent = incoming.getParam("dst_agent");
                String dst_plugin = incoming.getParam("dst_plugin");
                String repo_name = incoming.getParam("repo_name");

                MsgEvent filesTransfer = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC,dst_region, dst_agent, dst_plugin);
                filesTransfer.setParam("action", "putfiles");
                filesTransfer.setParam("repo_name",repo_name);
                filesTransfer.addFiles(fileList);
                logger.info("File Transfer Params: " + filesTransfer.getParams().toString());
                plugin.msgOut(filesTransfer);
                //plugin.sendRPC(filesTransfer);


                incoming.setParam("status","10");
                incoming.setParam("status_desc","request sent");

            } else {
                logger.error("filelist not found");
            }

        } catch(Exception ex){
            logger.error("putFileRemote: " + ex.getMessage());
            logger.error("filerepo error", ex);
        }

        if(incoming.getParams().containsKey("filedata")) {
            incoming.removeParam("filedata");
        }

        return incoming;
    }

    private MsgEvent clearRepo(MsgEvent incoming) {

        try {

            if(incoming.paramsContains("repo_name")){

                String repoName = incoming.getParam("repo_name");

                // force=true clears an only copy too: whoever sends it has decided the contents are disposable.
                // The check runs inside clearRepo, under the repository gate, never before it.
                boolean force = "true".equalsIgnoreCase(incoming.getParam("force"));
                RepoEngine.Clear cleared = repoEngine.clearRepo(force);
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
                                + cleared.why + "); pass force=true to clear it anyway");
                        incoming.setParam("refusal","only-copy");
                        break;
                    default:
                        incoming.setParam("status","9");
                        incoming.setParam("status_desc","repo not cleared: " + cleared.why);
                        break;
                }

            } else {
                logger.error("No repo name found");
                incoming.setParam("status","9");
                incoming.setParam("status_desc","No repo or file name name found");
            }

        } catch(Exception ex){
            logger.error("clearrepo: " + ex.getMessage());
            incoming.setParam("status","8");
            incoming.setParam("status_desc","clear repo error: " + ex.getMessage());
        }

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

            if(incoming.paramsContains("repo_name") && incoming.paramsContains("file_name")){

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
                logger.error("No repo name found");
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

    private MsgEvent putFiles(MsgEvent incoming) {

        try {
            //String fileName = incoming.getParam("filename");
            //String fileMD5 = incoming.getParam("md5");
            //byte[] fileData = incoming.getDataParam("filedata");
            //String filePath = incoming.getFileList().get(0);
            List<String> fileList = incoming.getFileList();

            boolean overwrite = false;
            boolean isLocal = false;

            if((fileList != null) && incoming.paramsContains("repo_name")){

                String repoName = incoming.getParam("repo_name");

                // a landing from this agent copies the paths the caller names: none may lie in a live-tier root
                for (String f : fileList) {
                    String conflict = repoEngine.liveRootConflict(Paths.get(f));
                    if (conflict != null) {
                        return refuseLiveRoot(incoming, "a file to land " + conflict);
                    }
                }


                try{
                    if(incoming.getParam("overwrite") != null) {
                        overwrite = Boolean.parseBoolean(incoming.getParam("overwrite"));
                    }
                } catch(Exception ex){
                    logger.error("filerepo error", ex);
                }

                if((incoming.getSrcAgent().equals(incoming.getDstAgent())) && (incoming.getSrcRegion().equals(incoming.getDstRegion()))) {
                    isLocal = true;
                }

                if(repoEngine.putFiles(fileList,repoName, overwrite, isLocal)) {
                    MsgEvent filesConfirm = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC,incoming.getSrcRegion(),incoming.getSrcAgent(),incoming.getSrcPlugin());
                    filesConfirm.setParam("action", "repoconfirm");
                    filesConfirm.setParam("transfer_id", incoming.getParam("transfer_id"));
                    plugin.msgOut(filesConfirm);
                    logger.debug("putfiles stored, confirmation sent");
                    incoming.setParam("status","10");
                    incoming.setParam("status_desc","putfiles stored");
                } else {
                    logger.error("putfiles failed for repo " + repoName);
                    incoming.setParam("status","9");
                    incoming.setParam("status_desc","putfiles failed");
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
            String transferId = incoming.getParam("transfer_id");

            if((repoListStringIn != null) && (transferId != null)) {

                //this needs to be a new thread
                repoEngine.getFileRepoDiff(repoListStringIn, transferId, incoming.getSrcRegion(), incoming.getSrcAgent(), incoming.getSrcPlugin());

                incoming.setParam("status_code","10");
                incoming.setParam("status_desc","New transferID accepted");

                //logger.info("repoListIn OK");
                //String repoDiffString = repoEngine.getFileRepoDiff(repoIn,repoListStringIn);
                //incoming.setCompressedParam("repodiff",repoDiffString);


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
        return incoming;
    }

}