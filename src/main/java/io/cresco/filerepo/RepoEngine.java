package io.cresco.filerepo;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.library.app.gEdge;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RepoEngine {

    private PluginBuilder plugin;
    private CLogger logger;
    private Gson gson;
    private Type crescoType;

    private Type repoListType;

    private AtomicBoolean inScan = new AtomicBoolean(false);

    private AtomicBoolean lockFileMap = new AtomicBoolean();
    private Map<String, Map<String,FileObject>> fileMap;

    private AtomicBoolean lockPeerVersionMap = new AtomicBoolean();
    private Map<String, String> peerVersionMap;

    private AtomicBoolean lockPeerUpdateStateMap = new AtomicBoolean();
    private Map<String, Boolean> peerUpdateStateMap;

    private AtomicBoolean lockPeerUpdateQueueMap = new AtomicBoolean();
    private Map<String, Queue<Map<String,String>>> peerUpdateQueueMap;

    private Type edgeType;
    private Type mapType;

    private Timer fileScanTimer;

    private String scanDirString;

    private List<Map<String,String>> iNodeList;

    private int transferId = -1;

    private  DBEngine dbEngine;

    public RepoEngine(PluginBuilder pluginBuilder, DBEngine dbEngine) {

        this.plugin = pluginBuilder;
        logger = plugin.getLogger(RepoEngine.class.getName(), CLogger.Level.Info);
        this.dbEngine = dbEngine;
        gson = new Gson();

        this.crescoType = new TypeToken<Map<String, List<Map<String, String>>>>() {
        }.getType();

        this.repoListType = new TypeToken<Map<String,FileObject>>() {
        }.getType();

        this.edgeType = new TypeToken<List<gEdge>>() {
        }.getType();

        this.mapType = new TypeToken<Map<String,String>>() {
        }.getType();

        iNodeList = new ArrayList<>();

        fileMap = Collections.synchronizedMap(new HashMap<>());
        peerVersionMap = Collections.synchronizedMap(new HashMap<>());
        peerUpdateStateMap = Collections.synchronizedMap(new HashMap<>());
        peerUpdateQueueMap = Collections.synchronizedMap(new HashMap<>());

        scanDirString =  plugin.getConfig().getStringParam("scan_dir");


    }

    public void confirmTransfer(String incomingTransferId, String region, String agent, String pluginId) {
        try{

            String repoId = region + "-" + agent + "-" + pluginId;
            synchronized (lockPeerVersionMap) {
                peerVersionMap.put(repoId,incomingTransferId);
            }

        } catch (Exception ex) {
            logger.error(ex.getMessage());
            ex.printStackTrace();
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
                        peerUpdateStateMap.put(repoId,false);
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
                    new Thread() {
                        public void run() {
                            try {

                                boolean workExist = true;
                                while(workExist && plugin.isActive()) {

                                    Map<String, String> update = null;

                                    synchronized (lockPeerUpdateQueueMap) {
                                       update = peerUpdateQueueMap.get(repoId).poll();
                                    }

                                    if(update == null) {

                                        workExist = false;

                                    } else {

                                        //get the update
                                        Map.Entry<String, String> entry = update.entrySet().iterator().next();
                                        String currentTransferId = entry.getKey();
                                        String repoDiffString = entry.getValue();

                                        //extract file objects
                                        Map<String,FileObject> remoteRepoFiles = gson.fromJson(repoDiffString, repoListType);

                                        logger.debug("UPDATING " + repoId + " transferid: " + currentTransferId);

                                        for (Map.Entry<String, FileObject> diffEntry : remoteRepoFiles .entrySet()) {
                                            String fileName = diffEntry.getKey();
                                            FileObject fileObject = diffEntry.getValue();

                                            //public Path downloadRemoteFile(String remoteRegion, String remoteAgent, String remoteFilePath, String localFilePath) {
                                            File localDir = getRepoDir();
                                            logger.debug("localDir: " + localDir.getAbsolutePath());
                                            Path localPath = Paths.get(localDir.getAbsolutePath() + "/" + fileObject.fileName);
                                            logger.debug("localFilePath: " + localPath.toFile().getAbsolutePath());

                                            Path tmpFile = plugin.getAgentService().getDataPlaneService().downloadRemoteFile(region,agent,fileObject.filePath, localPath.toFile().getAbsolutePath());
                                            logger.debug("Synced " + tmpFile.toFile().getAbsolutePath());
                                        }

                                        logger.debug("SENDING UPDATE " + repoId);

                                        MsgEvent filesConfirm = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC,region,agent,pluginId);
                                        filesConfirm.setParam("action", "repoconfirm");
                                        filesConfirm.setParam("transfer_id", currentTransferId);
                                        plugin.msgOut(filesConfirm);

                                    }
                                }

                                synchronized (lockPeerUpdateStateMap) {
                                    peerUpdateStateMap.put(repoId,false);
                                }

                            } catch (Exception v) {
                                logger.error(v.getMessage());
                            }
                        }
                    }.start();
                }


        } catch (Exception ex) {
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logger.error("getFileRepoDiff() " + errors.toString());

        }

    }

    public String getFileRepoDiff(String repoIn, String repoDiffString) {
        String repoDiffStringOut = null;
        try {
            Map<String, FileObject> myRepoFiles = null;
            synchronized (lockFileMap) {
                if (fileMap.containsKey(repoIn)) {
                    myRepoFiles = new HashMap<>();
                    myRepoFiles.putAll(fileMap.get(repoIn));
                }

                if(myRepoFiles != null) {

                    Map<String,FileObject> remoteRepoFiles = gson.fromJson(repoDiffString, repoListType);

                    for (Map.Entry<String, FileObject> entry : myRepoFiles.entrySet()) {
                        String fileName = entry.getKey();
                        FileObject fileObject = entry.getValue();

                        if(remoteRepoFiles.containsKey(fileName)) {
                            if(remoteRepoFiles.get(fileName).MD5.equals(fileObject.MD5)) {
                               remoteRepoFiles.remove(fileName);
                            }
                        }

                    }

                    repoDiffStringOut = gson.toJson(remoteRepoFiles);

                } else {
                    //repo does not exist, send everything
                    repoDiffStringOut =  repoDiffString;
                }

            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return repoDiffStringOut;
    }

    private Map<String,FileObject> buildRepoList() {

        Map<String,FileObject> fileDiffMap = null;
        try {

            fileDiffMap = new HashMap<>();

            //get all files in the scan directory
            File folder = new File(scanDirString);
            File[] listOfFiles = folder.listFiles();

            if(listOfFiles != null) {
                for (int i = 0; i < listOfFiles.length; i++) {
                    if (listOfFiles[i].isFile()) {
                        String fileName = listOfFiles[i].getName();
                        String filePath = listOfFiles[i].getAbsolutePath();
                        long lastModified = listOfFiles[i].lastModified();

                        boolean add = false;
                        boolean update = false;

                        //see if file is in the database
                        long lastModifiedDb = dbEngine.getLastModified(filePath);
                        logger.trace("file: " + filePath + " lastmodified: " + lastModified + " dblastmodified: " + lastModifiedDb);
                        if (lastModifiedDb == -1) {
                            add = true;
                        } else if (lastModifiedDb < lastModified) {
                            update = true;
                        } else if (lastModifiedDb > lastModified) {
                            logger.error("How can an older file be recored in DB? lastModifiedDb > lastModified ");
                            update = true;
                        }

                        String MD5hash = plugin.getMD5(filePath);
                        logger.trace("fileName:" + filePath + " MD5:" + MD5hash + " filepath:" + filePath);

                        if (add || update) {
                            FileObject fileObject = new FileObject(fileName, MD5hash, filePath, lastModified);
                            fileDiffMap.put(filePath, fileObject);
                        }

                        if (add) {
                            dbEngine.addFile(filePath, MD5hash, lastModified);
                        }

                        if (update) {
                            dbEngine.updateFile(filePath, MD5hash, 0, lastModified);
                        }
                    }
                }
            }

        }catch (Exception ex) {
            logger.error(ex.getMessage());
            ex.printStackTrace();
        }
        return fileDiffMap;
    }

    public void startScan() {
        long delay  = 5000L;
        //long period = 15000L;

        long period =  plugin.getConfig().getLongParam("scan_period", 15000L);

        if(scanDirString != null) {
        //if(scanDirString != null) {
        logger.debug("Starting file scan : " + scanDirString);
            startScan(delay, period);
        }

    }

    private void syncRegionFiles(Map<String,FileObject> fileDiffMap) {
        String returnString = null;
        try {
                if(iNodeList.size() == 0) {
                    updateNodeMap();
                }

                        for (Map<String, String> repoMap : iNodeList) {

                            if ((plugin.getRegion().equals(repoMap.get("region_id"))) && (plugin.getAgent().equals(repoMap.get("agent_id"))) && (plugin.getPluginID().equals(repoMap.get("plugin_id")))) {
                                //do nothing if self
                                //logger.info("found self");
                                //} else if (plugin.getRegion().equals(repoMap.get("region_id"))) {
                            } else {
                                //This is another filerepo in my region, I need to send it data
                                String region = repoMap.get("region_id");
                                String agent = repoMap.get("agent_id");
                                String pluginID = repoMap.get("plugin_id");

                                logger.debug("SEND :" + region + " " + agent + " " + pluginID + " data");

                                MsgEvent fileRepoRequest = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC,region,agent,pluginID);
                                fileRepoRequest.setParam("action","repolistin");
                                //String repoListStringIn = getFileRepoList(scanRepo);
                                String repoListStringIn = gson.toJson(fileDiffMap);
                                fileRepoRequest.setCompressedParam("repolistin",repoListStringIn);
                                fileRepoRequest.setParam("transfer_id", String.valueOf(transferId));

                                logger.debug("repoListStringIn: " + repoListStringIn);

                                MsgEvent fileRepoResponse = plugin.sendRPC(fileRepoRequest);

                                if(fileRepoResponse != null) {

                                    logger.debug("Host Region: " + region + " Agent: " + agent + " pluginId:" + pluginID + " responded");

                                    if(fileRepoResponse.paramsContains("status_code") && fileRepoResponse.paramsContains("status_desc")) {
                                        int status_code = Integer.parseInt(fileRepoResponse.getParam("status_code"));
                                        String status_desc = fileRepoResponse.getParam("status_code");
                                        if(status_code != 10) {
                                            logger.error("Region: " + region + " Agent: " + agent + " pluginId:" + pluginID + " filerepo update failed status_code: " + status_code + " status_desc:" + status_desc);
                                        } else {
                                            for (Map.Entry<String, FileObject> entry : fileDiffMap.entrySet()) {
                                                //String key = entry.getKey();
                                                FileObject fileObject = entry.getValue();
                                                dbEngine.updateFile(fileObject.filePath, fileObject.MD5, 1, fileObject.lastModified);
                                            }
                                            logger.info("Transfered " + fileDiffMap.size() + " files to " + pluginID);
                                        }
                                    }


                                } else {
                                    logger.error("Host Region: " + region + " Agent: " + agent + " pluginId:" + pluginID + " failed to respond!");
                                }
                            }
                        }

        }catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void startScan(long delay, long period) {

        TimerTask fileScanTask = new TimerTask() {
            public void run() {
                try {

                    if(plugin.isActive()) {

                            if (!inScan.get()) {

                                //logger.error("\t\t ***STARTING SCAN " + inScan.get() + " tid:" + transferId);
                                inScan.set(true);

                                //logger.error("\t\t ***STARTED SCAN " + inScan.get());
                                //build file list
                                Map<String, FileObject> diffList = buildRepoList();
                                if (diffList.size() > 0) {
                                    //start sync
                                    transferId++;
                                    //find other repos
                                    logger.debug("SYNC Files");
                                    syncRegionFiles(diffList);
                                }

                                //logger.error("\t\t ***ENDING SCAN " + inScan.get());
                                //inScan.set(false);
                                //logger.error("\t\t ***ENDED SCAN " + inScan.get());

                                inScan.set(false);

                            } else {
                                logger.error("\t\t ***ALREADY IN SCAN");
                            }

                    } else {
                        logger.error("NO ACTIVE");
                    }

                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        };

        fileScanTimer = new Timer("Timer");
        fileScanTimer.scheduleAtFixedRate(fileScanTask, delay, period);
        logger.debug("filescantimer : set : " + period);
    }

    public void stopScan() {
        if(fileScanTimer != null) {
            fileScanTimer.cancel();
        }
    }

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
            ex.printStackTrace();
        }
        return repoDir;
    }

    private File getRepoDir() {
        File repoDir = null;
        try {

            String rootRepo = getRootRepoDir().getAbsolutePath();

            File tmpRepo = new File(rootRepo);
            if(tmpRepo.isDirectory()) {
                repoDir = tmpRepo;
            } else {
                tmpRepo.mkdir();
                repoDir = tmpRepo;
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return repoDir;
    }

    public Boolean putFiles(List<String> fileList, String repoName, boolean overwrite, boolean isLocal) {

        boolean isUploaded = false;
        try {

            boolean isFault = false;

            for(String incomingFileName : fileList) {

                Path tmpFilePath = Paths.get(incomingFileName);

                String fileSavePath = getRepoDir().getAbsolutePath() + "/" + tmpFilePath.getFileName();
                File checkFile = new File(fileSavePath);

                if ((!checkFile.exists()) || (overwrite)) {

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
                        String md5 = plugin.getMD5(fileSavePath);

                            //given there is no last modified provided by the transfer, we will create a last modified locally time
                            FileObject fileObject = new FileObject(fileSaved.getName(), md5, fileSavePath, System.currentTimeMillis());

                            synchronized (lockFileMap) {
                                if (!fileMap.containsKey(repoName)) {
                                    Map<String, FileObject> repoFileMap = new HashMap<>();
                                    repoFileMap.put(fileSaved.getName(), fileObject);
                                    fileMap.put(repoName, repoFileMap);
                                } else {
                                    fileMap.get(repoName).put(fileSaved.getName(), fileObject);
                                }
                            }

                    } else {
                        isFault = true;
                    }
                } else {
                    logger.info("file exist : " + checkFile.exists() + " overwrite=" + overwrite);
                }
            }

            if(!isFault) {
                isUploaded = true;
            }


        } catch(Exception ex){
            ex.printStackTrace();
        }

        return isUploaded;
    }

    private List<gEdge> jsonToEdgeList(String json) {
        List<gEdge> returnMap = null;
        try{
            returnMap = gson.fromJson(json,edgeType);
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error(ex.getMessage());
        }
        return returnMap;

    }

    private void updateNodeMap() {

        String edgeMapString = plugin.getConfig().getStringParam("edges");
        List<gEdge> edgeList = jsonToEdgeList(edgeMapString);
        if (edgeList != null) {
            for (gEdge edge : edgeList) {
                logger.debug(edge.edge_id + " from: " + edge.node_from + " to: " + edge.node_to);
                MsgEvent req = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.EXEC);
                req.setParam("action", "getinodestatus");
                req.setParam("inode_id", edge.node_to);
                MsgEvent resp = plugin.sendRPC(req);
                String inodeString = resp.getCompressedParam("inodemap");
                Map<String, String> inodeMap = gson.fromJson(inodeString, mapType);
                iNodeList.add(inodeMap);
            }
        }
    }

}
