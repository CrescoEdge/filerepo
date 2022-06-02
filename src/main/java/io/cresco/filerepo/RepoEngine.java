package io.cresco.filerepo;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import javax.jms.Message;
import javax.jms.TextMessage;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RepoEngine {

    private PluginBuilder plugin;
    private CLogger logger;
    private Gson gson;

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

    private Type mapType;

    private Timer fileScanTimer;
    private Timer repoBroadcastTimer;

    private String scanDirString;

    private AtomicBoolean lockSubscriberMap = new AtomicBoolean();
    private Map<String,Map<String,String>> subscriberMap;

    private int transferId = -1;

    private  DBEngine dbEngine;

    private List<String> listenerList;

    private String fileRepoName;

    public RepoEngine(PluginBuilder pluginBuilder, DBEngine dbEngine) {

        this.plugin = pluginBuilder;
        logger = plugin.getLogger(RepoEngine.class.getName(), CLogger.Level.Info);
        this.dbEngine = dbEngine;
        gson = new Gson();

        this.repoListType = new TypeToken<Map<String,FileObject>>() {
        }.getType();


        this.mapType = new TypeToken<Map<String,String>>() {
        }.getType();


        subscriberMap = Collections.synchronizedMap(new HashMap<>());

        listenerList = new ArrayList<>();

        fileMap = Collections.synchronizedMap(new HashMap<>());
        peerVersionMap = Collections.synchronizedMap(new HashMap<>());
        peerUpdateStateMap = Collections.synchronizedMap(new HashMap<>());
        peerUpdateQueueMap = Collections.synchronizedMap(new HashMap<>());

        scanDirString =  plugin.getConfig().getStringParam("scan_dir");
        fileRepoName =  plugin.getConfig().getStringParam("filerepo_name");

    }

    public void start() {

        long delay =  plugin.getConfig().getLongParam("scan_delay", 5000L);
        long period =  plugin.getConfig().getLongParam("scan_period", 15000L);

        if((scanDirString != null) && (fileRepoName != null)) {
            logger.info("Starting file scan : " + scanDirString + " filerepo: " + fileRepoName);
            startScan(delay, period);
        } else if((scanDirString == null) && (fileRepoName != null)) {
            logger.info("Start listening for filerepo: " + fileRepoName);
            createSubListener(fileRepoName);
        }


    }

    public void startScan(long delay, long period) {

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

                            if (!inScan.get()) {

                                logger.info("\t\t ***STARTING SCAN repo_name: " + fileRepoName + " inScan: " + inScan.get() + " tid:" + transferId);
                                inScan.set(true);

                                //build file list
                                Map<String, FileObject> diffList = getFileRepoDiff();
                                if (diffList.size() > 0) {

                                    logger.debug("SYNC Files");
                                    syncRegionFiles(diffList);
                                }

                                inScan.set(false);

                            } else {
                                logger.error("\t\t ***ALREADY IN SCAN");
                            }
                        } else {
                            logger.error("\t\t ***FILE LOCATION " + scanDirString + " NO LONGER EXISTS");
                        }

                    } else {
                        logger.error("NO ACTIVE");
                    }

                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        };

        //create timer task
        TimerTask repoBroadcastTask = new TimerTask() {
            public void run() {
                try {

                    if(plugin.isActive()) {

                        logger.info("\t\t ***BROADCASTING repo_name: " + fileRepoName + " inScan: " + inScan.get() + " tid:" + transferId);
                        //let everyone know repo exists
                        repoBroadcast(fileRepoName,"discover", transferId);

                    } else {
                        logger.error("NOT ACTIVE");
                    }

                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        };

        fileScanTimer = new Timer("Timer");
        fileScanTimer.scheduleAtFixedRate(fileScanTask, delay, period);
        logger.info("filescantimer : set : " + period);

        repoBroadcastTimer = new Timer("BroadCastTimer");
        repoBroadcastTimer.scheduleAtFixedRate(repoBroadcastTask, delay, period);
        logger.info("broadcasttimer : set : " + period);
    }

    public void shutdown() {

        stopScan();
        //if(fileRepoName != null) {
        //    repoBroadcast(fileRepoName,"shutdown");
        //}
        for(String listenerid : listenerList) {
            plugin.getAgentService().getDataPlaneService().removeMessageListener(listenerid);
        }

    }

    public void stopScan() {
        if(fileScanTimer != null) {
            logger.warn("Stopping existing scan");
            fileScanTimer.cancel();
            fileScanTimer = null;
        } else {
            logger.warn("No scan currently active");
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
            e.printStackTrace();
        }
        return fileNames;
    }

    //build and sync
    private Map<String,FileObject> getFileRepoDiff() {

        Map<String,FileObject> fileDiffMap = null;
        try {

            fileDiffMap = new HashMap<>();

            File[] listOfFiles = null;
            boolean scanRecursive = plugin.getConfig().getBooleanParam("scan_recursive",false);
            if(scanRecursive) {
                logger.debug("SCAN RECURSIVE");
                List<File> tp = new ArrayList<>();
                List<File> fn = getFileNames(tp,Paths.get(scanDirString));
                for(File f : fn) {
                    logger.error("File: " + f.getAbsolutePath());
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
                        String fileName = listOfFiles[i].getName();
                        String filePath = listOfFiles[i].getAbsolutePath();
                        long lastModified = listOfFiles[i].lastModified();
                        long filesize = listOfFiles[i].length();

                        boolean add = false;
                        boolean update = false;

                        //see if file is in the database
                        long lastModifiedDb = dbEngine.getLastModified(filePath);
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
                            MD5hash = plugin.getMD5(filePath);
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

        }catch (Exception ex) {
            logger.error(ex.getMessage());
            ex.printStackTrace();
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
                for (Map<String, String> subscriberMap : currentSubscriberList) {

                    //This is another filerepo in my region, I need to send it data
                    String region = subscriberMap.get("sub_region_id");
                    String agent = subscriberMap.get("sub_agent_id");
                    String pluginID = subscriberMap.get("sub_plugin_id");

                    logger.debug("SEND :" + region + " " + agent + " " + pluginID + " data");

                    MsgEvent fileRepoRequest = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, region, agent, pluginID);
                    fileRepoRequest.setParam("action", "repolistin");
                    //String repoListStringIn = getFileRepoList(scanRepo);
                    String repoListStringIn = gson.toJson(fileDiffMap);
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
                                for (Map.Entry<String, FileObject> entry : fileDiffMap.entrySet()) {
                                    //String key = entry.getKey();
                                    FileObject fileObject = entry.getValue();
                                    dbEngine.updateFile(fileObject.filePath, fileObject.MD5, 1, fileObject.lastModified, fileObject.filesize);
                                }
                                logger.info("Transfered " + fileDiffMap.size() + " files to " + pluginID);
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
            ex.printStackTrace();
        }
    }

    //data transfer
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
                                        FileObject fileObject = diffEntry.getValue();

                                        //public Path downloadRemoteFile(String remoteRegion, String remoteAgent, String remoteFilePath, String localFilePath) {
                                        File localDir = getRepoDir();
                                        logger.debug("localDir: " + localDir.getAbsolutePath());
                                        Path localPath = Paths.get(localDir.getAbsolutePath() + "/" + fileObject.fileName);
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
                                            Path tmpFile = plugin.getAgentService().getDataPlaneService().downloadRemoteFile(region, agent, fileObject.filePath, localPath.toFile().getAbsolutePath());
                                            logger.debug("Synced " + tmpFile.toFile().getAbsolutePath());
                                        }

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

    public Map<String,String> getFileInfo(String filePath) {
        return dbEngine.getFileInfo(filePath);
    }

    public String getFileRepoString(String repoName) {
        String repoString = null;
        try {

            List<Map<String,String>> repoFileList = dbEngine.getRepoList();
            repoString = gson.toJson(repoFileList);
            logger.debug("repoList: " + repoString);

        } catch (Exception ex) {
            logger.error("getFileRepoString: " + ex.getMessage());
        }
        return repoString;
    }

    public Boolean clearRepo() {
        boolean isRemoved = false;
        try {

            while (inScan.get()) {
                Thread.sleep(1000);
                logger.info("Waiting for file scan to stop");
            }
            //Stop scanning so we can clear out files
            inScan.set(true);

            //List all files recorded in repo and remove them
            List<Map<String,String>> repoFileList = dbEngine.getRepoList();
            for(Map<String,String> filerecord : repoFileList) {
                File removeFile = Paths.get(filerecord.get("filepath")).toFile();
                dbEngine.deleteFile(removeFile.getAbsolutePath());
                removeFile.delete();
            }
            //delete any files or directories not recorded in repo dir
            Files.walk(Paths.get(getRepoDir().getAbsolutePath()))
                    .filter(Files::isRegularFile)
                    .map(Path::toFile)
                    .forEach(File::delete);

            //release scan
            inScan.set(false);
            isRemoved = true;

        } catch (Exception ex) {
            logger.error("removeFile: " + ex.getMessage());
            isRemoved = false;
        }
        return isRemoved;
    }

    public Boolean removeFile(String fileRepoName, String fileName) {
        boolean isRemoved = false;
        try {

            File checkFile = Paths.get(getRepoDir().getAbsolutePath() + "/" + fileName).toFile();
            if(checkFile.exists()) {
                int deleteStatus = dbEngine.deleteFile(checkFile.getAbsolutePath());
                logger.error("delete status: " + deleteStatus);
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
        try {

            boolean isFault = false;

            for(String incomingFileName : fileList) {

                Path tmpFilePath = Paths.get(incomingFileName);

                logger.info("incoming file: " + tmpFilePath);

                String fileSavePath = getRepoDir().getAbsolutePath() + "/" + tmpFilePath.getFileName();
                File checkFile = new File(fileSavePath);

                if ((checkFile.exists() && overwrite) || (!checkFile.exists())) {

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

                        if(!plugin.getConfig().getBooleanParam("enable_scan",Boolean.TRUE)) {
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
            ex.printStackTrace();
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


        javax.jms.MessageListener ml = new javax.jms.MessageListener() {
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

                    ex.printStackTrace();
                }
            }
        };

        String queryString = "filerepo_name='" + filerepoName + "' AND broadcast";
        String node_from_listner_id = plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,queryString);

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

            plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.AGENT,updateMessage);


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

            plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.AGENT,updateMessage);
            logger.debug("SENDING MESSAGE: " + update);

        } catch (Exception ex) {
            logger.error("failed to update subscribers");
            logger.error(ex.getMessage());
        }

    }

    private void createRepoSubListener(String filerepoName) {


        javax.jms.MessageListener ml = new javax.jms.MessageListener() {
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

                    ex.printStackTrace();
                }
            }
        };

        String queryString = "filerepo_name='" + filerepoName + "' AND region_id='" + plugin.getRegion() + "' AND agent_id='" + plugin.getAgent() + "' AND plugin_id='" + plugin.getPluginID() + "'";
        String node_from_listner_id = plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,queryString);

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
            ex.printStackTrace();
        }
        return repoDir;
    }

    private File getRepoDir() {
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
            ex.printStackTrace();
        }
        return repoDir;
    }


}
