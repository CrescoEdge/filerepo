package io.cresco.filerepo;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
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


    private Timer fileScanTimer;

    private String scanRepo;
    private String scanDirString;

    private int transferId = -1;


    public RepoEngine(PluginBuilder pluginBuilder) {

        this.plugin = pluginBuilder;
        logger = plugin.getLogger(ExecutorImpl.class.getName(), CLogger.Level.Info);
        gson = new Gson();

        this.crescoType = new TypeToken<Map<String, List<Map<String, String>>>>() {
        }.getType();

        this.repoListType = new TypeToken<Map<String,FileObject>>() {
        }.getType();


        fileMap = Collections.synchronizedMap(new HashMap<>());
        peerVersionMap = Collections.synchronizedMap(new HashMap<>());
        peerUpdateStateMap = Collections.synchronizedMap(new HashMap<>());
        peerUpdateQueueMap = Collections.synchronizedMap(new HashMap<>());


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

    public void getFileRepoDiff(String repoIn, String repoDiffString, String transferId, String region, String agent, String pluginId) {
        try {

            if(repoIn.equals(scanRepo)) {

                String repoId = region + "-" + agent + "-" + pluginId;
                Map<String,String> update = new HashMap<>();
                update.put(transferId,repoDiffString);

                synchronized (lockPeerUpdateQueueMap) {
                    if(!peerUpdateQueueMap.containsKey(repoId)) {
                        peerUpdateQueueMap.put(repoId,new LinkedList());
                        //we need to add transfer_id 0 here if the transfer is not zero
                        logger.info("getFileRepoDiff() creating new queuMap for " + repoIn + " repo: " + scanRepo);
                    }
                    logger.info("getFileRepoDiff() adding transfer_id: " + transferId + " to queueMap for " + repoIn + " repo: " + scanRepo);
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
                    logger.info("starting new updater thread for repoId: " + repoId + " transfer id: " + transferId + " repo: " + repoIn);
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

                                        logger.info("UPDATING " + repoId + " transferid: " + currentTransferId);

                                        for (Map.Entry<String, FileObject> diffEntry : remoteRepoFiles .entrySet()) {
                                            String fileName = diffEntry.getKey();
                                            FileObject fileObject = diffEntry.getValue();

                                            //public Path downloadRemoteFile(String remoteRegion, String remoteAgent, String remoteFilePath, String localFilePath) {
                                            File localDir = getRepoDir(repoIn);
                                            logger.info("localDir: " + localDir.getAbsolutePath());
                                            Path localPath = Paths.get(localDir.getAbsolutePath() + "/" + fileObject.fileName);
                                            logger.info("localFilePath: " + localPath.toFile().getAbsolutePath());

                                            Path tmpFile = plugin.getAgentService().getDataPlaneService().downloadRemoteFile(region,agent,fileObject.filePath, localPath.toFile().getAbsolutePath());
                                            logger.info("Synced " + tmpFile.toFile().getAbsolutePath());
                                        }

                                        logger.info("SENDING UPDATE " + repoId);

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
                                System.out.println(v);
                            }
                        }
                    }.start();
                }

            } else {
                logger.error("repo " + repoIn + " not active on this instance!");
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
            logger.info("scan dir: " + scanDirString);
            File folder = new File(scanDirString);
            File[] listOfFiles = folder.listFiles();

            synchronized (lockFileMap) {
                if(!fileMap.containsKey(scanRepo)) {
                    fileMap.put(scanRepo,new HashMap<>());
                }
            }

            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile()) {
                    String fileName = listOfFiles[i].getName();
                    long lastModified = listOfFiles[i].lastModified();

                    boolean update = false;

                    synchronized (lockFileMap) {
                        if(fileMap.get(scanRepo).containsKey(fileName)) {
                            if(!(fileMap.get(scanRepo).get(fileName).lastModified == lastModified)) {
                                update = true;
                            }
                        } else {
                            update = true;
                        }
                    }

                    if(update) {
                        String filePath = listOfFiles[i].getAbsolutePath();
                        String MD5hash = plugin.getMD5(filePath);
                        logger.info("fileName:" + fileName + " MD5:" + MD5hash + " filepath:" + filePath);
                        FileObject fileObject = new FileObject(fileName, MD5hash, scanRepo, filePath, lastModified);
                        synchronized (lockFileMap) {
                            fileMap.get(scanRepo).put(fileName, fileObject);
                        }

                        //might need to create a new object as not to cause concurrency problems
                        //FileObject fileObjectDiff = new FileObject(fileName, MD5hash, scanRepo, filePath, lastModified);
                        fileDiffMap.put(fileName,fileObject);
                    }
                }
            }

        }catch (Exception ex) {
            logger.error(ex.getMessage());
        }
        return fileDiffMap;
    }

    public void startScan() {
        long delay  = 5000L;
        //long period = 15000L;

        scanDirString =  plugin.getConfig().getStringParam("scan_dir");
        scanRepo =  plugin.getConfig().getStringParam("scan_repo");
        long period =  plugin.getConfig().getLongParam("scan_period", 15000L);

        if((scanDirString != null) && (scanRepo != null)) {
        //if(scanDirString != null) {
        logger.info("Starting file scan : " + scanDirString + " repo:" + scanRepo);
            startScan(delay, period);
        }

    }

    /*
    private void syncRegionFiles() {
        String returnString = null;
        try {

            MsgEvent request = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.EXEC);
            request.setParam("action", "listpluginsbytype");
            request.setParam("action_plugintype_id", "pluginname");
            request.setParam("action_plugintype_value", "io.cresco.filerepo");
            MsgEvent response = plugin.sendRPC(request);

            if (response != null) {

                returnString = response.getCompressedParam("pluginsbytypelist");

                Map<String, List<Map<String, String>>> myRepoMap = gson.fromJson(returnString, crescoType);

                if (myRepoMap != null) {

                    if (myRepoMap.containsKey("plugins")) {

                        for (Map<String, String> repoMap : myRepoMap.get("plugins")) {

                            if ((plugin.getRegion().equals(repoMap.get("region"))) && (plugin.getAgent().equals(repoMap.get("agent"))) && (plugin.getPluginID().equals(repoMap.get("pluginid")))) {
                                //do nothing if self
                                //logger.info("found self");
                            } else if (plugin.getRegion().equals(repoMap.get("region"))) {
                                //This is another filerepo in my region, I need to send it data
                                String region = repoMap.get("region");
                                String agent = repoMap.get("agent");
                                String pluginID = repoMap.get("pluginid");

                                logger.error("SEND :" + region + " " + agent + " " + pluginID + " data");

                                MsgEvent fileRepoRequest = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC,region,agent,pluginID);
                                fileRepoRequest.setParam("action","repolistin");
                                String repoListStringIn = getFileRepoList(scanRepo);

                                logger.info("repoListStringIn: " + repoListStringIn);

                                fileRepoRequest.setCompressedParam("repolistin",repoListStringIn);
                                fileRepoRequest.setParam("repo",scanRepo);

                                MsgEvent fileRepoResponse = plugin.sendRPC(fileRepoRequest);

                                if(fileRepoResponse != null) {
                                    String repoDiffString = fileRepoResponse.getCompressedParam("repodiff");
                                    if(repoDiffString != null) {
                                        logger.info("repoDiffString: " + repoDiffString);

                                        Map<String,FileObject> sendFileMap = gson.fromJson(repoDiffString, repoListType);

                                        if(sendFileMap.size() > 0) {

                                            //transferId = UUID.randomUUID().toString();

                                            Map<String, FileObject> myFileMap = new HashMap<>();

                                            synchronized (lockFileMap) {
                                                if (fileMap.containsKey(scanRepo)) {
                                                    myFileMap.putAll(fileMap.get(scanRepo));
                                                }
                                            }

                                            MsgEvent filePutRequest = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, region, agent, pluginID);
                                            filePutRequest.setParam("action", "putfiles");

                                            //filePutRequest.setParam("filename", fileName);
                                            //filePutRequest.setParam("md5", fileObject.MD5);
                                            filePutRequest.setParam("repo_name", scanRepo);
                                            filePutRequest.setParam("transfer_id", String.valueOf(transferId));
                                            //overwrite remote files
                                            filePutRequest.setParam("overwrite", Boolean.TRUE.toString());

                                            for (Map.Entry<String, FileObject> entry : sendFileMap.entrySet()) {
                                                String fileName = entry.getKey();
                                                FileObject fileObject = entry.getValue();

                                                if (myFileMap.containsKey(fileName)) {

                                                    Path filePath = Paths.get(fileObject.filePath);
                                                    filePutRequest.addFile(filePath.toAbsolutePath().toString());


                                                } else {
                                                    logger.error("Filename: " + fileName + " on transfer list, but not found locally!");
                                                }
                                            }

                                            //ready to send
                                            plugin.msgOut(filePutRequest);

                                        }
                                    }

                                    //


                                }
                            }
                        }
                    }
                } else {
                    logger.error("syncRegionFiles() No filerepo found by global controller");
                }
            } else {
                logger.error("syncRegionFiles() Null response from global controller");
            }

        }catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    */

    private void syncRegionFiles(Map<String,FileObject> fileDiffMap) {
        String returnString = null;
        try {

            MsgEvent request = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.EXEC);
            request.setParam("action", "listpluginsbytype");
            request.setParam("action_plugintype_id", "pluginname");
            request.setParam("action_plugintype_value", "io.cresco.filerepo");
            MsgEvent response = plugin.sendRPC(request);

            if (response != null) {

                returnString = response.getCompressedParam("pluginsbytypelist");

                Map<String, List<Map<String, String>>> myRepoMap = gson.fromJson(returnString, crescoType);

                if (myRepoMap != null) {

                    if (myRepoMap.containsKey("plugins")) {

                        for (Map<String, String> repoMap : myRepoMap.get("plugins")) {

                            if ((plugin.getRegion().equals(repoMap.get("region"))) && (plugin.getAgent().equals(repoMap.get("agent"))) && (plugin.getPluginID().equals(repoMap.get("pluginid")))) {
                                //do nothing if self
                                //logger.info("found self");
                            } else if (plugin.getRegion().equals(repoMap.get("region"))) {
                                //This is another filerepo in my region, I need to send it data
                                String region = repoMap.get("region");
                                String agent = repoMap.get("agent");
                                String pluginID = repoMap.get("pluginid");

                                logger.error("SEND :" + region + " " + agent + " " + pluginID + " data");

                                MsgEvent fileRepoRequest = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC,region,agent,pluginID);
                                fileRepoRequest.setParam("action","repolistin");
                                //String repoListStringIn = getFileRepoList(scanRepo);
                                String repoListStringIn = gson.toJson(fileDiffMap);
                                fileRepoRequest.setCompressedParam("repolistin",repoListStringIn);
                                fileRepoRequest.setParam("repo",scanRepo);
                                fileRepoRequest.setParam("transfer_id", String.valueOf(transferId));

                                logger.info("repoListStringIn: " + repoListStringIn);

                                MsgEvent fileRepoResponse = plugin.sendRPC(fileRepoRequest);

                                if(fileRepoResponse != null) {

                                    logger.info("Host Region: " + region + " Agent: " + agent + " pluginId:" + pluginID + " responded");

                                    if(fileRepoResponse.paramsContains("status_code") && fileRepoResponse.paramsContains("status_desc")) {
                                        int status_code = Integer.parseInt(fileRepoResponse.getParam("status_code"));
                                        String status_desc = fileRepoResponse.getParam("status_code");
                                        if(status_code != 10) {
                                            logger.error("Region: " + region + " Agent: " + agent + " pluginId:" + pluginID + " filerepo update failed status_code: " + status_code + " status_desc:" + status_desc);
                                        }
                                    }

                                    /*
                                    String repoDiffString = fileRepoResponse.getCompressedParam("repodiff");
                                    if(repoDiffString != null) {
                                        logger.info("repoDiffString: " + repoDiffString);

                                        Map<String,FileObject> sendFileMap = gson.fromJson(repoDiffString, repoListType);

                                        if(sendFileMap.size() > 0) {

                                            //transferId = UUID.randomUUID().toString();

                                            Map<String, FileObject> myFileMap = new HashMap<>();

                                            synchronized (lockFileMap) {
                                                if (fileMap.containsKey(scanRepo)) {
                                                    myFileMap.putAll(fileMap.get(scanRepo));
                                                }
                                            }

                                            MsgEvent filePutRequest = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, region, agent, pluginID);
                                            filePutRequest.setParam("action", "putfiles");

                                            //filePutRequest.setParam("filename", fileName);
                                            //filePutRequest.setParam("md5", fileObject.MD5);
                                            filePutRequest.setParam("repo_name", scanRepo);
                                            filePutRequest.setParam("transfer_id", String.valueOf(transferId));
                                            //overwrite remote files
                                            filePutRequest.setParam("overwrite", Boolean.TRUE.toString());

                                            for (Map.Entry<String, FileObject> entry : sendFileMap.entrySet()) {
                                                String fileName = entry.getKey();
                                                FileObject fileObject = entry.getValue();

                                                if (myFileMap.containsKey(fileName)) {

                                                    Path filePath = Paths.get(fileObject.filePath);
                                                    filePutRequest.addFile(filePath.toAbsolutePath().toString());


                                                } else {
                                                    logger.error("Filename: " + fileName + " on transfer list, but not found locally!");
                                                }
                                            }

                                            //ready to send
                                            plugin.msgOut(filePutRequest);

                                        }
                                    }
                                    */

                                } else {
                                    logger.error("Host Region: " + region + " Agent: " + agent + " pluginId:" + pluginID + " failed to respond!");
                                }
                            }
                        }
                    }
                } else {
                    logger.error("syncRegionFiles() No filerepo found by global controller");
                }
            } else {
                logger.error("syncRegionFiles() Null response from global controller");
            }

        }catch (Exception ex) {
            ex.printStackTrace();
        }
    }


    public void startScan(long delay, long period) {

        TimerTask fileScanTask = new TimerTask() {
            public void run() {
                try {

                    if(!inScan.get()) {

                        logger.error("\t\t ***STARTING SCAN " + inScan.get() + " tid:" + transferId);
                        inScan.set(true);

                        logger.error("\t\t ***STARTED SCAN " + inScan.get());
                        //build file list
                        Map<String,FileObject> diffList = buildRepoList();
                        if(diffList.size() > 0) {
                            //start sync
                            transferId++;
                            //find other repos
                            logger.error("SYNC Files");
                            syncRegionFiles(diffList);
                        }

                        logger.error("\t\t ***ENDING SCAN " + inScan.get());
                        //inScan.set(false);
                        logger.error("\t\t ***ENDED SCAN " + inScan.get());

                        inScan.set(false);

                    } else {
                        logger.error("\t\t ***ALREADY IN SCAN");
                    }

                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        };

        fileScanTimer = new Timer("Timer");
        fileScanTimer.scheduleAtFixedRate(fileScanTask, delay, period);
        logger.info("filescantimer : set : " + period);
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

    private File getRepoDir(String repoDirString) {
        File repoDir = null;
        try {

            String rootRepo = getRootRepoDir().getAbsolutePath();

            repoDirString = rootRepo + "/" + repoDirString;


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

    private String getFileRepoList(String repo) {
        String returnString = null;
        try {

            synchronized (fileMap) {
                if(fileMap.containsKey(repo)) {
                    returnString = gson.toJson(fileMap.get(repo));
                }
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return returnString;
    }

    public Boolean putFile(String fileName, String fileMD5, String repoName, long lastModified, byte[] fileData, boolean overwrite) {

        boolean isUploaded = false;
        try {

                String fileSavePath = getRepoDir(repoName).getAbsolutePath() + "/" + fileName;
                File checkFile = new File(fileSavePath);

                if((!checkFile.exists()) || (overwrite)) {

                    Path path = Paths.get(fileSavePath);
                    Files.write(path, fileData);
                    File fileSaved = new File(fileSavePath);
                    if (fileSaved.isFile()) {
                        String md5 = plugin.getMD5(fileSavePath);
                        if (fileMD5.equals(md5)) {

                            FileObject fileObject = new FileObject(fileName, fileMD5, repoName, fileSavePath, lastModified);

                            synchronized (lockFileMap) {
                                if(!fileMap.containsKey(repoName)) {
                                    Map<String,FileObject> repoFileMap = new HashMap<>();
                                    repoFileMap.put(fileName,fileObject);
                                    fileMap.put(repoName, repoFileMap);
                                } else {
                                    fileMap.get(repoName).put(fileName,fileObject);
                                }
                            }

                            isUploaded = true;
                        }
                    }
                } else {
                    logger.info("file exist : " + checkFile.exists() + " overwrite=" + overwrite);
                }


        } catch(Exception ex){
            ex.printStackTrace();
        }

        return isUploaded;
    }

    public Boolean putFile(String fileName, String fileMD5, String repoName, String filePath, long lastModified, boolean overwrite) {

        boolean isUploaded = false;
        try {

            String fileSavePath = getRepoDir(repoName).getAbsolutePath() + "/" + fileName;
            File checkFile = new File(fileSavePath);

            if((!checkFile.exists()) || (overwrite)) {

                File fileSaved = new File(fileSavePath);

                //move file from temp to requested location
                Path tmpFilePath = Paths.get(filePath);
                Files.move(tmpFilePath,fileSaved.toPath());

                if (fileSaved.isFile()) {
                    String md5 = plugin.getMD5(fileSavePath);
                    if (fileMD5.equals(md5)) {

                        FileObject fileObject = new FileObject(fileName, fileMD5, repoName, fileSavePath, lastModified);

                        synchronized (lockFileMap) {
                            if(!fileMap.containsKey(repoName)) {
                                Map<String,FileObject> repoFileMap = new HashMap<>();
                                repoFileMap.put(fileName,fileObject);
                                fileMap.put(repoName, repoFileMap);
                            } else {
                                fileMap.get(repoName).put(fileName,fileObject);
                            }
                        }

                        isUploaded = true;
                    }
                }
            } else {
                logger.info("file exist : " + checkFile.exists() + " overwrite=" + overwrite);
            }


        } catch(Exception ex){
            ex.printStackTrace();
        }

        return isUploaded;
    }

    public Boolean putFiles(List<String> fileList, String repoName, boolean overwrite, boolean isLocal) {

        boolean isUploaded = false;
        try {

            boolean isFault = false;

            for(String incomingFileName : fileList) {

                Path tmpFilePath = Paths.get(incomingFileName);

                String fileSavePath = getRepoDir(repoName).getAbsolutePath() + "/" + tmpFilePath.getFileName();
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
                            FileObject fileObject = new FileObject(fileSaved.getName(), md5, repoName, fileSavePath, System.currentTimeMillis());

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


}
