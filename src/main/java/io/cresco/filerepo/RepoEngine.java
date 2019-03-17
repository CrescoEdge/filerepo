package io.cresco.filerepo;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sun.org.apache.xpath.internal.operations.Bool;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.io.File;
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

    private Timer fileScanTimer;

    private String scanRepo;
    private String scanDirString;

    private String transferId;


    public RepoEngine(PluginBuilder pluginBuilder) {

        this.plugin = pluginBuilder;
        logger = plugin.getLogger(ExecutorImpl.class.getName(), CLogger.Level.Info);
        gson = new Gson();

        this.crescoType = new TypeToken<Map<String, List<Map<String, String>>>>() {
        }.getType();

        this.repoListType = new TypeToken<Map<String,FileObject>>() {
        }.getType();


        fileMap = Collections.synchronizedMap(new HashMap<>());

    }

    public void confirmTransfer(String incomingTransferId) {
        try{
            if(incomingTransferId != null) {
                if(transferId.equals(incomingTransferId)) {
                    transferId = null;
                    inScan.set(false);
                }
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            ex.printStackTrace();
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

    private void buildRepoList() {

        try {

            Map<String,FileObject> fileObjectMap = new HashMap<>();

            File folder = new File(scanDirString);

            logger.info("scan dir: " + scanDirString);

            File[] listOfFiles = folder.listFiles();

            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile()) {
                    String fileName = listOfFiles[i].getName();
                    String filePath = listOfFiles[i].getAbsolutePath();
                    String MD5hash = plugin.getAgentService().getDataPlaneService().getMD5(filePath);
                    logger.info("fileName:" + fileName + " MD5:" + MD5hash + " filepath:" + filePath);
                    FileObject fileObject = new FileObject(fileName,MD5hash,scanRepo,filePath);
                    fileObjectMap.put(fileName, fileObject);
                }
            }

            synchronized (lockFileMap) {
                fileMap.put(scanRepo,fileObjectMap);
            }

        }catch (Exception ex) {
            logger.error(ex.getMessage());
        }

    }

    public void startScan() {
        long delay  = 5000L;
        //long period = 15000L;

        scanDirString =  plugin.getConfig().getStringParam("scan_dir");
        scanRepo =  plugin.getConfig().getStringParam("scan_repo");
        long period =  plugin.getConfig().getLongParam("scan_period", 15000L);

        if((scanDirString != null) && (scanRepo != null)) {
            logger.info("Starting file scan : " + scanDirString + " repo:" + scanRepo);
            startScan(delay, period);
        }

    }

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

                                            transferId = UUID.randomUUID().toString();

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
                                            filePutRequest.setParam("transfer_id", transferId);
                                            //overwrite remote files
                                            filePutRequest.setParam("overwrite", Boolean.TRUE.toString());

                                            for (Map.Entry<String, FileObject> entry : sendFileMap.entrySet()) {
                                                String fileName = entry.getKey();
                                                FileObject fileObject = entry.getValue();

                                                if (myFileMap.containsKey(fileName)) {

                                                    Path filePath = Paths.get(fileObject.filePath);
                                                    filePutRequest.addFile(filePath.toAbsolutePath().toString());

                                                /*
                                                MsgEvent filePutRequest = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC,region,agent,pluginID);
                                                filePutRequest.setParam("action", "putfile");

                                                filePutRequest.setParam("filename", fileName);
                                                filePutRequest.setParam("md5", fileObject.MD5);
                                                filePutRequest.setParam("repo_name",scanRepo);
                                                //overwrite remote files
                                                filePutRequest.setParam("overwrite", Boolean.TRUE.toString());


                                                Path filePath = Paths.get(fileObject.filePath);

                                                //filePutRequest.setDataParam("filedata", java.nio.file.Files.readAllBytes(filePath));
                                                filePutRequest.addFile(filePath.toAbsolutePath().toString());

                                                MsgEvent filePutResponse = plugin.sendRPC(filePutRequest);
                                                if(filePutResponse != null) {
                                                    //incoming.setParam("uploaded", Boolean.TRUE.toString());
                                                    try {
                                                        if(!Boolean.parseBoolean(filePutResponse.getParam("uploaded"))) {
                                                            logger.info("Error transfering : " + fileName + " to " +  region + " " + agent + " " + pluginID );
                                                        } else {
                                                            logger.info("Transfered : " + fileName);
                                                        }
                                                    } catch (Exception ex) {
                                                     logger.error("Could not verify transfer");
                                                     logger.error(ex.getMessage());
                                                    }
                                                }
                                                */
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

    public void startScan(long delay, long period) {

        TimerTask fileScanTask = new TimerTask() {
            public void run() {
                try {

                    if(!inScan.get()) {
                        logger.error("\t\t ***STARTING SCAN " + inScan.get() + " tid:" + transferId);
                        inScan.set(true);
                        logger.error("\t\t ***STARTED SCAN " + inScan.get());
                        //build file list
                        buildRepoList();
                        //find other repos
                        syncRegionFiles();
                        logger.error("\t\t ***ENDING SCAN " + inScan.get());
                        //inScan.set(false);
                        logger.error("\t\t ***ENDED SCAN " + inScan.get());
                        if(transferId == null) {
                            inScan.set(false);
                        }
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

            String repoDirString =  plugin.getConfig().getStringParam("filerepo_dir", "filerepo");


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

    public Boolean putFile(String fileName, String fileMD5, String repoName, byte[] fileData, boolean overwrite) {

        boolean isUploaded = false;
        try {

                String fileSavePath = getRepoDir(repoName).getAbsolutePath() + "/" + fileName;
                File checkFile = new File(fileSavePath);

                if((!checkFile.exists()) || (overwrite)) {

                    Path path = Paths.get(fileSavePath);
                    Files.write(path, fileData);
                    File fileSaved = new File(fileSavePath);
                    if (fileSaved.isFile()) {
                        String md5 = plugin.getAgentService().getDataPlaneService().getMD5(fileSavePath);
                        if (fileMD5.equals(md5)) {

                            FileObject fileObject = new FileObject(fileName, fileMD5, repoName, fileSavePath);

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

    public Boolean putFile(String fileName, String fileMD5, String repoName, String filePath, boolean overwrite) {

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
                    String md5 = plugin.getAgentService().getDataPlaneService().getMD5(fileSavePath);
                    if (fileMD5.equals(md5)) {

                        FileObject fileObject = new FileObject(fileName, fileMD5, repoName, fileSavePath);

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

    public Boolean putFiles(List<String> fileList, String repoName, boolean overwrite) {

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
                    Files.move(tmpFilePath, fileSaved.toPath());

                    if (fileSaved.isFile()) {
                        String md5 = plugin.getAgentService().getDataPlaneService().getMD5(fileSavePath);

                            FileObject fileObject = new FileObject(fileSaved.getName(), md5, repoName, fileSavePath);

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
