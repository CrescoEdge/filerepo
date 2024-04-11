package io.cresco.filerepo;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Executor;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExecutorImpl implements Executor {

    private PluginBuilder plugin;
    private CLogger logger;
    private Gson gson;
    private RepoEngine repoEngine;
    private Type listType;

    private final AtomicBoolean transferLock = new AtomicBoolean();

    private Map<String,StreamObject> transferStreams;

    public ExecutorImpl(PluginBuilder pluginBuilder, RepoEngine repoEngine) {
        this.plugin = pluginBuilder;
        logger = plugin.getLogger(ExecutorImpl.class.getName(), CLogger.Level.Info);
        gson = new Gson();
        this.repoEngine = repoEngine;
        listType = new TypeToken<ArrayList<String>>(){}.getType();
        transferStreams = Collections.synchronizedMap(new HashMap<>());
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

        logger.debug("Processing Exec message : " + incoming.getParams());

        if(incoming.getParams().containsKey("action")) {
            switch (incoming.getParam("action")) {

                case "repolist":
                    return repoList(incoming);
                case "getrepofilelist":
                    return getRepoFileList(incoming);
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
               msg.setCompressedParam("repofilelist", repoEngine.getFileRepoString(repo_name));
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
            ex.printStackTrace();
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
            ex.printStackTrace();
        }
        return repoDir;
    }

    private MsgEvent putPluginJar(MsgEvent incoming) {

        try {

            String pluginName = incoming.getParam("pluginname");
            String pluginMD5 = incoming.getParam("md5");
            String pluginJarFile = incoming.getParam("jarfile");
            String pluginVersion = incoming.getParam("version");

            if((pluginName != null) && (pluginMD5 != null) && (pluginJarFile != null) && (pluginVersion != null)) {

                String jarFileSavePath = getRepoDir().getAbsolutePath() + "/" + pluginJarFile;
                Path path = Paths.get(jarFileSavePath);
                Files.write(path, incoming.getDataParam("jardata"));
                File jarFileSaved = new File(jarFileSavePath);
                if (jarFileSaved.isFile()) {
                    String md5 = plugin.getMD5(jarFileSavePath);
                    if (pluginMD5.equals(md5)) {
                        incoming.setParam("uploaded", pluginName);

                    }
                }
            }

        } catch(Exception ex){
            ex.printStackTrace();
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

                    List<Map<String, String>> pluginInventory = plugin.getPluginInventory(getRepoDir().getAbsolutePath());
                    for (Map<String, String> repoMap : pluginInventory) {

                        if (repoMap.containsKey("pluginname") && repoMap.containsKey("md5") && repoMap.containsKey("jarfile")) {
                            String pluginName = repoMap.get("pluginname");
                            String pluginMD5 = repoMap.get("md5");
                            String pluginJarFile = repoMap.get("jarfile");

                            if (pluginName.equals(requestPluginName) && pluginMD5.equals(requestPluginMD5)) {

                                Path jarPath = Paths.get(repoDir + "/" + pluginJarFile);
                                incoming.setDataParam("jardata", Files.readAllBytes(jarPath));

                            }
                        }

                    }

                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return incoming;
    }

    private MsgEvent getFile(MsgEvent incoming) {

        try {

            if(incoming.getParam("file_path") != null){
                String filePath = incoming.getParam("file_path");
                //logger.error("file_name: " + filename);

                ///opt/cresco/filerepo/extract_dump_7e90b402-8076-11ec-bc89-0242ac11001a.json
                //Path filePath = Paths.get(repoEngine.getRepoDir().getAbsolutePath(), filename);
                //incoming.setParam("file_path",filePath.toFile().getAbsolutePath());
                //logger.error("file_path: " + filePath.toFile().getAbsolutePath());
                Map<String,String> fileInfo = repoEngine.getFileInfo(filePath);

                if(fileInfo != null) {
                    incoming.setCompressedParam("file_metadata",gson.toJson(fileInfo));
                    incoming.setDataParam("file_data", Files.readAllBytes(Paths.get(filePath)));
                    incoming.setParam("status","10");
                    incoming.setParam("status_desc","found list");
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
            ex.printStackTrace();
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
            ex.printStackTrace();
        }
        return incoming;
    }

    private void streamFile(Map<String,String> transferInfo) {
        String transferId = transferInfo.get("transfer_id");
        int BUFFER_SIZE = Integer.parseInt(transferInfo.get("buffer_size"));
        logger.info("transferid: " + transferId + " BUFFER_SIZE: " + BUFFER_SIZE + " TRANSFERINFO: " + transferInfo);

        try {

            new Thread() {
                public void run() {
                    try {

                        boolean alerted = false;
                        //int BUFFER_SIZE = 1024 * 1024;
                        //int BUFFER_SIZE = 1024 * 8; //this is too low
                        long startByte = Long.parseLong(transferInfo.get("start_byte"));
                        long byteLength = Long.parseLong(transferInfo.get("byte_length"));
                        String filePath = transferInfo.get("file_path");
                        StreamObject streamObject = new StreamObject(transferId, filePath, startByte, byteLength);
                        synchronized (transferLock) {
                            transferStreams.put(transferId, streamObject);
                        }

                        RandomAccessFile raf = new RandomAccessFile(filePath, "r");
                        raf.seek(startByte);
                        //InputStream rafIs = Channels.newInputStream(raf.getChannel());
                        //InputStream is = ByteStreams.limit(rafIs, byteLength);

                        int seqNum = 0;
                        byte[] buffer = new byte[BUFFER_SIZE];
                        int read;
                        boolean isActive = true;
                        while((byteLength > 0) && (isActive)){
                            BytesMessage updateMessage = plugin.getAgentService().getDataPlaneService().createBytesMessage();
                            read = BUFFER_SIZE;
                            if(byteLength < BUFFER_SIZE) {
                                read = (int)byteLength;
                            }
                            raf.read(buffer);
                            updateMessage.writeBytes(buffer, 0, read);
                            updateMessage.setStringProperty(transferInfo.get("ident_key"), transferInfo.get("ident_id"));
                            updateMessage.setStringProperty("transfer_id", transferId);
                            updateMessage.setStringProperty("seq_num", String.valueOf(seqNum));
                            //logger.error("ADDING SEQ: " + seqNum + " transfer_id: " + transferId);
                            plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.AGENT,updateMessage, DeliveryMode.NON_PERSISTENT, 0, 0);
                            //logger.error("WRITING " + read + " BYTES FOR " + transferId);
                            byteLength = byteLength - read;
                            seqNum += 1;

                            synchronized (transferLock) {
                                if(transferStreams.containsKey(transferId)) {
                                    transferStreams.get(transferId).setBytesTransfered(transferStreams.get(transferId).getBytesTransfered() + read);
                                    isActive = transferStreams.get(transferId).isActive();
                                    //logger.error("streamFile transferId: " + transferId + " bytesTransfered: " + transferStreams.get(transferId).getBytesTransfered());


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
                        //is.close();
                        //rafIs.close();
                        raf.close();


                    } catch(Exception ex) {
                        logger.error("streamFile error: " + ex.getMessage());
                        synchronized (transferLock) {
                            if(transferStreams.containsKey(transferId)) {
                                transferStreams.get(transferId).setActive(false);
                            }
                        }
                    } finally {
                        //synchronized (transferLock) {
                        //    transferStreams.remove(transferId);
                        //}
                    }
                }
            }.start();


        } catch (Exception ex) {
            logger.error("Error streamFile(Map<String,String> transferInfo) " + ex.getMessage());
            ex.printStackTrace();
        }

    }
    private MsgEvent streamFile(MsgEvent incoming) {

        try {

            if(incoming.getParam("file_path") != null) {
                String filePath = incoming.getParam("file_path");

                File file = new File(filePath);
                if(file.exists()) {
                    long startByte = Long.parseLong(incoming.getParam("start_byte"));
                    long byteLength = Long.parseLong(incoming.getParam("byte_length"));
                    long endByte = startByte + byteLength;

                    if(endByte <= file.length()) {

                        Map<String, String> fileInfo = repoEngine.getFileInfo(filePath);
                        if (fileInfo != null) {
                            fileInfo.put("transfer_id", incoming.getParam("transfer_id"));
                            fileInfo.put("file_path", incoming.getParam("file_path"));
                            fileInfo.put("start_byte", incoming.getParam("start_byte"));
                            fileInfo.put("byte_length", incoming.getParam("byte_length"));
                            fileInfo.put("ident_key", incoming.getParam("ident_key"));
                            fileInfo.put("ident_id", incoming.getParam("ident_id"));

                            String bufferSizeStr = incoming.getParam("buffer_size");
                            if (bufferSizeStr == null) {
                                bufferSizeStr = String.valueOf(1024 * 32);

                            }
                            fileInfo.put("buffer_size", bufferSizeStr);

                            //transfer in new thread, send recept
                            streamFile(fileInfo);
                            //logger.error("transferid: " + incoming.getParam("transfer_id") + " START");

                            incoming.setParam("status", "10");
                            incoming.setParam("status_desc", "endByte > file size");
                        } else {
                            incoming.setParam("status", "9");
                            incoming.setParam("status_desc", "fileInfo == null");
                        }
                    } else {
                        incoming.setParam("status", "8");
                        incoming.setParam("status_desc", "fileInfo == null");
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
            ex.printStackTrace();
        }
        return incoming;
    }

    private MsgEvent streamFileCancel(MsgEvent incoming) {
        logger.error("streamFileCancel STREAM FILE CANCEL");
        try {
            incoming.setParam("status_code","9");
            logger.error("transferid: " + incoming.getParam("transfer_id") + " END");

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
            ex.printStackTrace();
        }
        return incoming;
    }


    private void confirmTransfer(MsgEvent incoming) {
        repoEngine.confirmTransfer(incoming.getParam("transfer_id"), incoming.getSrcRegion(), incoming.getSrcAgent(), incoming.getSrcPlugin());
    }

    private MsgEvent putFileRemote(MsgEvent incoming) {

        try {

            if(incoming.paramsContains("file_list") && incoming.paramsContains("dst_region")
                    && incoming.paramsContains("dst_agent") && incoming.paramsContains("dst_plugin")
                    && incoming.paramsContains("repo_name") )  {

                String fileListString = incoming.getCompressedParam("file_list");
                List<String> fileList = gson.fromJson(fileListString, listType);
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
            ex.printStackTrace();
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

                boolean isCleared = repoEngine.clearRepo();
                if(isCleared) {
                    incoming.setParam("status","10");
                    incoming.setParam("status_desc","repo cleared");
                } else {
                    incoming.setParam("status","9");
                    incoming.setParam("status_desc","repo cleared");
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


                try{
                    if(incoming.getParam("overwrite") != null) {
                        overwrite = Boolean.parseBoolean(incoming.getParam("overwrite"));
                    }
                } catch(Exception ex){
                    ex.printStackTrace();
                }

                if((incoming.getSrcAgent().equals(incoming.getDstAgent())) && (incoming.getSrcRegion().equals(incoming.getDstRegion()))) {
                    isLocal = true;
                }

                if(repoEngine.putFiles(fileList,repoName, overwrite, isLocal)) {
                    MsgEvent filesConfirm = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC,incoming.getSrcRegion(),incoming.getSrcAgent(),incoming.getSrcPlugin());
                    filesConfirm.setParam("action", "repoconfirm");
                    filesConfirm.setParam("transfer_id", incoming.getParam("transfer_id"));
                    plugin.msgOut(filesConfirm);
                    logger.info("SEND CONFIRMATION MESSAGE!");
                } else {
                    logger.error("PUTFILES FAILED!!");
                }

            } else {
                logger.error("No repo name found");
            }

        } catch(Exception ex){
            ex.printStackTrace();
        }

        if(incoming.getParams().containsKey("filedata")) {
            incoming.removeParam("filedata");
        }

        return null;
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
            ex.printStackTrace();
        }
        if(incoming.paramsContains("repolistin")) {
            incoming.removeParam("repolistin");
        }
        return incoming;
    }

}