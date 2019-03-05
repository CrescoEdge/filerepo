package io.cresco.filerepo;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
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

    private AtomicBoolean lockFileMap = new AtomicBoolean();
    private Map<String, Map<String,FileObject>> fileMap;

    private TimerTask fileScanTask;

    private String scanRepo;

    public RepoEngine(PluginBuilder pluginBuilder) {

        this.plugin = pluginBuilder;
        logger = plugin.getLogger(ExecutorImpl.class.getName(), CLogger.Level.Info);
        gson = new Gson();

        this.crescoType = new TypeToken<Map<String, List<Map<String, String>>>>() {
        }.getType();

        fileMap = Collections.synchronizedMap(new HashMap<>());

    }

    public void startScan() {
        long delay  = 5000L;
        //long period = 15000L;

        String scanDirString =  plugin.getConfig().getStringParam("scan_dir");
        scanRepo =  plugin.getConfig().getStringParam("scan_repo");
        long period =  plugin.getConfig().getLongParam("scan_period", 15000L);

        startScan(delay,period);
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
            }

            Map<String, List<Map<String, String>>> myRepoMap = gson.fromJson(returnString, crescoType);

            if (myRepoMap != null) {

                if (myRepoMap.containsKey("plugins")) {

                    for (Map<String, String> repoMap : myRepoMap.get("plugins")) {

                        if ((plugin.getRegion().equals(repoMap.get("region"))) && (plugin.getAgent().equals(repoMap.get("agent")))) {
                            //do nothing if self

                        } else if (plugin.getRegion().equals(repoMap.get("region"))) {
                            //This is another filerepo in my region, I need to send it data
                            String region = repoMap.get("region");
                            String agent = repoMap.get("agent");
                            String pluginID = repoMap.get("pluginid");

                            logger.error("SEND :" + region + " " + agent + " " + pluginID + " data");

                        }
                    }
                }
            }



        }catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void startScan(long delay, long period) {

        fileScanTask = new TimerTask() {
            public void run() {
                try {
                    //find other repos

                    syncRegionFiles();

                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        };

        Timer timer = new Timer("Timer");
        timer.scheduleAtFixedRate(fileScanTask, delay, period);
    }

    public void stopScan() {
        if(fileScanTask != null) {
            fileScanTask.cancel();
        }
    }


    private File getRepoDir(String repoDirString) {
        File repoDir = null;
        try {

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

    private String getFileRepoList() {
        String returnString = null;
        try {

            synchronized (fileMap) {
                returnString = gson.toJson(fileMap);
            }

            System.out.println(returnString);

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
                        String md5 = plugin.getJarMD5(fileSavePath);
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
                }


        } catch(Exception ex){
            ex.printStackTrace();
        }

        return isUploaded;
    }


}
