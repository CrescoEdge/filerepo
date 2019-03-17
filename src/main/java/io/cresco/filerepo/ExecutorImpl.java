package io.cresco.filerepo;

import com.google.gson.Gson;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Executor;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.io.File;
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


    public ExecutorImpl(PluginBuilder pluginBuilder, RepoEngine repoEngine) {
        this.plugin = pluginBuilder;
        logger = plugin.getLogger(ExecutorImpl.class.getName(), CLogger.Level.Info);
        gson = new Gson();

        this.repoEngine = repoEngine;

    }

    @Override
    public MsgEvent executeCONFIG(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeDISCOVER(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeERROR(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeINFO(MsgEvent incoming) { return null; }
    @Override
    public MsgEvent executeEXEC(MsgEvent incoming) {


        logger.debug("Processing Exec message : " + incoming.getParams());

        if(incoming.getParams().containsKey("action")) {
            switch (incoming.getParam("action")) {

                case "repolist":
                    return repoList(incoming);
                case "getjar":
                    return getPluginJar(incoming);
                case "putjar":
                    return putPluginJar(incoming);
                case "putfiles":
                    return putFile(incoming);
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
    public MsgEvent executeWATCHDOG(MsgEvent incoming) { return null;}
    @Override
    public MsgEvent executeKPI(MsgEvent incoming) { return null; }

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

            String repoDirString =  plugin.getConfig().getStringParam("repo_dir", "repo");

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
                    String md5 = plugin.getJarMD5(jarFileSavePath);
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

    private void confirmTransfer(MsgEvent incoming) {
        repoEngine.confirmTransfer(incoming.getParam("transfer_id"));
    }

    private MsgEvent putFile(MsgEvent incoming) {

        try {

            //String fileName = incoming.getParam("filename");
            //String fileMD5 = incoming.getParam("md5");
            String repoName = incoming.getParam("repo_name");
            //byte[] fileData = incoming.getDataParam("filedata");
            //String filePath = incoming.getFileList().get(0);
            List<String> fileList = incoming.getFileList();

            boolean overwrite = false;

            if(fileList != null) {

                try{
                    if(incoming.getParam("overwrite") != null) {
                        overwrite = Boolean.parseBoolean(incoming.getParam("overwrite"));
                    }
                } catch(Exception ex){
                    ex.printStackTrace();
                }

                if(repoEngine.putFiles(fileList,repoName, overwrite)) {
                    MsgEvent filesConfirm = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC,incoming.getSrcRegion(),incoming.getSrcAgent(),incoming.getSrcPlugin());
                    filesConfirm.setParam("action", "repoconfirm");
                    filesConfirm.setParam("transfer_id", incoming.getParam("transfer_id"));
                    plugin.msgOut(filesConfirm);
                    logger.info("SEND CONFIRMATION MESSAGE!");
                } else {
                    logger.error("PUTFILES FAILED!!");
                }

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
            String repoIn = incoming.getParam("repo");

            if((repoListStringIn != null) && (repoIn != null)) {

                String repoDiffString = repoEngine.getFileRepoDiff(repoIn,repoListStringIn);
                incoming.setCompressedParam("repodiff",repoDiffString);

            }

        } catch(Exception ex){
            ex.printStackTrace();
        }

        return incoming;
    }


}