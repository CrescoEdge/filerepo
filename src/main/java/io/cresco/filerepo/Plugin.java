package io.cresco.filerepo;


import io.cresco.library.agent.AgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Executor;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.plugin.PluginService;
import io.cresco.library.utilities.CLogger;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.*;

import java.util.Map;

@Component(
        service = { PluginService.class },
        scope=ServiceScope.PROTOTYPE,
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        servicefactory = true,
        reference=@Reference(name="io.cresco.library.agent.AgentService", service= AgentService.class)
)

public class Plugin implements PluginService {

    public BundleContext context;
    private PluginBuilder pluginBuilder;
    private Executor executor;
    private CLogger logger;
    private Map<String,Object> map;
    private DBEngine dbEngine;

    private RepoEngine repoEngine;

    @Activate
    void activate(BundleContext context, Map<String,Object> map) {

        this.context = context;
        this.map = map;

    }

    @Modified
    void modified(BundleContext context, Map<String,Object> map) {
        System.out.println("Modified Config Map PluginID:" + (String) map.get("pluginID"));
    }

    @Deactivate
    void deactivate(BundleContext context, Map<String,Object> map) {

        isStopped();
        this.context = null;
        this.map = null;

    }

    @Override
    public boolean inMsg(MsgEvent incoming) {
        pluginBuilder.msgIn(incoming);
        return true;
    }

    @Override
    public boolean isStarted() {

        try {
            if(pluginBuilder == null) {
                pluginBuilder = new PluginBuilder(this.getClass().getName(), context, map);
                this.logger = pluginBuilder.getLogger(Plugin.class.getName(), CLogger.Level.Info);

                //Plugin is either receving or sending
                String scanDirString =  pluginBuilder.getConfig().getStringParam("scan_dir");
                String scanRepo =  pluginBuilder.getConfig().getStringParam("scan_repo");
                String repoDirString =  pluginBuilder.getConfig().getStringParam("repo_dir");

                boolean isSending = false;
                boolean isReceving = false;

                if((scanDirString != null) && (scanRepo != null) && (repoDirString != null)) {
                    logger.error("fileRepo can't be both sending and receving");
                    return false;
                } else if((scanDirString != null) && (scanRepo != null) && (repoDirString == null)) {
                    isSending = true;
                    logger.info("fileRepo configured as sender: scan_repo: " + scanRepo + " scan_dir:" + scanDirString);

                } else if((scanDirString == null) && (scanRepo != null) && (repoDirString != null)) {
                    isReceving = true;
                    logger.info("fileRepo configured as sender: scan_repo: " + scanRepo + " scan_dir:" + scanDirString);
                } else {
                    logger.error("no configuration found for either sending and receving");
                    return false;
                }

                //Log message to notify of plugin initialization
                logger.info("Starting repoEngine...");

                dbEngine = new DBEngine(pluginBuilder);
                //Starting the RepoEngine Threads
                repoEngine = new RepoEngine(pluginBuilder, dbEngine);


                //Starting custom message handler
                this.executor = new ExecutorImpl(pluginBuilder, repoEngine);
                pluginBuilder.setExecutor(executor);

                while (!pluginBuilder.getAgentService().getAgentState().isActive()) {
                    logger.info("Plugin " + pluginBuilder.getPluginID() + " waiting on Agent Init");
                    Thread.sleep(1000);
                }

                //setting plugin active on the agent
                pluginBuilder.setIsActive(true);

                if(isSending) {
                    //Starting any configured file scans
                    repoEngine.startScan();
                }

                //Log message to notify of plugin startup
                logger.info("Started repoEngine...");



            }
            return true;
        } catch(Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean isStopped() {

        if(pluginBuilder != null) {
            if(repoEngine != null) {
                repoEngine.stopScan();
            }
            pluginBuilder.setExecutor(null);
            pluginBuilder.setIsActive(false);
        }
        return true;
    }



}