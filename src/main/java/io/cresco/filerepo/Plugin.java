package io.cresco.filerepo;


import io.cresco.library.agent.AgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Executor;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.plugin.PluginService;
import io.cresco.library.utilities.CLogger;
import org.apache.felix.hc.api.HealthCheck;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.*;

import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;

@Component(
        service = { PluginService.class },
        scope=ServiceScope.PROTOTYPE,
        configurationPolicy = ConfigurationPolicy.REQUIRE,
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
    // Felix HealthCheck registration (central health); best-effort, unregistered on stop
    private ServiceRegistration<HealthCheck> healthReg;

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
    public boolean isActive() {
        return pluginBuilder.isActive();
    }

    @Override
    public void setIsActive(boolean isActive) {
        pluginBuilder.setIsActive(isActive);
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
                //String outgoingPathString =  pluginBuilder.getConfig().getStringParam("outgoing_storage_path");
                //String incomingPathString =  pluginBuilder.getConfig().getStringParam("incoming_storage_path");
                //String fileRepoName =  pluginBuilder.getConfig().getStringParam("filerepo_name");

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

                /*
                if((scanDirString != null) && (fileRepoName != null)) {
            logger.info("Starting file scan : " + scanDirString + " filerepo: " + fileRepoName);
            startScan(delay, period);
        } else if((scanDirString == null) && (fileRepoName != null)) {
            logger.info("Start listening for filerepo: " + fileRepoName);
            createSubListener(fileRepoName);
        }

        scanDirString =  plugin.getConfig().getStringParam("scan_dir");
        fileRepoName =  plugin.getConfig().getStringParam("filerepo_name");
        repo_dir
                 */

                boolean enableScan = false;
                if((pluginBuilder.getConfig().getStringParam("scan_dir") != null) &&
                        (pluginBuilder.getConfig().getStringParam("enable_scan") != null) &&
                        (pluginBuilder.getConfig().getStringParam("filerepo_name") != null)) {

                    if(pluginBuilder.getConfig().getBooleanParam("enable_scan", false)) {
                        repoEngine.start();
                        //Log message to notify of plugin startup
                        logger.info("repoEngine Started");
                    }
                } else if((pluginBuilder.getConfig().getStringParam("repo_dir") != null) &&
                        (pluginBuilder.getConfig().getStringParam("filerepo_name") != null)) {
                            repoEngine.start();
                }

                // Register the central Felix HealthCheck (best-effort; must never break startup).
                registerHealthCheck();
            }
            return true;
        } catch(Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    /** Register filerepo's Felix HealthCheck so CrescoHealthExecutor discovers it. Best-effort. */
    private void registerHealthCheck() {
        try {
            Dictionary<String, Object> props = new Hashtable<>();
            props.put(HealthCheck.NAME, "filerepo");
            props.put(HealthCheck.TAGS, new String[]{"local"});
            healthReg = context.registerService(HealthCheck.class,
                    new FileRepoHealthCheck(pluginBuilder, repoEngine), props);
            logger.info("Registered filerepo HealthCheck (Felix HC)");
        } catch (Throwable t) {
            // health is best-effort: a missing hc.api bundle must never break filerepo
            if (logger != null) logger.warn("Could not register filerepo HealthCheck: " + t.getMessage());
        }
    }

    @Override
    public boolean isStopped() {

        // deactivate FIRST: an in-flight health check racing this teardown would otherwise see
        // isActive()==true with a closed catalog and emit a spurious CRITICAL during a normal stop
        if (pluginBuilder != null) {
            pluginBuilder.setIsActive(false);
        }

        try {
            if (healthReg != null) { healthReg.unregister(); healthReg = null; }
        } catch (Exception ignore) { }

        if(pluginBuilder != null) {

            if(repoEngine != null) {
                repoEngine.shutdown();
                repoEngine = null;
            }

            if(dbEngine != null) {
                dbEngine.shutdown();
                dbEngine = null;
            }

            if(executor instanceof ExecutorImpl) {
                ((ExecutorImpl) executor).cleanup();
            }

            pluginBuilder.setExecutor(null);
            pluginBuilder.setIsActive(false);
        }
        return true;
    }

}