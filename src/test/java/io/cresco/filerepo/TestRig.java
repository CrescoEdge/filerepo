package io.cresco.filerepo;

import io.cresco.library.agent.AgentService;
import io.cresco.library.agent.AgentState;
import io.cresco.library.agent.ControllerState;
import io.cresco.library.data.DataPlaneService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * filerepo's own parts (PluginBuilder, DBEngine, RepoEngine, ExecutorImpl) in a plain JVM, over a stand-in agent: the
 * real Derby catalog in a temporary directory, messages sent and dataplane listeners recorded, log lines kept.
 */
final class TestRig implements AutoCloseable {

    static final String REGION = "region-1", AGENT = "agent-1", PLUGIN_ID = "plugin-0";

    final Path agentDir;
    final Map<String, Object> config;
    final PluginBuilder plugin;
    final DBEngine db;
    final RepoEngine repo;
    final ExecutorImpl exec;
    /** MsgEvents filerepo sent (msgOut). */
    final List<MsgEvent> sent = Collections.synchronizedList(new ArrayList<>());
    /** Dataplane listeners filerepo registered, by selector. */
    final List<String> listeners = Collections.synchronizedList(new ArrayList<>());
    /** Dataplane messages filerepo sent. */
    final List<Object> dataplaneSent = Collections.synchronizedList(new ArrayList<>());
    /** Remote files filerepo asked the agent to download (remote path), in order; each "fails" (null). */
    final List<String> downloads = Collections.synchronizedList(new ArrayList<>());
    /** Every warn and error line logged. */
    final List<String> log = Collections.synchronizedList(new ArrayList<>());

    /** filerepo with {@code config} (pluginID added), its catalog under {@code agentDir}, and {@code live} (may be null). */
    TestRig(Path agentDir, Map<String, Object> config, LiveTierClient live) throws Exception {
        this.agentDir = agentDir;
        Files.createDirectories(agentDir);
        this.config = new HashMap<>(config);
        this.config.put("pluginID", PLUGIN_ID);
        this.plugin = new PluginBuilder(agent(), "io.cresco.filerepo.Plugin", null, this.config);
        this.db = new DBEngine(plugin);
        this.repo = new RepoEngine(plugin, db, live);
        this.exec = new ExecutorImpl(plugin, repo, live);
        plugin.setIsActive(true);
    }

    /** One EXEC request, as a caller on this agent sends it. */
    static MsgEvent request(String action, String... kv) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, REGION, AGENT, "caller", REGION, AGENT, PLUGIN_ID, false, false);
        m.setParam("action", action);
        for (int i = 0; i + 1 < kv.length; i += 2) m.setParam(kv[i], kv[i + 1]);
        return m;
    }

    /** Dispatch a request; the reply (the same MsgEvent, answered), or null when the verb answers nothing. */
    MsgEvent call(String action, String... kv) {
        return exec.executeEXEC(request(action, kv));
    }

    MsgEvent call(MsgEvent m) {
        return exec.executeEXEC(m);
    }

    @Override public void close() {
        plugin.setIsActive(false);
        try { repo.shutdown(); } catch (Exception ignore) { }
        try { exec.cleanup(); } catch (Exception ignore) { }
        try { db.shutdown(); } catch (Exception ignore) { }
        try { plugin.close(); } catch (Exception ignore) { }
    }

    /** A logger that keeps warn and error lines. */
    CLogger logger() {
        return (CLogger) Proxy.newProxyInstance(CLogger.class.getClassLoader(), new Class<?>[]{CLogger.class}, (p, m, a) -> {
            if ((m.getName().equals("error") || m.getName().equals("warn")) && a != null && a.length > 0) {
                log.add(m.getName() + ": " + a[0]);
            }
            return null;
        });
    }

    private AgentService agent() {
        ControllerState cs = (ControllerState) Proxy.newProxyInstance(ControllerState.class.getClassLoader(),
                new Class<?>[]{ControllerState.class}, (p, m, a) -> {
                    switch (m.getName()) {
                        case "getRegion": return REGION;
                        case "getAgent": return AGENT;
                        case "isActive": return true;
                        default: return defaultValue(m.getReturnType());
                    }
                });
        AgentState state = new AgentState(cs);
        CLogger log = logger();
        DataPlaneService dp = (DataPlaneService) Proxy.newProxyInstance(DataPlaneService.class.getClassLoader(),
                new Class<?>[]{DataPlaneService.class}, (p, m, a) -> {
                    switch (m.getName()) {
                        case "addMessageListener":
                            listeners.add(String.valueOf(a[2]));
                            return "listener-" + listeners.size();
                        case "sendMessage":
                            dataplaneSent.add(a[a.length > 2 && !(a[1] instanceof jakarta.jms.Message) ? 2 : 1]);
                            return true;
                        case "downloadRemoteFile":
                            downloads.add(String.valueOf(a[2]));
                            return null;
                        default:
                            return defaultValue(m.getReturnType());
                    }
                });
        return (AgentService) Proxy.newProxyInstance(AgentService.class.getClassLoader(), new Class<?>[]{AgentService.class},
                (p, m, a) -> {
                    switch (m.getName()) {
                        case "getAgentState": return state;
                        case "getCLogger": return log;
                        case "getAgentDataDirectory": return agentDir.toString();
                        case "getDataPlaneService": return dp;
                        case "msgOut":
                            sent.add((MsgEvent) a[1]);
                            return null;
                        default: return defaultValue(m.getReturnType());
                    }
                });
    }

    static Object defaultValue(Class<?> t) {
        if (t == boolean.class) return false;
        if (t == int.class) return 0;
        if (t == long.class) return 0L;
        if (t == double.class) return 0d;
        if (t == float.class) return 0f;
        if (t == short.class) return (short) 0;
        if (t == byte.class) return (byte) 0;
        if (t == char.class) return (char) 0;
        return null;
    }
}
