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
    /**
     * The sync_key an admin provisions for the test fabric's repositories (generated per run, never a fixed secret):
     * every rig gets it unless its config says otherwise ("sync_key" present, even empty: no key).
     */
    static final String SYNC_KEY = SyncReceiver.token();

    /**
     * The test fabric's producer, and its admin's provisioning: the producer key of plugin-p (made per run, never a
     * fixed key) is placed in its state directory before it starts, as restoring a backup of that directory would, and
     * every mirror is configured with the pin of that key (sync_producers) unless its config says otherwise
     * ("sync_producers" present, even empty: no pin). A mirror takes a producer only by such an admin's pin, never by
     * who answers; any other producer on the fabric (plugin-q, plugin-x, ...) makes its own key, which nothing pins.
     */
    static final String FABRIC_PRODUCER = "plugin-p";
    static final Path FABRIC_KEY_DIR;
    static final String FABRIC_FINGERPRINT;
    static final String FABRIC_PIN;
    static {
        try {
            FABRIC_KEY_DIR = Files.createTempDirectory("filerepo-fabric-key");
            FABRIC_FINGERPRINT = ProducerKey.loadOrCreate(FABRIC_KEY_DIR).key.fingerprint;
            FABRIC_PIN = REGION + "/" + AGENT + "#" + FABRIC_FINGERPRINT;
            FABRIC_KEY_DIR.toFile().deleteOnExit();
            FABRIC_KEY_DIR.resolve(ProducerKey.FILE).toFile().deleteOnExit();
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    final Path agentDir;
    final String pluginId;
    final Fabric fabric;
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
    /** The agent's plugin-signing policy: the test CA's trust store unless a test says otherwise. */
    volatile boolean signingTrustConfigured = true;
    /** The agent's security mode (AgentService.isSecurityEnabled): secured unless a test says otherwise. */
    volatile boolean securityEnabled = true;
    volatile boolean signingRequired = true;

    /** filerepo with {@code config} (pluginID added), its catalog under {@code agentDir}, and {@code live} (may be null). */
    TestRig(Path agentDir, Map<String, Object> config, LiveTierClient live) throws Exception {
        this(agentDir, config, live, PLUGIN_ID, null);
    }

    /** filerepo as plugin {@code pluginId} on this agent, joined to {@code fabric} (may be null: messages only recorded). */
    TestRig(Path agentDir, Map<String, Object> config, LiveTierClient live, String pluginId, Fabric fabric) throws Exception {
        this.agentDir = agentDir;
        this.pluginId = pluginId;
        this.fabric = fabric;
        Files.createDirectories(agentDir);
        this.config = new HashMap<>(config);
        this.config.put("pluginID", pluginId);
        if (!this.config.containsKey("sync_key")) this.config.put("sync_key", SYNC_KEY);
        boolean mirrorConfig = this.config.get("filerepo_name") != null && this.config.get("repo_dir") != null
                && this.config.get("scan_dir") == null && this.config.get("live_row") == null;
        if (mirrorConfig && !this.config.containsKey("sync_producers")) this.config.put("sync_producers", FABRIC_PIN);
        this.plugin = new PluginBuilder(agent(), "io.cresco.filerepo.Plugin", null, this.config);
        this.db = new DBEngine(plugin);
        if (FABRIC_PRODUCER.equals(pluginId) && this.config.get("scan_dir") != null && db.openError() == null
                && db.stateDir() != null && !Files.exists(db.stateDir().resolve(ProducerKey.FILE))) {
            Path key = db.stateDir().resolve(ProducerKey.FILE);
            Files.copy(FABRIC_KEY_DIR.resolve(ProducerKey.FILE), key);
            Files.setPosixFilePermissions(key, java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
        }
        this.repo = new RepoEngine(plugin, db, live);
        this.exec = new ExecutorImpl(plugin, repo, live);
        plugin.setIsActive(true);
        if (fabric != null) fabric.register(this);
    }

    /** One EXEC request to this rig's plugin, from a caller on this agent. */
    MsgEvent requestTo(String action, String... kv) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, REGION, AGENT, "caller", REGION, AGENT, pluginId, false, false);
        m.setParam("action", action);
        for (int i = 0; i + 1 < kv.length; i += 2) m.setParam(kv[i], kv[i + 1]);
        return m;
    }

    /** Stamp {@code m} as infrastructure would for an API user of the tenant holding {@code roles}. */
    static MsgEvent as(MsgEvent m, String... roles) {
        m.setParam(Callers.PARAM_CALLER, "api:tenant-a/operator");
        m.setParam("cresco_caller_tenant", "tenant-a");
        m.setParam(Callers.PARAM_ROLES, String.join(",", roles));
        return m;
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
                            if (fabric != null) return fabric.addListener(String.valueOf(a[2]), (jakarta.jms.MessageListener) a[1]);
                            return "listener-" + listeners.size();
                        case "removeMessageListener":
                            if (fabric != null) fabric.removeListener(String.valueOf(a[0]));
                            return null;
                        case "sendMessage": {
                            Object msg = a[a.length > 2 && !(a[1] instanceof jakarta.jms.Message) ? 2 : 1];
                            dataplaneSent.add(msg);
                            if (fabric != null && msg instanceof jakarta.jms.Message) return fabric.publish((jakarta.jms.Message) msg);
                            return true;
                        }
                        case "createBytesMessage":
                            return FakeJms.bytes();
                        case "createTextMessage":
                            return FakeJms.text();
                        case "createMapMessage":
                            return FakeJms.map();
                        case "shardFor":
                            return 0;
                        case "getDataPlaneShardCount":
                            return 1;
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
                        case "isSecurityEnabled": return securityEnabled;
                        case "getDataPlaneService": return dp;
                        case "getPluginSigningTrust": return signingTrustConfigured ? JarSigning.trustStore() : null;
                        case "isPluginSigningRequired": return signingRequired;
                        case "getTenant": return Fabric.TENANT;
                        case "msgOut":
                            sent.add((MsgEvent) a[1]);
                            if (fabric != null) fabric.route(this, (MsgEvent) a[1]);
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
