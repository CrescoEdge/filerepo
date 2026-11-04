package io.cresco.filerepo;

import io.cresco.library.messaging.MsgEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The round-4 design beyond the review's own tests: producers known by their node and their own signing key, signed
 * offers and deletes.
 */
class SyncRound4Test {

    @TempDir Path tmp;
    final Fabric fabric = new Fabric();
    final List<TestRig> rigs = new ArrayList<>();
    Path scan, mirror;
    final com.google.gson.Gson gson = new com.google.gson.Gson();

    @BeforeEach
    void up() throws Exception {
        tmp = tmp.toRealPath();
        scan = Files.createDirectories(tmp.resolve("scan"));
        mirror = Files.createDirectories(tmp.resolve("mirror"));
    }

    @AfterEach
    void down() {
        Collections.reverse(rigs);
        for (TestRig r : rigs) r.close();
    }

    TestRig rig(String pluginId, Map<String, Object> cfg) throws Exception {
        TestRig r = new TestRig(tmp.resolve("agent"), cfg, null, pluginId, fabric);
        rigs.add(r);
        return r;
    }

    TestRig producer(String pluginId, Path dir, String... kv) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("filerepo_name", "fr");
        cfg.put("scan_dir", dir.toString());
        cfg.put("scan_delay", "3600000");
        cfg.put("scan_period", "3600000");
        for (int i = 0; i + 1 < kv.length; i += 2) cfg.put(kv[i], kv[i + 1]);
        TestRig r = rig(pluginId, cfg);
        r.repo.start();
        return r;
    }

    TestRig subscriber(String pluginId, Path dir, String... kv) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("filerepo_name", "fr");
        cfg.put("repo_dir", dir.toString());
        cfg.put("sync_stream_buffer", "4096");
        for (int i = 0; i + 1 < kv.length; i += 2) cfg.put(kv[i], kv[i + 1]);
        TestRig r = rig(pluginId, cfg);
        r.repo.start();
        return r;
    }

    static void subscribe(TestRig p, TestRig s) throws Exception {
        p.repo.repoBroadcast("fr", "discover", 0);
        await(() -> p.repo.isSubscriber(TestRig.REGION, TestRig.AGENT, s.pluginId)
                && s.repo.isKnownProducer(TestRig.REGION, TestRig.AGENT, p.pluginId), s.pluginId + " subscribed to " + p.pluginId);
    }

    static void await(BooleanSupplier cond, String what) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) fail("timed out waiting: " + what);
            Thread.sleep(20);
        }
    }

    static boolean within(long ms, BooleanSupplier cond) throws Exception {
        long deadline = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return true;
            Thread.sleep(25);
        }
        return cond.getAsBoolean();
    }

    static String subKey(TestRig s) {
        return TestRig.REGION + "_" + TestRig.AGENT + "_" + s.pluginId;
    }

    static void cycle(TestRig p, TestRig... subs) throws Exception {
        assertTrue(p.repo.scanOnce());
        for (TestRig s : subs) await(() -> p.db.pendingCount(subKey(s)) == 0, s.pluginId + " settled");
    }

    static Path write(Path f, String s) throws Exception {
        Files.createDirectories(f.getParent());
        return Files.write(f, s.getBytes());
    }

    static String read(Path f) {
        try {
            return Files.readString(f);
        } catch (Exception e) {
            return null;
        }
    }

    /** A repolistin from {@code from} to {@code s}, stamped as infrastructure stamps {@code from}. */
    MsgEvent offer(TestRig s, String from, String tid, String files, String deletes, String sig) {
        MsgEvent in = s.requestTo("repolistin", "transfer_id", tid, "sync_gen", s.repo.getSyncGen(), "sync_proto",
                RepoEngine.SYNC_PROTO);
        in.setCompressedParam("repolistin", files);
        if (deletes != null) in.setCompressedParam("repodeletes", deletes);
        if (sig != null) in.setParam("sync_sig", sig);
        Fabric.stamp(in, from);
        return s.call(in);
    }

    // =================================================================== the producer's key

    @Test
    void theProducerKeyIsMadeOnceOwnerOnlyAndKeptWhenTheProducerIsRedeployed() throws Exception {
        TestRig p = producer("plugin-p", scan);
        String fp = p.repo.signingFingerprint();
        assertNotNull(fp);
        assertTrue(fp.matches("[0-9a-f]{64}"), fp);
        Path key = p.db.stateDir().resolve(ProducerKey.FILE);
        assertTrue(Files.isRegularFile(key));
        assertEquals("rw-------", PosixFilePermissions.toString(Files.getPosixFilePermissions(key)));
        MsgEvent st = p.call("repostate");
        assertEquals(fp, st.getParam("producer_key"));
        assertEquals(TestRig.REGION + "/" + TestRig.AGENT + "#" + fp, st.getParam("producer_pin"));
        // redeployed over the same directory (a new plugin id): the same key
        p.close();
        rigs.remove(p);
        TestRig again = producer("plugin-p-redeployed", scan);
        assertEquals(fp, again.repo.signingFingerprint(), "a redeployed producer keeps its key");
        // a producer over another directory is another producer
        TestRig other = producer("plugin-q", Files.createDirectories(tmp.resolve("other")));
        assertNotEquals(fp, other.repo.signingFingerprint());
        // a mirror has no producer key
        TestRig s = subscriber("plugin-s", mirror);
        assertNull(s.repo.signingFingerprint());
        assertNull(s.call("repostate").getParam("producer_pin"));
    }

    @Test
    void aProducerWhoseKeyCannotBeReadServesNoMirrorAndNeverMakesANewKey() throws Exception {
        TestRig p = producer("plugin-p", scan);
        Path key = p.db.stateDir().resolve(ProducerKey.FILE);
        p.close();
        rigs.remove(p);
        Files.writeString(key, "private=AAAA\npublic=AAAA\n");
        String damaged = Files.readString(key);
        TestRig again = producer("plugin-p", scan);
        assertNull(again.repo.signingFingerprint());
        assertEquals(damaged, Files.readString(key), "the damaged key is left for an admin, never replaced");
        assertNotNull(again.call("repostate").getParam("producer_key_error"));
        MsgEvent sub = again.requestTo("reposubscribe", "filerepo_name", "fr", "sync_gen", "ab", "sync_proto",
                RepoEngine.SYNC_PROTO, "sync_nonce", SyncReceiver.token(), "sync_auth", "00");
        Fabric.stamp(sub, "plugin-s");
        again.call(sub);
        MsgEvent answer = again.sent.stream().filter(x -> "reposubscribed".equals(x.getParam("action"))).findFirst().orElseThrow();
        assertEquals("false", answer.getParam("accepted"));
        assertTrue(answer.getParam("refusal").contains("signing key"), answer.getParam("refusal"));
        assertTrue(again.log.stream().anyMatch(l -> l.contains("cannot be used")), again.log.toString());
    }

    @Test
    void aPinIsANodeAndAProducerKeyNeverANodeOrADirectory() {
        assertNull(RepoEngine.Pin.parse("region-1/agent-1"));
        assertNull(RepoEngine.Pin.parse("region-1/agent-1:/data/scan"));
        assertNull(RepoEngine.Pin.parse("region-1/agent-1/plugin-p"));
        assertNull(RepoEngine.Pin.parse("region-1/agent-1#abc"));
        assertTrue(RepoEngine.Pin.why("region-1/agent-1:/data/scan").contains("no producer key"));
        RepoEngine.Pin pin = RepoEngine.Pin.parse(" region-1/agent-1#" + "AB".repeat(32) + " ");
        assertNotNull(pin);
        assertEquals("region-1/agent-1#" + "ab".repeat(32), pin.toString());
    }

    // =================================================================== another plugin on the producer's agent

    @Test
    void aPluginOnTheProducersAgentWithItsOwnKeyIsNeverTheProducer() throws Exception {
        TestRig p = producer("plugin-p", scan, "sync_key", "");
        TestRig s = subscriber("plugin-s", mirror, "sync_key", "", "sync_producers", p.repo.producerPin());
        write(scan.resolve("a.bin"), "weights a");
        subscribe(p, s);
        cycle(p, s);
        assertEquals("weights a", read(mirror.resolve("a.bin")));
        // a filerepo on the same agent producing the same name, from its own directory, with its own key
        Path rogueDir = Files.createDirectories(tmp.resolve("rogue"));
        write(rogueDir.resolve("a.bin"), "MALICIOUS");
        write(rogueDir.resolve("b.bin"), "MALICIOUS");
        TestRig x = producer("plugin-x", rogueDir, "sync_key", "");
        subscribe(x, s);
        cycle(x, s);
        assertEquals("weights a", read(mirror.resolve("a.bin")));
        assertFalse(Files.exists(mirror.resolve("b.bin")));
        // and what it deletes is ignored
        Files.delete(rogueDir.resolve("a.bin"));
        assertTrue(x.repo.scanOnce());
        assertFalse(within(1500, () -> !Files.exists(mirror.resolve("a.bin"))), "the other plugin deleted the producer's file");
        assertTrue(s.repo.syncUnpinned.get() > 0);
    }

    @Test
    void anAnswerCarryingTheProducersKeyAndSignatureFromAnotherPluginIsNotTheProducer() throws Exception {
        TestRig p = producer("plugin-p", scan, "sync_key", "");
        TestRig s = subscriber("plugin-s", mirror, "sync_key", "", "sync_producers", p.repo.producerPin());
        write(scan.resolve("a.bin"), "weights a");
        subscribe(p, s);
        cycle(p, s);
        MsgEvent real = p.sent.stream().filter(m -> "reposubscribed".equals(m.getParam("action"))).findFirst().orElseThrow();

        // another plugin on the agent (not producing) replays the producer's key and signature in its own answer
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("repo_dir", Files.createDirectories(tmp.resolve("other")).toString());
        cfg.put("sync_key", "");
        TestRig x = rig("plugin-x", cfg);
        x.repo.repoBroadcast("fr", "discover", 0);
        await(() -> s.sent.stream().anyMatch(m -> "reposubscribe".equals(m.getParam("action")) && "plugin-x".equals(m.getDstPlugin())),
                "the mirror asked plugin-x");
        MsgEvent ask = null;
        for (MsgEvent m : s.sent) if ("reposubscribe".equals(m.getParam("action")) && "plugin-x".equals(m.getDstPlugin())) ask = m;
        MsgEvent answer = x.plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, TestRig.REGION, TestRig.AGENT, "plugin-s");
        for (String k : new String[]{"action", "filerepo_name", "sync_proto", "accepted", "producer_dir", "producer_epoch",
                "producer_pubkey", "producer_key", "producer_sig"}) {
            answer.setParam(k, real.getParam(k));
        }
        answer.setParam("sync_nonce", ask.getParam("sync_nonce"));
        x.plugin.msgOut(answer);
        await(() -> s.repo.isKnownProducer(TestRig.REGION, TestRig.AGENT, "plugin-x"), "plugin-x known");
        long unverified = s.repo.syncUnverified.get();
        assertTrue(unverified > 0, "the replayed signature does not verify for plugin-x's answer");

        // its delete of what the producer delivered, signed or not, changes nothing
        String deletes = gson.toJson(List.of(new DeleteObject("a.bin", Hashes.content("weights a".getBytes()), "/x/a.bin", 1L)));
        MsgEvent took = offer(s, "plugin-x", "x.1", "{}", deletes, null);
        assertTrue("10".equals(took.getParam("status_code")) || "9".equals(took.getParam("status_code")), took.getParams().toString());
        assertFalse(within(1500, () -> !Files.exists(mirror.resolve("a.bin"))), "plugin-x deleted the producer's file");
    }

    @Test
    void anOfferNotSignedByThePinnedProducersKeyIsRefused() throws Exception {
        TestRig p = producer("plugin-p", scan, "sync_key", "");
        TestRig s = subscriber("plugin-s", mirror, "sync_key", "", "sync_producers", p.repo.producerPin());
        write(scan.resolve("a.bin"), "weights a");
        write(scan.resolve("keep.bin"), "keeps the scan dir non-empty");
        subscribe(p, s);
        cycle(p, s);
        String deletes = gson.toJson(List.of(new DeleteObject("a.bin", Hashes.content("weights a".getBytes()), "/x/a.bin", 1L)));
        // stamped as the producer's own plugin, but unsigned, or signed by another key, or signed for other content
        assertEquals("9", offer(s, "plugin-p", "u.1", "{}", deletes, null).getParam("status_code"));
        ProducerKey other = ProducerKey.loadOrCreate(Files.createDirectories(tmp.resolve("k"))).key;
        String me = SyncAuth.addr(TestRig.REGION, TestRig.AGENT, "plugin-p"), them = SyncAuth.addr(TestRig.REGION, TestRig.AGENT, "plugin-s");
        assertEquals("9", offer(s, "plugin-p", "u.2", "{}", deletes, other.sign(RepoEngine.offerParts("fr", me, them, "u.2",
                s.repo.getSyncGen(), "{}", deletes))).getParam("status_code"));
        assertTrue(Files.exists(mirror.resolve("a.bin")));
        assertTrue(s.repo.syncUnverified.get() >= 2);
        // the producer's own signed delete applies (as its scan would send it)
        Files.delete(scan.resolve("a.bin"));
        assertTrue(p.repo.scanOnce());
        await(() -> !Files.exists(mirror.resolve("a.bin")), "the producer's signed delete applied");
    }
}
