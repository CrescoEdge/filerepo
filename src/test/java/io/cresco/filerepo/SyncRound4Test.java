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

    // =================================================================== pinning: never by who answers

    @Test
    void aMirrorWithNoPinTakesNothingFromAnyoneAndListsWhoAnswered() throws Exception {
        TestRig s = subscriber("plugin-s", mirror, "sync_producers", "");
        Path rogueDir = Files.createDirectories(tmp.resolve("rogue"));
        write(rogueDir.resolve("release.bin"), "MALICIOUS");
        TestRig x = producer("plugin-x", rogueDir);
        subscribe(x, s);
        cycle(x, s);
        TestRig p = producer("plugin-p", scan);
        write(scan.resolve("release.bin"), "good");
        subscribe(p, s);
        cycle(p, s);
        assertFalse(Files.exists(mirror.resolve("release.bin")), "an unpinned mirror took a file");
        MsgEvent st = s.call("repostate");
        assertTrue(st.getParam("pinned_producer").startsWith("none"), st.getParams().toString());
        String candidates = st.getParam("candidate_producers");
        assertTrue(candidates.contains(p.repo.producerPin()) && candidates.contains(x.repo.producerPin()), candidates);
        assertTrue(s.log.stream().anyMatch(l -> l.contains("syncs nothing until an admin pins")), s.log.toString());
        assertTrue(s.log.stream().anyMatch(l -> l.contains("syncpin confirm=") && l.contains(p.repo.producerPin())), s.log.toString());
        // nothing survives a restart as a pin either
        s.close();
        rigs.remove(s);
        TestRig again = subscriber("plugin-s", mirror, "sync_producers", "");
        assertTrue(again.call("repostate").getParam("pinned_producer").startsWith("none"));
    }

    @Test
    void sync_producersNamingNoKeyPinsNothing() throws Exception {
        TestRig p = producer("plugin-p", scan);
        write(scan.resolve("a.txt"), "a");
        TestRig s = subscriber("plugin-s", mirror, "sync_producers", TestRig.REGION + "/" + TestRig.AGENT + ":" + scan
                + "," + TestRig.REGION + "/" + TestRig.AGENT);
        assertTrue(s.log.stream().anyMatch(l -> l.contains("pins nothing")), s.log.toString());
        p.repo.repoBroadcast("fr", "discover", 0);
        assertTrue(p.repo.scanOnce());
        Thread.sleep(500);
        assertFalse(Files.exists(mirror.resolve("a.txt")));
        assertTrue(s.call("repostate").getParam("pinned_producer").startsWith("none"));
        // and syncpin does not override configuration
        assertEquals("pin", s.call(TestRig.as(s.requestTo("syncpin", "confirm", mirror.toString(), "producer",
                p.repo.producerPin()), "admin")).getParam("refusal"));
    }

    // =================================================================== a producer that moved

    @Test
    void aMovedProducersContentIsFrozenAndShownUntilAnAdminHandsItOn() throws Exception {
        TestRig p = producer("plugin-p", scan);
        TestRig s = subscriber("plugin-s", mirror);
        write(scan.resolve("m.bin"), "v1");
        write(scan.resolve("keep.txt"), "keep");
        subscribe(p, s);
        cycle(p, s);
        assertEquals("v1", read(mirror.resolve("m.bin")));
        // the producer moves to another directory: a new key; the admin re-pins the mirror in its configuration
        p.close();
        rigs.remove(p);
        Path moved = Files.createDirectories(tmp.resolve("moved"));
        write(moved.resolve("m.bin"), "v1");
        write(moved.resolve("keep.txt"), "keep");
        TestRig p2 = producer("plugin-p2", moved);
        assertNotEquals(TestRig.FABRIC_FINGERPRINT, p2.repo.signingFingerprint());
        s.close();
        rigs.remove(s);
        TestRig s2 = subscriber("plugin-s", mirror, "sync_producers", p2.repo.producerPin());
        Path f = write(moved.resolve("m.bin"), "v2");
        Files.setLastModifiedTime(f, java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() + 10_000));
        subscribe(p2, s2);
        for (int i = 0; i < 2; i++) {
            assertTrue(p2.repo.scanOnce());
            Thread.sleep(300);
        }
        assertEquals("v1", read(mirror.resolve("m.bin")), "the old producer's content is frozen without adopt");
        MsgEvent st = s2.call("repostate");
        assertEquals("2", st.getParam("orphaned_files"), st.getParams().toString());
        assertTrue(st.getParam("orphaned_from").contains(TestRig.FABRIC_PIN), st.getParam("orphaned_from"));
        String resolve = st.getParam("orphaned_resolve");
        assertTrue(resolve.contains("syncpin confirm=" + mirror) && resolve.contains("producer=" + p2.repo.producerPin()), resolve);
        // the admin hands it on, by the old pin (or the holder repostate shows): the new producer updates it
        String from = resolve.substring(resolve.indexOf("from=") + 5, resolve.indexOf(" producer="));
        MsgEvent adopt = s2.call(TestRig.as(s2.requestTo("syncpin", "confirm", mirror.toString(), "adopt", "true", "from",
                from, "producer", p2.repo.producerPin()), "admin"));
        assertEquals("10", adopt.getParam("status"), adopt.getParams().toString());
        subscribe(p2, s2);
        assertTrue(p2.repo.scanOnce());
        await(() -> "v2".equals(read(mirror.resolve("m.bin"))), "the moved producer's update applied after the adopt");
        assertNull(s2.call("repostate").getParam("orphaned_files"));
    }

    @Test
    void aPinAnEarlierBuildRecordedByDirectoryPinsNothingAndItsContentCanBeHandedOn() throws Exception {
        TestRig s = subscriber("plugin-s", mirror, "sync_producers", "");
        RepoEngine.Identity legacy = new RepoEngine.Identity(Fabric.TENANT, TestRig.REGION, TestRig.AGENT, "fr", null, scan.toString());
        write(mirror.resolve("m.bin"), "v1");
        s.db.recordMirrored(legacy.holder(), "m.bin", Hashes.content("v1".getBytes()), DBEngine.HOLD_FETCHED);
        s.db.setMeta(RepoEngine.META_PRODUCER_PIN, legacy.full());
        s.close();
        rigs.remove(s);
        TestRig p = producer("plugin-p", scan);
        Path f = write(scan.resolve("m.bin"), "v2");
        write(scan.resolve("keep.txt"), "keep");
        TestRig again = subscriber("plugin-s", mirror, "sync_producers", "");
        MsgEvent st = again.call("repostate");
        assertTrue(st.getParam("pinned_producer").startsWith("none"), st.getParams().toString());
        assertEquals("1", st.getParam("orphaned_files"));
        assertTrue(st.getParam("orphaned_from").contains("by directory"), st.getParam("orphaned_from"));
        subscribe(p, again);
        assertTrue(p.repo.scanOnce());
        Thread.sleep(500);
        assertEquals("v1", read(mirror.resolve("m.bin")));
        // the admin pins the producer by its key, adopting what the earlier pin delivered
        MsgEvent pin = again.call(TestRig.as(again.requestTo("syncpin", "confirm", mirror.toString(), "adopt", "true",
                "producer", p.repo.producerPin()), "admin"));
        assertEquals("10", pin.getParam("status"), pin.getParams().toString());
        subscribe(p, again);
        assertTrue(p.repo.scanOnce());
        await(() -> "v2".equals(read(mirror.resolve("m.bin"))), "updated once adopted");
        assertEquals("keep", read(mirror.resolve("keep.txt")));
    }

    // =================================================================== the guards' budgets: gross, persisted, monotonic

    TestRig restart(TestRig p, String pluginId, Path dir, String... kv) throws Exception {
        p.close();
        rigs.remove(p);
        return producer(pluginId, dir, kv);
    }

    @Test
    void deletesReplacedByNewFilesAreHeldByTheDayBudgetAcrossRestarts() throws Exception {
        String[] cfg = {"sync_delete_guard_window_ms", "1"};
        TestRig p = producer("plugin-p", scan, cfg);
        for (int i = 0; i < 40; i++) write(scan.resolve("doc" + i), "original " + i);
        assertTrue(p.repo.scanOnce());
        int deleted = 0, cycles = 0;
        while (p.repo.getDeletesHeld() == 0 && deleted < 30) {
            for (int i = 0; i < 2; i++) {
                Files.delete(scan.resolve("doc" + deleted));
                write(scan.resolve("doc" + deleted + ".locked"), "ENCRYPTED");
                deleted++;
            }
            assertTrue(p.repo.scanOnce());
            if (p.repo.getDeletesHeld() == 0) p = restart(p, "plugin-p", scan, cfg);   // every cycle: nothing is forgotten
            cycles++;
        }
        assertTrue(p.repo.getDeletesHeld() > 0, deleted + " originals deleted and replaced over " + cycles + " restarts, never held");
        assertTrue(deleted <= 12, "a day's budget is 25% of 40: held by the sixth pair, not " + deleted);
        assertEquals(deleted - 2, p.repo.getTombstoneCount());
    }

    @Test
    void aDrainOfReplacedFilesIsHeldByTheTotalSinceTheLastAcknowledgement() throws Exception {
        TestRig p = producer("plugin-p", scan, "sync_delete_guard_window_ms", "1", "sync_delete_guard_day_fraction", "0");
        for (int i = 0; i < 40; i++) write(scan.resolve("doc" + i), "original " + i);
        assertTrue(p.repo.scanOnce());
        int deleted = 0;
        while (p.repo.getDeletesHeld() == 0 && deleted < 39) {
            for (int i = 0; i < 3; i++) {
                Files.delete(scan.resolve("doc" + deleted));
                write(scan.resolve("doc" + deleted + ".locked"), "ENCRYPTED");
                deleted++;
            }
            Thread.sleep(3);
            assertTrue(p.repo.scanOnce());
        }
        assertTrue(p.repo.getDeletesHeld() > 0, "the catalog never shrank, and every original went");
        assertTrue(p.repo.getTombstoneCount() <= 20, "unheld deletes are bounded by half of the 40: " + p.repo.getTombstoneCount());
        // the release acknowledges the total: the next few are ordinary again
        assertTrue(p.repo.releaseHeldDeletes(p.repo.getHoldId()) > 0);
        assertTrue(p.repo.scanOnce());
        assertEquals(0, p.repo.getDeletesHeld());
        for (int i = 0; i < 3; i++) Files.delete(scan.resolve("doc" + (deleted++)));
        Thread.sleep(3);
        assertTrue(p.repo.scanOnce());
        assertEquals(0, p.repo.getDeletesHeld(), "after the acknowledgement, three more deletes are ordinary " + p.log);
    }

    @Test
    void aReleaseConsumesOnlyTheBudgetsThatHeldIt() throws Exception {
        TestRig p = producer("plugin-p", scan, "sync_delete_guard_window_ms", "1");
        for (int i = 0; i < 100; i++) write(scan.resolve("f" + i), "f" + i);
        for (int i = 0; i < 10; i++) write(scan.resolve("sub/s" + i), "s" + i);
        assertTrue(p.repo.scanOnce());
        // a drip under the day's budget (25% of 110: 27.5)
        int next = 0;
        for (int c = 0; c < 3; c++) {
            for (int i = 0; i < 8; i++) Files.delete(scan.resolve("f" + (next++)));
            Thread.sleep(3);
            assertTrue(p.repo.scanOnce());
            assertEquals(0, p.repo.getDeletesHeld(), "cycle " + c);
        }
        // an unrelated hold (a whole subtree gone), released by an admin
        for (int i = 0; i < 10; i++) Files.delete(scan.resolve("sub/s" + i));
        assertTrue(p.repo.scanOnce());
        assertEquals(10, p.repo.getDeletesHeld());
        assertEquals(10, p.repo.releaseHeldDeletes(p.repo.getHoldId()));
        assertTrue(p.repo.scanOnce());
        assertEquals(0, p.repo.getDeletesHeld());
        // the day's budget kept counting: 24 + 4 is over it
        for (int i = 0; i < 4; i++) Files.delete(scan.resolve("f" + (next++)));
        Thread.sleep(3);
        assertTrue(p.repo.scanOnce());
        assertEquals(4, p.repo.getDeletesHeld(), "a release of a subtree's hold reset the day's budget");
    }

    @Test
    void rewritesAndDeletesShareTheBudgets() throws Exception {
        TestRig p = producer("plugin-p", scan, "sync_delete_guard_window_ms", "1");
        for (int i = 0; i < 40; i++) write(scan.resolve("doc" + i), "original " + i);
        assertTrue(p.repo.scanOnce());
        for (int i = 0; i < 6; i++) {
            Path f = write(scan.resolve("doc" + i), "ENCRYPTED " + i);
            Files.setLastModifiedTime(f, java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() + 10_000));
        }
        assertTrue(p.repo.scanOnce());
        assertEquals(0, p.repo.getRewritesHeld(), "six rewrites of forty: under every budget");
        for (int i = 10; i < 15; i++) Files.delete(scan.resolve("doc" + i));
        Thread.sleep(3);
        assertTrue(p.repo.scanOnce());
        assertEquals(5, p.repo.getDeletesHeld(), "six rewritten and five deleted in a day is over a quarter of forty");
    }

    @Test
    void theRewriteGuardsDayIsKeptAcrossARestart() throws Exception {
        String[] cfg = {"sync_rewrite_guard_total_fraction", "0"};
        TestRig p = producer("plugin-p", scan, cfg);
        for (int i = 0; i < 40; i++) write(scan.resolve("doc" + i), "original " + i);
        assertTrue(p.repo.scanOnce());
        int i = 0;
        for (int c = 0; c < 2; c++) {
            for (int k = 0; k < 6; k++, i++) {
                Path f = write(scan.resolve("doc" + i), "ENCRYPTED " + i);
                Files.setLastModifiedTime(f, java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() + 10_000));
            }
            assertTrue(p.repo.scanOnce());
            if (c == 0) {
                assertEquals(0, p.repo.getRewritesHeld());
                p = restart(p, "plugin-p", scan, cfg);
            }
        }
        assertEquals(6, p.repo.getRewritesHeld(), "twelve of forty rewritten within a day, across a restart");
    }

    @Test
    void aClockThatMovedWhileDownClearsNoBudget() throws Exception {
        String[] cfg = {"sync_delete_guard_window_ms", "1"};
        TestRig p = producer("plugin-p", scan, cfg);
        for (int i = 0; i < 40; i++) write(scan.resolve("doc" + i), "original " + i);
        assertTrue(p.repo.scanOnce());
        int next = 0;
        for (int c = 0; c < 4; c++) {
            for (int i = 0; i < 2; i++) Files.delete(scan.resolve("doc" + (next++)));
            Thread.sleep(3);
            assertTrue(p.repo.scanOnce());
        }
        assertEquals(0, p.repo.getDeletesHeld());
        // the wall clock jumps three days ahead while the producer is down (or it was down three days): only an hour of
        // it counts, so the day's eight deletes still count
        p.db.setMeta(GuardLedger.META_WALL, String.valueOf(System.currentTimeMillis() - 3L * 24 * 3600 * 1000));
        p = restart(p, "plugin-p", scan, cfg);
        assertTrue(p.log.stream().anyMatch(l -> l.contains("count only 60 min")), p.log.toString());
        for (int i = 0; i < 3; i++) Files.delete(scan.resolve("doc" + (next++)));
        assertTrue(p.repo.scanOnce());
        assertEquals(3, p.repo.getDeletesHeld(), "a clock that moved three days cleared the day's budget");
        // a clock behind the last record is said so, and ages nothing
        p.db.setMeta(GuardLedger.META_WALL, String.valueOf(System.currentTimeMillis() + 24L * 3600 * 1000));
        p = restart(p, "plugin-p", scan, cfg);
        assertTrue(p.log.stream().anyMatch(l -> l.contains("behind the guards' last record")), p.log.toString());
        assertTrue(p.repo.scanOnce());
        assertEquals(3, p.repo.getDeletesHeld());
    }
}
