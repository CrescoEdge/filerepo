package io.cresco.filerepo;

import io.cresco.library.messaging.MsgEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Adversarial review, round 4 (review/plugins-20261104), of filerepo bd20d08: producers pinned by stamped node and
 * reported directory (trust on first use by default), cumulative delete budgets, the mirror's generation marker. Every
 * test FAILS on bd20d08. Local only.
 */
class ReviewRound4Test {

    @TempDir Path tmp;
    final Fabric fabric = new Fabric();
    final List<TestRig> rigs = new ArrayList<>();
    Path scan, mirror;

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

    // =================================================================== pinning

    /**
     * HIGH (default configuration). With no sync_producers (and sync_key now only extra proof), a mirror pins the first
     * verified producer that answers, on first start (and after syncpin reset=true). Round 3's default synced nothing;
     * now any plugin of the tenant that broadcasts a discover before the real producer does -- every 100 ms, against
     * the producer's scan_period -- is pinned for good, and the real producer is refused at every new mirror.
     */
    @Test
    void aNewMirrorNeverPinsWhicheverPluginAnswersFirst() throws Exception {
        TestRig s = subscriber("plugin-s", mirror, "sync_key", "");
        Path rogueDir = Files.createDirectories(tmp.resolve("rogue"));
        write(rogueDir.resolve("release.bin"), "MALICIOUS release");
        TestRig rogue = producer("plugin-x", rogueDir, "sync_key", "");
        subscribe(rogue, s);                       // the rogue's discover arrives first
        cycle(rogue, s);

        TestRig p = producer("plugin-p", scan, "sync_key", "");
        write(scan.resolve("release.bin"), "good release");
        subscribe(p, s);
        assertTrue(p.repo.scanOnce());
        Thread.sleep(1000);
        assertEquals("good release", read(mirror.resolve("release.bin")),
                "the new mirror pinned whichever plugin answered first (" + s.repo.pinnedProducers()
                        + ") and refuses its real producer");
    }

    /**
     * MED-HIGH. A producer's identity is its stamped NODE plus the directory it REPORTS (producer_dir): the plugin id is
     * dropped, and the directory is the answerer's own claim (with a sync_key, a key holder can prove any directory).
     * Any plugin on the producer's agent answers reposubscribe with the producer's directory (repostate shows it as
     * pinned_producer), is the pinned producer at every mirror, and deletes what the producer delivered -- bypassing the
     * producer's own delete guard.
     */
    @Test
    void aPluginOnTheProducersAgentCannotClaimTheProducersDirectory() throws Exception {
        TestRig p = producer("plugin-p", scan, "sync_key", "");
        TestRig s = subscriber("plugin-s", mirror, "sync_key", "");
        subscribe(p, s);
        write(scan.resolve("a.bin"), "weights a");
        write(scan.resolve("b.bin"), "weights b");
        cycle(p, s);
        String pinned = s.call("repostate").getParam("pinned_producer");

        // another plugin on the same agent (any bundle: here a non-producing filerepo instance)
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("repo_dir", Files.createDirectories(tmp.resolve("other")).toString());
        cfg.put("sync_key", "");
        TestRig rogue = rig("plugin-x", cfg);
        rogue.repo.repoBroadcast("fr", "discover", 0);
        await(() -> s.sent.stream().anyMatch(m -> "reposubscribe".equals(m.getParam("action")) && "plugin-x".equals(m.getDstPlugin())),
                "the mirror asked plugin-x");
        MsgEvent ask = null;
        for (MsgEvent m : s.sent) if ("reposubscribe".equals(m.getParam("action")) && "plugin-x".equals(m.getDstPlugin())) ask = m;
        Thread.sleep(200);

        MsgEvent answer = rogue.plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, TestRig.REGION, TestRig.AGENT, "plugin-s");
        answer.setParam("action", "reposubscribed");
        answer.setParam("filerepo_name", "fr");
        answer.setParam("sync_proto", RepoEngine.SYNC_PROTO);
        answer.setParam("accepted", "true");
        answer.setParam("producer_dir", scan.toString());          // the producer's directory, claimed
        answer.setParam("sync_nonce", ask.getParam("sync_nonce"));
        rogue.plugin.msgOut(answer);
        await(() -> s.repo.isKnownProducer(TestRig.REGION, TestRig.AGENT, "plugin-x"), "plugin-x known");

        MsgEvent diff = rogue.plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, TestRig.REGION, TestRig.AGENT, "plugin-s");
        diff.setParam("action", "repolistin");
        diff.setParam("transfer_id", "x.1");
        diff.setParam("sync_proto", RepoEngine.SYNC_PROTO);
        diff.setParam("sync_gen", ask.getParam("sync_gen"));
        diff.setCompressedParam("repolistin", "{}");
        diff.setCompressedParam("repodeletes", new com.google.gson.Gson().toJson(List.of(
                new DeleteObject("a.bin", Hashes.content("weights a".getBytes()), "/x/a.bin", System.currentTimeMillis()),
                new DeleteObject("b.bin", Hashes.content("weights b".getBytes()), "/x/b.bin", System.currentTimeMillis()))));
        MsgEvent took = rogue.plugin.sendRPC(diff, 5000);
        boolean gone = within(5000, () -> !Files.exists(mirror.resolve("a.bin")));
        assertFalse(gone, "plugin-x, on the producer's agent, claimed its directory (" + pinned + "), was taken as the pinned"
                + " producer and deleted a.bin and b.bin at the mirror (repolistin " + (took == null ? null : took.getParam("status_code")) + ")");
    }

    // =================================================================== the cumulative delete guard

    /**
     * MED. The total budget ("never past 50% since the last acknowledgement") is a NET shrink from the catalog's high-water
     * mark: deletes that are replaced by new files (ransomware that writes x.locked and deletes x) never shrink it. Only
     * the rolling-day budget (25%) is left, and that one is kept in memory: a restart forgets it. Deleting originals
     * just under the window budget, replacing each, and restarting once a "day" drains every original.
     */
    @Test
    void replacedDeletesAreHeldBeforeHalfTheOriginalsAreGone() throws Exception {
        String[] cfg = {"sync_delete_guard_window_ms", "300"};
        TestRig p = producer("plugin-p", scan, cfg);
        int n = 60;
        for (int i = 0; i < n; i++) write(scan.resolve("orig" + i + ".doc"), "original " + i);
        assertTrue(p.repo.scanOnce());
        int deleted = 0, junk = 0, windows = 0;
        while (deleted < 40 && p.repo.getDeletesHeld() == 0 && windows < 40) {
            for (int i = 0; i < 5; i++) {
                Files.delete(scan.resolve("orig" + (deleted++) + ".doc"));
                write(scan.resolve("orig" + (junk++) + ".doc.locked"), "ENCRYPTED " + junk);
            }
            assertTrue(p.repo.scanOnce());
            Thread.sleep(350);
            windows++;
            if (windows % 3 == 0 && p.repo.getDeletesHeld() == 0) {
                // a restart (or a day) later: the rolling day is forgotten, the net shrink is still zero
                p.close();
                rigs.remove(p);
                p = producer("plugin-p", scan, cfg);
            }
        }
        assertTrue(p.repo.getDeletesHeld() > 0 || deleted <= n / 2, deleted + " of " + n + " originals deleted (each replaced"
                + " by a .locked file) over " + windows + " windows with no hold");
    }

    // =================================================================== the mirror's generation marker

    /**
     * MED. The marker catches a missing marker or an entirely empty top level. A mirror that lost its files but kept its
     * directories (find -type f -delete, a failed disk under one subtree) still lists entries at the top level and keeps
     * its marker: its producer has everything settled for it, and nothing is ever offered again.
     */
    @Test
    void aMirrorThatLostItsFilesButNotItsDirectoriesIsRefilled() throws Exception {
        TestRig p = producer("plugin-p", scan);
        TestRig s = subscriber("plugin-s", mirror);
        subscribe(p, s);
        for (int i = 0; i < 4; i++) write(scan.resolve("set" + i + "/model.bin"), "model " + i);
        cycle(p, s);
        assertTrue(Files.exists(mirror.resolve("set2/model.bin")));

        s.close();
        rigs.remove(s);
        for (int i = 0; i < 4; i++) Files.delete(mirror.resolve("set" + i + "/model.bin"));
        TestRig again = subscriber("plugin-s", mirror);
        subscribe(p, again);
        for (int i = 0; i < 3; i++) {
            assertTrue(p.repo.scanOnce());
            Thread.sleep(300);
        }
        assertTrue(within(3000, () -> Files.exists(mirror.resolve("set2/model.bin"))),
                "the mirror lost every file (its directories remain) and is never refilled; pending at the producer "
                        + p.db.pendingCount(subKey(again)));
    }
}
