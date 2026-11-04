package io.cresco.filerepo;

import io.cresco.library.messaging.MsgEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The round-3 design beyond the review's own tests: explicit sync roles, the guards' longer horizons (since the last
 * acknowledgement), the generation marker of a replaced volume, the lock released by a failed start, and the default
 * repository's journaled move.
 */
class SyncRound3Test {

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
                && s.repo.isKnownProducer(TestRig.REGION, TestRig.AGENT, p.pluginId), s.pluginId + " subscribed");
    }

    static void await(BooleanSupplier cond, String what) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) fail("timed out waiting: " + what);
            Thread.sleep(20);
        }
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

    @Test
    void aRepositoryConfiguredAsAMirrorNeverActsAsAProducer() throws Exception {
        TestRig s = subscriber("plugin-s", mirror);
        // a scan dir, but sync_role=mirror: it answers no subscribe and broadcasts no discover
        TestRig m = producer("plugin-m", scan, "sync_role", "mirror");
        write(scan.resolve("x.txt"), "x");
        assertFalse(m.repo.isProducer());
        MsgEvent sub = s.requestTo("reposubscribe", "filerepo_name", "fr", "sync_gen", "ab", "sync_proto", RepoEngine.SYNC_PROTO,
                "sync_nonce", SyncReceiver.token());
        Fabric.stamp(sub, "plugin-s");
        m.call(sub);
        MsgEvent answer = m.sent.stream().filter(x -> "reposubscribed".equals(x.getParam("action"))).findFirst().orElseThrow();
        assertEquals("false", answer.getParam("accepted"));
        assertEquals("not a producer", answer.getParam("refusal"));
        assertTrue(m.log.stream().anyMatch(l -> l.contains("sync_role=mirror with a scan_dir")), m.log.toString());
    }

    @Test
    void aDrainAcrossDaysIsHeldBeforeHalfTheAcknowledgedCatalogIsGone() throws Exception {
        // the day budget off, the window short: only the total since the last acknowledgement is left to catch it
        TestRig p = producer("plugin-p", scan, "sync_delete_guard_window_ms", "1", "sync_delete_guard_day_fraction", "0");
        for (int i = 0; i < 100; i++) write(scan.resolve("f" + i + ".dat"), "f" + i);
        assertTrue(p.repo.scanOnce());
        int next = 0;
        while (p.repo.getDeletesHeld() == 0 && next < 100) {
            for (int i = 0; i < 8; i++) Files.delete(scan.resolve("f" + (next++) + ".dat"));
            Thread.sleep(5);
            assertTrue(p.repo.scanOnce());
        }
        assertTrue(p.repo.getDeletesHeld() > 0);
        assertTrue(p.repo.getTombstoneCount() <= 50, "deleted unheld: " + p.repo.getTombstoneCount());
        // the acknowledged size survives a restart
        p.close();
        rigs.remove(p);
        TestRig again = producer("plugin-p2", scan, "sync_delete_guard_window_ms", "1", "sync_delete_guard_day_fraction", "0");
        assertTrue(again.repo.scanOnce());
        assertTrue(again.repo.getDeletesHeld() > 0, "a restart does not forget the size acknowledged");
        // released: the size now is the acknowledged one; one more delete is ordinary
        assertTrue(again.repo.releaseHeldDeletes(again.repo.getHoldId()) > 0);
        assertTrue(again.repo.scanOnce());
        Files.delete(scan.resolve("f99.dat"));
        assertTrue(again.repo.scanOnce());
        assertEquals(0, again.repo.getDeletesHeld());
    }

    @Test
    void rewritesAcrossDaysAreHeldBeforeHalfTheCatalogIsRewrittenSinceTheLastAcknowledgement() throws Exception {
        TestRig p = producer("plugin-p", scan, "sync_rewrite_guard_day_fraction", "0");
        TestRig s = subscriber("plugin-s", mirror);
        subscribe(p, s);
        int n = 40;
        for (int i = 0; i < n; i++) write(scan.resolve("doc" + i + ".txt"), "real " + i);
        cycle(p, s);
        int i = 0;
        while (p.repo.getRewritesHeld() == 0 && i < n) {
            for (int k = 0; k < 6 && i < n; k++, i++) {
                Path f = write(scan.resolve("doc" + i + ".txt"), "rewritten " + i);
                Files.setLastModifiedTime(f, FileTime.fromMillis(System.currentTimeMillis() + 10_000));
            }
            assertTrue(p.repo.scanOnce());
        }
        assertTrue(p.repo.getRewritesHeld() > 0, "a rewrite spread over many cycles is held by the total since the last acknowledgement");
        assertTrue(i <= 30, "rewritten before the hold: " + i);
    }

    @Test
    void aMirrorWhoseVolumeWasReplacedStartsANewGenerationAndIsRefilled() throws Exception {
        TestRig p = producer("plugin-p", scan);
        TestRig s = subscriber("plugin-s", mirror);
        subscribe(p, s);
        for (int i = 0; i < 3; i++) write(scan.resolve("v" + i + ".bin"), "v " + i);
        cycle(p, s);
        String gen = s.repo.getSyncGen();
        s.close();
        rigs.remove(s);
        // a new, empty volume on the mount point: no files, no marker
        try (var st = Files.walk(mirror)) {
            st.sorted(Collections.reverseOrder()).filter(x -> !x.equals(mirror)).forEach(x -> x.toFile().delete());
        }
        TestRig again = subscriber("plugin-s", mirror);
        assertNotEquals(gen, again.repo.getSyncGen(), "a new generation");
        subscribe(p, again);
        assertTrue(p.repo.scanOnce());
        await(() -> Files.exists(mirror.resolve("v2.bin")), "refilled");
        assertTrue(again.log.stream().anyMatch(l -> l.contains("no generation marker")), again.log.toString());
    }

    @Test
    void aFailedStartReleasesTheDirectoryLock() throws Exception {
        Map<String, Object> bad = new HashMap<>();
        bad.put("repo_dir", mirror.toString());
        bad.put("db_driver", "no.such.Driver");
        TestRig failed = rig("plugin-bad", bad);
        assertNotNull(failed.db.openError());
        Map<String, Object> good = new HashMap<>();
        good.put("repo_dir", mirror.toString());
        TestRig ok = rig("plugin-good", good);
        assertNull(ok.db.openError(), "the failed start kept the directory locked: " + ok.db.openError());
    }

    @Test
    void anInterruptedMoveOfTheDefaultRepositoryIsCompletedAtTheNextStart() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        TestRig first = rig("plugin-old", cfg);
        Path pluginData = Path.of(first.plugin.getPluginDataDirectory());
        Path stable = first.repo.repoDirFile().toPath();
        Path legacy = pluginData.resolve("repo");
        Path landed = write(legacy.resolve("landed.bin"), "landed before the upgrade");
        first.db.addFile(landed.toString(), Hashes.content(landed), 1L, Files.size(landed));
        Path journal = first.db.stateDir().resolve("migration.journal");
        first.close();
        rigs.remove(first);
        // a crash right after the rename: the files moved, the journal is there, the catalog still names the old paths
        try (var st = Files.walk(stable)) {
            st.sorted(Collections.reverseOrder()).forEach(x -> x.toFile().delete());
        }
        Files.move(legacy, stable);
        java.util.Properties j = new java.util.Properties();
        j.setProperty("from", legacy.toString());
        j.setProperty("to", stable.toString());
        try (var out = Files.newOutputStream(journal)) {
            j.store(out, "test");
        }
        TestRig next = rig("plugin-old", cfg);
        assertFalse(Files.exists(journal), "the journal is done with");
        assertNotNull(next.repo.getFileInfo(stable.resolve("landed.bin").toString()), "the catalog names the file where it is");
        assertEquals("10", next.call("getfile", "file_path", stable.resolve("landed.bin").toString()).getParam("status"));
    }
}
