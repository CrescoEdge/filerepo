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
 * Review round 5 (review/plugins-20261104), filerepo 9a61d75. The shared TestRig now pins plugin-p's key on every mirror
 * that names no sync_producers, so ReviewRound4Test.aNewMirrorNeverPinsWhicheverPluginAnswersFirst exercises a PINNED
 * mirror. This restores the property it was written for -- an UNCONFIGURED mirror (sync_producers given, empty) takes
 * nothing from whichever plugin answers first -- and then checks the admin's way in. It passes on 9a61d75 and fails
 * on bd20d08 (trust on first use). Local only.
 */
class ReviewRound5Test {

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
        r.repo.start();
        return r;
    }

    TestRig producer(String pluginId, Path dir) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("filerepo_name", "fr");
        cfg.put("scan_dir", dir.toString());
        cfg.put("scan_delay", "3600000");
        cfg.put("scan_period", "3600000");
        cfg.put("sync_key", "");
        return rig(pluginId, cfg);
    }

    static boolean within(long ms, BooleanSupplier cond) throws Exception {
        long deadline = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return true;
            Thread.sleep(25);
        }
        return cond.getAsBoolean();
    }

    static String read(Path f) {
        try {
            return Files.readString(f);
        } catch (Exception e) {
            return null;
        }
    }

    @Test
    void anUnconfiguredMirrorTakesNothingFromWhicheverPluginAnswersFirst() throws Exception {
        Map<String, Object> mcfg = new HashMap<>();
        mcfg.put("filerepo_name", "fr");
        mcfg.put("repo_dir", mirror.toString());
        mcfg.put("sync_stream_buffer", "4096");
        mcfg.put("sync_key", "");
        mcfg.put("sync_producers", "");          // present and empty: no pin, the default an admin starts from
        TestRig s = rig("plugin-s", mcfg);

        Path rogueDir = Files.createDirectories(tmp.resolve("rogue"));
        Files.write(rogueDir.resolve("release.bin"), "MALICIOUS release".getBytes());
        TestRig rogue = producer("plugin-x", rogueDir);
        rogue.repo.repoBroadcast("fr", "discover", 0);   // the rogue answers first
        assertTrue(within(5000, () -> s.repo.isKnownProducer(TestRig.REGION, TestRig.AGENT, "plugin-x")), "the rogue answered");
        for (int i = 0; i < 3; i++) {
            rogue.repo.scanOnce();
            Thread.sleep(200);
        }
        Files.write(scan.resolve("release.bin"), "good release".getBytes());
        TestRig p = producer("plugin-p", scan);
        p.repo.repoBroadcast("fr", "discover", 0);
        assertTrue(within(5000, () -> s.repo.isKnownProducer(TestRig.REGION, TestRig.AGENT, "plugin-p")), "the producer answered");
        for (int i = 0; i < 3; i++) {
            p.repo.scanOnce();
            Thread.sleep(200);
        }
        assertFalse(Files.exists(mirror.resolve("release.bin")),
                "an unpinned mirror took release.bin (" + read(mirror.resolve("release.bin")) + ") from whoever answered");
        MsgEvent st = s.call("repostate");
        assertTrue(String.valueOf(st.getParam("pinned_producer")).startsWith("none"), st.getParams().toString());

        // the admin pins the real producer by its key; only then does the mirror sync, and only from it
        MsgEvent pin = s.call(TestRig.as(s.requestTo("syncpin", "confirm", mirror.toString(), "producer", TestRig.FABRIC_PIN), "admin"));
        assertEquals("10", pin.getParam("status"), pin.getParams().toString());
        p.repo.repoBroadcast("fr", "discover", 0);
        rogue.repo.repoBroadcast("fr", "discover", 0);
        boolean got = false;
        for (int i = 0; i < 10 && !got; i++) {
            p.repo.scanOnce();
            rogue.repo.scanOnce();
            got = within(500, () -> "good release".equals(read(mirror.resolve("release.bin"))));
        }
        assertTrue(got, "after the admin's pin the mirror holds " + read(mirror.resolve("release.bin")));
    }
    /**
     * Rounds 1-2 properties under the new pinning, with BOTH producers pinned (the shared harness pins only plugin-p,
     * so the original tests now pass trivially: the second producer is simply unpinned). A file both producers hold is
     * still updated by one of them, and a delete by one never removes what the other still holds.
     */
    @Test
    void twoPinnedProducersNeitherFreezeNorDeleteEachOthersFiles() throws Exception {
        Path other = Files.createDirectories(tmp.resolve("other"));
        Files.write(scan.resolve("model.bin"), "weights v1".getBytes());
        Files.write(other.resolve("model.bin"), "weights v1".getBytes());
        Files.write(scan.resolve("shared.txt"), "same".getBytes());
        Files.write(other.resolve("shared.txt"), "same".getBytes());
        Files.write(other.resolve("keep.txt"), "keeps q's scan dir non-empty".getBytes());
        TestRig p = producer("plugin-p", scan);
        TestRig q = producer("plugin-q", other);
        String qPin = TestRig.REGION + "/" + TestRig.AGENT + "#" + q.repo.signingFingerprint();
        Map<String, Object> mcfg = new HashMap<>();
        mcfg.put("filerepo_name", "fr");
        mcfg.put("repo_dir", mirror.toString());
        mcfg.put("sync_stream_buffer", "4096");
        mcfg.put("sync_key", "");
        mcfg.put("sync_producers", TestRig.FABRIC_PIN + "," + qPin);
        TestRig s = rig("plugin-s", mcfg);
        for (TestRig prod : new TestRig[]{p, q}) {
            prod.repo.repoBroadcast("fr", "discover", 0);
            TestRig pp = prod;
            assertTrue(within(5000, () -> s.repo.isKnownProducer(TestRig.REGION, TestRig.AGENT, pp.pluginId)), prod.pluginId);
        }
        for (int i = 0; i < 3; i++) {
            p.repo.scanOnce();
            q.repo.scanOnce();
            Thread.sleep(300);
        }
        assertEquals("weights v1", read(mirror.resolve("model.bin")));
        assertTrue(Files.exists(mirror.resolve("keep.txt")), "plugin-q is pinned and synced");

        Files.write(scan.resolve("model.bin"), "weights v2".getBytes());
        Files.setLastModifiedTime(scan.resolve("model.bin"), java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() + 10_000));
        Files.delete(other.resolve("shared.txt"));
        for (int i = 0; i < 4; i++) {
            p.repo.scanOnce();
            q.repo.scanOnce();
            Thread.sleep(300);
        }
        assertTrue(within(3000, () -> "weights v2".equals(read(mirror.resolve("model.bin")))),
                "plugin-p's update of a file plugin-q also holds was refused: " + read(mirror.resolve("model.bin")));
        assertTrue(Files.exists(mirror.resolve("shared.txt")), "plugin-q's delete removed a file plugin-p still holds");
    }
    /**
     * MED (round 5, gross counting). The day and total budgets count originals only: files cataloged before the day's
     * start (or the last acknowledgement) and before the last release. Files added since are protected by the 15-minute
     * window alone (10% per window): a drip just under it drains every file that arrived after the last release and
     * within the last day -- the newest data -- with no hold. (Window shortened to 300 ms; the arithmetic is the same.)
     */
    @Test
    void filesAddedSinceTheLastReleaseAreHeldBeforeHalfOfThemAreDeleted() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("filerepo_name", "fr");
        cfg.put("scan_dir", scan.toString());
        cfg.put("scan_delay", "3600000");
        cfg.put("scan_period", "3600000");
        cfg.put("sync_key", "");
        cfg.put("sync_delete_guard_window_ms", "300");
        for (int i = 0; i < 4; i++) Files.write(scan.resolve("old" + i + ".dat"), ("old " + i).getBytes());
        TestRig p = rig("plugin-p", cfg);
        assertTrue(p.repo.scanOnce());                        // the first scan: these four are the originals
        int n = 100;
        for (int i = 0; i < n; i++) Files.write(scan.resolve("new" + i + ".dat"), ("new data " + i).getBytes());
        assertTrue(p.repo.scanOnce());
        Thread.sleep(350);
        int left = n, next = 0, windows = 0;
        while (left > n / 2 && p.repo.getDeletesHeld() == 0 && windows < 30) {
            int drip = Math.max(3, (int) Math.floor((left + 4) * 0.09));
            for (int i = 0; i < drip && next < n; i++) Files.delete(scan.resolve("new" + (next++) + ".dat"));
            left = n - next;
            assertTrue(p.repo.scanOnce());
            Thread.sleep(350);
            windows++;
        }
        assertTrue(p.repo.getDeletesHeld() > 0, (n - left) + " of the " + n + " files added after the first scan deleted over "
                + windows + " windows with no hold (tombstones " + p.repo.getTombstoneCount() + ")");
    }
}
