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
 * Adversarial review, round 3 (review/plugins-20261104), of filerepo a998024: sync_key, stable producer keys, orphaned
 * claims, the windowed delete guard, the rewrite guard, the state directory and its lock, the persisted generation.
 * Every test FAILS on a998024. Local only.
 */
class ReviewRound3Test {

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

    static long count(Path dir, String suffix) throws Exception {
        try (var st = Files.list(dir)) {
            return st.filter(p -> p.getFileName().toString().endsWith(suffix)).count();
        }
    }

    // =================================================================== sync_key: what a holder of the key can do

    /**
     * HIGH. sync_key is one symmetric secret shared by the producer AND every mirror of the name, and a mirror takes the
     * producer_key a key-holding answerer claims, unverified, as the identity its claims are recorded under. Every
     * mirror is told the real producer's key (in reposubscribed). So any key holder -- one compromised mirror, or any
     * bundle on a mirror's agent that reads its config -- answers as a producer with the real producer's key, becomes
     * the same holder at every other mirror, and deletes (or replaces) everything the real producer delivered. The real
     * producer has those files settled and never offers them again.
     */
    @Test
    void aKeyHolderCannotImpersonateTheRealProducersStableKey() throws Exception {
        TestRig p = producer("plugin-p", scan);
        TestRig s = subscriber("plugin-s", mirror);
        subscribe(p, s);
        write(scan.resolve("a.bin"), "weights a");
        write(scan.resolve("b.bin"), "weights b");
        cycle(p, s);
        assertTrue(Files.exists(mirror.resolve("a.bin")) && Files.exists(mirror.resolve("b.bin")));

        // a holder of the repository's sync_key (another mirror's agent, say), which learned the producer's key
        Path rogueDir = Files.createDirectories(tmp.resolve("rogue"));
        write(rogueDir.resolve("a.bin"), "weights a");
        write(rogueDir.resolve("b.bin"), "weights b");
        write(rogueDir.resolve("decoy.txt"), "keeps the rogue's own scan dir non-empty");
        TestRig rogue = producer("plugin-x", rogueDir);
        rogue.db.setMeta(RepoEngine.META_PRODUCER_KEY, p.repo.producerKey());
        subscribe(rogue, s);
        cycle(rogue, s);

        Files.delete(rogueDir.resolve("a.bin"));
        Files.delete(rogueDir.resolve("b.bin"));
        assertTrue(rogue.repo.scanOnce());
        boolean wiped = within(5000, () -> !Files.exists(mirror.resolve("a.bin")) && !Files.exists(mirror.resolve("b.bin")));
        for (int i = 0; i < 3; i++) {
            assertTrue(p.repo.scanOnce());
            Thread.sleep(200);
        }
        assertTrue(Files.exists(mirror.resolve("a.bin")),
                "plugin-x, holding the shared sync_key and claiming plugin-p's producer_key, deleted plugin-p's files at"
                        + " the mirror (both gone: " + wiped + "); plugin-p never offers them again (pending "
                        + p.db.pendingCount(subKey(s)) + ")");
    }

    /**
     * HIGH (the round-2 property, against a key holder). The adapted round-2 test proves a plugin WITHOUT the key is
     * never taken. Every mirror holds the key, though: a key holder that offers a name first (here release-2.bin, before
     * the real producer publishes it) still owns it at every mirror, and the real producer's file is refused as "another
     * producer's content" forever.
     */
    @Test
    void aKeyHolderCannotOwnANameBeforeTheRealProducerPublishesIt() throws Exception {
        TestRig p = producer("plugin-p", scan);
        TestRig s = subscriber("plugin-s", mirror);
        subscribe(p, s);
        write(scan.resolve("release-1.bin"), "good release 1");
        cycle(p, s);

        Path rogueDir = Files.createDirectories(tmp.resolve("rogue"));
        write(rogueDir.resolve("release-2.bin"), "MALICIOUS release 2");
        TestRig rogue = producer("plugin-x", rogueDir);   // holds the repository's sync_key, as every mirror does
        subscribe(rogue, s);
        cycle(rogue, s);

        write(scan.resolve("release-2.bin"), "good release 2");
        assertTrue(p.repo.scanOnce());
        Thread.sleep(1000);
        assertEquals("good release 2", read(mirror.resolve("release-2.bin")),
                "a key holder owns release-2.bin at the mirror; the real producer's is refused (conflicts="
                        + s.repo.syncConflicts.get() + ")");
    }

    /**
     * MED-HIGH. Orphaned claims "protect their files from every delete", but not from an update: once the producer has
     * been silent for sync_holder_ttl_ms (a decommissioned producer: the mirror now holds the only copy), any key holder
     * may overwrite the file (it matches the orphaned content), which drops the orphaned claim, and then delete it.
     */
    @Test
    void anOrphanedProducersFilesCannotBeOverwrittenAndThenDeleted() throws Exception {
        TestRig p = producer("plugin-p", scan);
        TestRig s = subscriber("plugin-s", mirror);
        subscribe(p, s);
        write(scan.resolve("archive.bin"), "the archived dataset");
        cycle(p, s);
        // the producer is decommissioned; sync_holder_ttl_ms (30 days) passes
        p.close();
        rigs.remove(p);
        s.repo.orphanSilentProducers(System.currentTimeMillis() + 1000);

        Path rogueDir = Files.createDirectories(tmp.resolve("rogue"));
        write(rogueDir.resolve("archive.bin"), "REPLACED");
        write(rogueDir.resolve("decoy1.txt"), "1");
        write(rogueDir.resolve("decoy2.txt"), "2");
        TestRig rogue = producer("plugin-x", rogueDir);
        subscribe(rogue, s);
        cycle(rogue, s);
        String afterOverwrite = read(mirror.resolve("archive.bin"));
        Files.delete(rogueDir.resolve("archive.bin"));
        assertTrue(rogue.repo.scanOnce());
        within(3000, () -> !Files.exists(mirror.resolve("archive.bin")));
        assertEquals("the archived dataset", read(mirror.resolve("archive.bin")),
                "the only copy of a decommissioned producer's file was overwritten (" + afterOverwrite
                        + ") and then deleted by another key holder");
    }

    // =================================================================== guards

    /**
     * MED. The delete guard's budget is per window (10% of the catalog within sync_delete_guard_window_ms, 15 min):
     * a slow failure or a deleter that stays just under it -- 9% a window -- is never held, and drains every mirror
     * down to the floor where the 3-file minimum bites. (Window shortened to 300 ms here; the arithmetic is the same.)
     */
    @Test
    void aSlowDripOfDeletesIsHeldBeforeHalfTheCatalogIsGone() throws Exception {
        TestRig p = producer("plugin-p", scan, "sync_delete_guard_window_ms", "300");
        TestRig s = subscriber("plugin-s", mirror);
        subscribe(p, s);
        int n = 200;
        for (int i = 0; i < n; i++) write(scan.resolve("f" + i + ".dat"), "f" + i);
        cycle(p, s);
        int left = n, next = 0, rounds = 0;
        while (left > n / 2 && p.repo.getDeletesHeld() == 0 && rounds < 30) {
            int drip = Math.max(3, (int) Math.floor(left * 0.09));
            for (int i = 0; i < drip; i++) Files.delete(scan.resolve("f" + (next++) + ".dat"));
            left -= drip;
            assertTrue(p.repo.scanOnce());
            Thread.sleep(350);
            rounds++;
        }
        long mirrorLeft = within(5000, () -> {
            try {
                return count(mirror, ".dat") <= n / 2;
            } catch (Exception e) {
                return false;
            }
        }) ? count(mirror, ".dat") : count(mirror, ".dat");
        assertTrue(p.repo.getDeletesHeld() > 0, "a 9%-per-window drip deleted " + (n - left) + " of " + n
                + " files over " + rounds + " windows without a hold; the mirror holds " + mirrorLeft);
    }

    /**
     * MED. The rewrite guard is per scan cycle only (at least 10 files AND more than 25% in ONE cycle) with no window:
     * ransomware that encrypts a few files per scan period never trips it, and every mirror copy is overwritten.
     */
    @Test
    void aSlowRewriteIsHeldBeforeEveryMirrorCopyIsOverwritten() throws Exception {
        TestRig p = producer("plugin-p", scan);
        TestRig s = subscriber("plugin-s", mirror);
        subscribe(p, s);
        int n = 40;
        for (int i = 0; i < n; i++) write(scan.resolve("doc" + i + ".txt"), "the real document " + i);
        cycle(p, s);
        for (int i = 0; i < n; i++) {
            Path f = write(scan.resolve("doc" + i + ".txt"), "ENCRYPTED-" + i);
            Files.setLastModifiedTime(f, FileTime.fromMillis(System.currentTimeMillis() + 10_000));
            if (i % 9 == 8 || i == n - 1) {
                // nine files per scan period
                assertTrue(p.repo.scanOnce());
                Thread.sleep(100);
            }
        }
        within(5000, () -> "ENCRYPTED-39".equals(read(mirror.resolve("doc39.txt"))));
        int encrypted = 0;
        for (int i = 0; i < n; i++) if (String.valueOf(read(mirror.resolve("doc" + i + ".txt"))).startsWith("ENCRYPTED")) encrypted++;
        assertTrue(encrypted < n / 2, encrypted + " of " + n + " mirror copies overwritten by a rewrite of 9 files per cycle"
                + " (rewrites held: " + p.repo.getRewritesHeld() + ")");
    }

    // =================================================================== the state directory and its lock

    /**
     * MED. "One plugin per directory" keys the state directory and lock by the configured path string, not the
     * directory: a second spelling of the same directory (a symlinked parent; /tmp vs /private/tmp on macOS) opens a
     * second catalog and lock. Then a mirror instance, whose own catalog shows nothing landed (MIRROR), clears the
     * directory without force -- deleting a file another instance landed there (ONLY_COPY).
     */
    @Test
    void aSecondSpellingOfTheSameDirectoryIsRefusedByTheLock() throws Exception {
        Path realParent = Files.createDirectories(tmp.resolve("data"));
        Path repo = Files.createDirectories(realParent.resolve("repo"));
        Path aliasParent = Files.createSymbolicLink(tmp.resolve("data-alias"), realParent);

        Map<String, Object> landing = new HashMap<>();
        landing.put("repo_dir", repo.toString());
        TestRig b = rig("plugin-b", landing);
        Map<String, Object> mirrorCfg = new HashMap<>();
        mirrorCfg.put("filerepo_name", "fr");
        mirrorCfg.put("repo_dir", aliasParent.resolve("repo").toString());
        TestRig a = rig("plugin-a", mirrorCfg);
        String lockRefusal = a.repo.catalogOpenError();

        Path src = write(tmp.resolve("in/only-copy.dat"), "landed here, nowhere else");
        MsgEvent put = b.requestTo("putfiles", "repo_name", "x");
        put.addFile(src.toString());
        put.setCompressedParam("file_hashes", new com.google.gson.Gson().toJson(Map.of("only-copy.dat",
                Hashes.content(Files.readAllBytes(src)))));
        assertEquals("10", b.call(TestRig.as(put, "admin")).getParam("status"), "precondition: landed");
        assertTrue(Files.exists(repo.resolve("only-copy.dat")));

        MsgEvent clear = a.call(TestRig.as(a.requestTo("clearrepo", "repo_name", "fr", "confirm",
                aliasParent.resolve("repo").toString()), "admin"));
        assertTrue(Files.exists(repo.resolve("only-copy.dat")),
                "a second instance on the same directory (lock: " + (lockRefusal == null ? "not refused" : lockRefusal)
                        + ") cleared it without force (" + clear.getParam("status") + " " + clear.getParam("state")
                        + ") and deleted another instance's ONLY_COPY file");
    }

    // =================================================================== the macOS path fallback

    /**
     * MED (macOS, Windows). The fallback proves an open by hashing the whole file through its descriptor -- before
     * getfile's max_inline_bytes check and before streamfile's scheduler admits anything. So getfile, an open verb, of a
     * large cataloged file makes the agent read all of it just to answer "too large" (and every streamfile range request,
     * a sync's gap re-asks included, re-reads the whole file). A sparse 4 GB file here: no disk is used.
     */
    @Test
    void getfileOfAFileTooLargeToInlineAnswersWithoutReadingIt() throws Exception {
        try (var ds = Files.newDirectoryStream(tmp)) {
            org.junit.jupiter.api.Assumptions.assumeTrue(!(ds instanceof java.nio.file.SecureDirectoryStream), "the descriptor path is used here");
        }
        Path repo = Files.createDirectories(tmp.resolve("repo"));
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("repo_dir", repo.toString());
        TestRig r = rig(TestRig.PLUGIN_ID, cfg);
        Path big = repo.resolve("big.img");
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(big.toFile(), "rw")) {
            raf.setLength(4L << 30);
        }
        r.db.addFile(big.toString(), "0".repeat(64), Files.getLastModifiedTime(big).toMillis(), Files.size(big));
        long t0 = System.nanoTime();
        MsgEvent m = r.call("getfile", "file_path", big.toString());
        long ms = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(ms < 500, "getfile of a 4 GB file took " + ms + " ms (status " + m.getParam("status") + " "
                + m.getParam("status_desc") + "): the whole file was read before the size check");
    }

    // =================================================================== the persisted generation

    /**
     * MED. The mirror's generation now survives a restart (good), but it lives in the catalog under the agent's data
     * directory, not with the mirror's files: a mirror whose directory comes back empty (a replaced or reformatted
     * volume, an admin's rm -rf instead of clearrepo) keeps its generation, its producer has everything settled for it,
     * and nothing is ever offered again. The mirror is silently empty forever.
     */
    @Test
    void aMirrorWhoseDirectoryCameBackEmptyIsRefilled() throws Exception {
        TestRig p = producer("plugin-p", scan);
        TestRig s = subscriber("plugin-s", mirror);
        subscribe(p, s);
        for (int i = 0; i < 5; i++) write(scan.resolve("m" + i + ".bin"), "model " + i);
        cycle(p, s);
        assertEquals(5, count(mirror, ".bin"));

        // the mirror's volume is replaced while its agent is down: the directory comes back empty
        s.close();
        rigs.remove(s);
        try (var st = Files.list(mirror)) {
            for (Path f : (Iterable<Path>) st::iterator) if (Files.isRegularFile(f)) Files.delete(f);
        }
        TestRig again = subscriber("plugin-s", mirror);
        subscribe(p, again);
        for (int i = 0; i < 3; i++) {
            assertTrue(p.repo.scanOnce());
            Thread.sleep(300);
        }
        assertTrue(within(3000, () -> {
            try {
                return count(mirror, ".bin") == 5;
            } catch (Exception e) {
                return false;
            }
        }), "the emptied mirror is never refilled: " + count(mirror, ".bin") + " of 5 files; pending at the producer "
                + p.db.pendingCount(subKey(again)));
    }
}
