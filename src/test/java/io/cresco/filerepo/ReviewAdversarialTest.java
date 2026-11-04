package io.cresco.filerepo;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.library.messaging.MsgEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import jakarta.jms.TextMessage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Adversarial review (review/plugins-20261104). Every test states the behaviour a production-grade filerepo must have;
 * each one FAILS on hardening f6cd170. Local only: never pushed.
 */
class ReviewAdversarialTest {

    @TempDir Path tmp;
    final Fabric fabric = new Fabric();
    final Gson gson = new Gson();
    final List<TestRig> rigs = new ArrayList<>();
    TestRig producer, subscriber;
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

    // ------------------------------------------------------------------ rigs

    TestRig rig(String pluginId, Map<String, Object> cfg, boolean onFabric) throws Exception {
        TestRig r = new TestRig(tmp.resolve("agent"), cfg, null, pluginId, onFabric ? fabric : null);
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
        TestRig r = rig(pluginId, cfg, true);
        r.repo.start();
        return r;
    }

    TestRig subscriber(String pluginId, Path dir, String... kv) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("filerepo_name", "fr");
        cfg.put("repo_dir", dir.toString());
        cfg.put("sync_stream_buffer", "4096");
        for (int i = 0; i + 1 < kv.length; i += 2) cfg.put(kv[i], kv[i + 1]);
        TestRig r = rig(pluginId, cfg, true);
        r.repo.start();
        return r;
    }

    void subscribeAll(TestRig p, TestRig... subs) throws Exception {
        p.repo.repoBroadcast("fr", "discover", 0);
        for (TestRig s : subs) {
            await(() -> p.repo.isSubscriber(TestRig.REGION, TestRig.AGENT, s.pluginId), s.pluginId + " subscribed");
        }
    }

    static void await(BooleanSupplier cond, String what) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) fail("timed out waiting: " + what);
            Thread.sleep(20);
        }
    }

    /** True if cond becomes true within ms. */
    static boolean within(long ms, BooleanSupplier cond) throws Exception {
        long deadline = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return true;
            Thread.sleep(25);
        }
        return cond.getAsBoolean();
    }

    static boolean confirmed(TestRig p) {
        return p.db.getFilesNotInSync().isEmpty();
    }

    void syncedAndConfirmed(TestRig p) throws Exception {
        assertTrue(p.repo.scanOnce());
        await(() -> confirmed(p), "every offered file confirmed");
    }

    static Path write(Path f, String s) throws Exception {
        Files.createDirectories(f.getParent());
        return Files.write(f, s.getBytes());
    }

    static void bump(Path f, long ms) throws Exception {
        Files.setLastModifiedTime(f, FileTime.fromMillis(System.currentTimeMillis() + ms));
    }

    static String sha384(byte[] b) throws Exception {
        return Hashes.hex(MessageDigest.getInstance("SHA-384").digest(b));
    }

    // =================================================================== DATA LOSS

    /**
     * HIGH. releasedeletes arms a COUNT (releaseUpTo) that is only consumed by a mass-delete cycle: a non-mass cycle
     * (here the operator restores part of what was held) tombstones and leaves the release armed. Any later mass event of
     * that size or smaller -- an unmount, weeks later -- is then released with no admin action and every mirror deletes.
     */
    @Test
    void aReleaseIsSpentOnceAndNeverReleasesALaterUnmount() throws Exception {
        producer = producer("plugin-p", scan, "sync_delete_guard_min", "5", "sync_delete_guard_fraction", "0.5");
        subscriber = subscriber("plugin-s", mirror);
        subscribeAll(producer, subscriber);
        for (int i = 0; i < 12; i++) write(scan.resolve("g" + i + ".dat"), "g" + i);
        syncedAndConfirmed(producer);

        for (int i = 0; i < 10; i++) Files.delete(scan.resolve("g" + i + ".dat"));
        assertTrue(producer.repo.scanOnce());
        assertEquals(10, producer.repo.getDeletesHeld(), "precondition: ten of twelve held");
        MsgEvent rel = producer.call(producer.requestTo("releasedeletes", "confirm", scan.toString()));
        assertEquals("10", rel.getParam("released"));

        // the operator restores half of what was held before the next cycle: a non-mass cycle
        for (int i = 0; i < 5; i++) write(scan.resolve("g" + i + ".dat"), "g" + i);
        assertTrue(producer.repo.scanOnce());
        assertEquals(5, producer.repo.getTombstoneCount(), "precondition: the five still gone were recorded");
        await(() -> producer.db.getTombstonesNotInSync(100).isEmpty(), "applied");

        // much later the volume is unmounted: the scan dir is empty
        try (var s = Files.list(scan)) {
            for (Path p : (Iterable<Path>) s::iterator) Files.delete(p);
        }
        assertTrue(producer.repo.scanOnce());
        boolean deleted = within(5000, () -> !Files.exists(mirror.resolve("g11.dat")));
        assertEquals(7, producer.repo.getDeletesHeld(),
                "an empty scan dir is held: the earlier release was already spent, nobody released this one"
                        + " (mirror copy deleted: " + deleted + ")");
        assertFalse(deleted, "the mirror keeps its copy while the delete is held");
    }

    /**
     * HIGH. The guard is (gone >= sync_delete_guard_min AND gone > fraction) OR the scan dir is entirely empty. A nested
     * volume (a mount point under scan_dir) holding fewer than 100 files that goes away -- an unmount leaves its mount
     * point as an empty directory -- is not "mass": every one of those deletes is offered and every mirror deletes them.
     */
    @Test
    void anUnmountedNestedVolumeIsHeldNotPropagated() throws Exception {
        producer = producer("plugin-p", scan);
        subscriber = subscriber("plugin-s", mirror);
        subscribeAll(producer, subscriber);
        Path vol = Files.createDirectories(scan.resolve("vol"));
        for (int i = 0; i < 99; i++) write(vol.resolve("ckpt-" + i + ".bin"), "checkpoint " + i);
        write(scan.resolve("index.txt"), "index");
        syncedAndConfirmed(producer);
        assertTrue(Files.exists(mirror.resolve("vol/ckpt-42.bin")));

        // the nested volume is unmounted: its mount point is an empty directory again (99 of 100 files gone)
        try (var s = Files.list(vol)) {
            for (Path p : (Iterable<Path>) s::iterator) Files.delete(p);
        }
        assertTrue(producer.repo.scanOnce());
        boolean deleted = within(5000, () -> !Files.exists(mirror.resolve("vol/ckpt-42.bin")));
        assertFalse(deleted,
                "99% of the catalog vanishing at once must be held for an admin, not deleted from every mirror (held="
                        + producer.repo.getDeletesHeld() + ", tombstones=" + producer.repo.getTombstoneCount() + ")");
    }

    /**
     * HIGH (authorization -> data loss). A subscriber subscribes to whatever announces itself on the dataplane as a
     * producer of its filerepo_name (the discover body's repo_*_id fields), and then accepts that plugin's diffs. Any
     * plugin of the tenant -- here plugin-x, which is not a producer of "fr" -- can therefore delete a mirror's files
     * (the content hash it needs is served by every repo's open getrepofilelist).
     */
    @Test
    void anyPluginThatAnnouncesItselfCanDeleteAMirrorsFiles() throws Exception {
        producer = producer("plugin-p", scan);
        subscriber = subscriber("plugin-s", mirror);
        subscribeAll(producer, subscriber);
        write(scan.resolve("model.bin"), "the weights");
        syncedAndConfirmed(producer);
        assertTrue(Files.exists(mirror.resolve("model.bin")));

        Map<String, Object> cfg = new HashMap<>();
        cfg.put("repo_dir", Files.createDirectories(tmp.resolve("rogue")).toString());
        TestRig rogue = rig("plugin-x", cfg, true);

        // 1. the content hash, from the real producer's getrepofilelist (no role needed)
        MsgEvent list = rogue.plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, TestRig.REGION, TestRig.AGENT, "plugin-p");
        list.setParam("action", "getrepofilelist");
        list.setParam("repo_name", "fr");
        MsgEvent listed = rogue.plugin.sendRPC(list, 5000);
        List<Map<String, String>> rows = gson.fromJson(listed.getCompressedParam("repofilelist"),
                new TypeToken<List<Map<String, String>>>() {}.getType());
        String md5 = rows.get(0).get("md5");

        // 2. it announces itself as a producer of "fr" (any dataplane sender can)
        rogue.repo.repoBroadcast("fr", "discover", 0);
        Thread.sleep(200);

        // 3. a diff that deletes the mirror's copy
        MsgEvent diff = rogue.plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, TestRig.REGION, TestRig.AGENT, "plugin-s");
        diff.setParam("action", "repolistin");
        diff.setParam("transfer_id", "1");
        diff.setCompressedParam("repolistin", "{}");
        diff.setCompressedParam("repodeletes", gson.toJson(List.of(
                new DeleteObject("model.bin", md5, "/anything/model.bin", System.currentTimeMillis()))));
        MsgEvent reply = rogue.plugin.sendRPC(diff, 5000);
        boolean deleted = within(5000, () -> !Files.exists(mirror.resolve("model.bin")));
        assertFalse(deleted,
                "a plugin that is not the mirror's producer deleted its file (repolistin answered "
                        + (reply == null ? null : reply.getParam("status_code")) + ")");
    }

    /**
     * MED. Two producers of one filerepo_name (the old protocol subscribes a mirror to every discover of that name).
     * The second deletes a file both hold; the mirror deletes its copy, and the first producer -- which still holds
     * it and has it marked in sync -- never offers it again.
     */
    @Test
    void aDeleteFromOneProducerNeverRemovesAFileAnotherProducerStillHolds() throws Exception {
        Path scan2 = Files.createDirectories(tmp.resolve("scan2"));
        producer = producer("plugin-p", scan);
        TestRig producer2 = producer("plugin-p2", scan2);
        subscriber = subscriber("plugin-s", mirror, "sync_offer_timeout_ms", "1000");
        subscribeAll(producer, subscriber);
        subscribeAll(producer2, subscriber);
        write(scan.resolve("shared.txt"), "same bytes");
        write(scan2.resolve("shared.txt"), "same bytes");
        write(scan2.resolve("other.txt"), "keeps the second scan dir non-empty");
        syncedAndConfirmed(producer);
        syncedAndConfirmed(producer2);

        Files.delete(scan2.resolve("shared.txt"));
        assertTrue(producer2.repo.scanOnce());
        assertEquals(1, producer2.repo.getTombstoneCount(), "precondition: an ordinary delete, recorded");
        await(() -> producer2.db.getTombstonesNotInSync(10).isEmpty(), "the second producer's delete settled");
        for (int i = 0; i < 3; i++) {
            assertTrue(producer.repo.scanOnce());
            Thread.sleep(300);
        }
        assertTrue(Files.exists(mirror.resolve("shared.txt")),
                "plugin-p still holds shared.txt and believes the mirror does too");
    }

    /**
     * MED. A file a subscriber landed directly (putfiles: repostate ONLY_COPY, "its producer does not hold them") is
     * silently overwritten by sync when the producer has a file of the same name.
     */
    @Test
    void syncNeverOverwritesAFileLandedInTheMirror() throws Exception {
        producer = producer("plugin-p", scan);
        subscriber = subscriber("plugin-s", mirror);
        Path src = write(tmp.resolve("incoming/notes.txt"), "the only copy of the field notes");
        MsgEvent put = subscriber.requestTo("putfiles", "repo_name", "fr");
        put.addFile(src.toString());
        put.setCompressedParam("file_hashes", gson.toJson(Map.of("notes.txt", Hashes.content(Files.readAllBytes(src)))));
        assertEquals("10", subscriber.call(TestRig.as(put, "admin")).getParam("status"));
        assertEquals(RepoState.ONLY_COPY, subscriber.call("repostate").getParam("state"));

        subscribeAll(producer, subscriber);
        write(scan.resolve("notes.txt"), "the producer's notes");
        assertTrue(producer.repo.scanOnce());
        boolean replaced = within(5000, () -> {
            try {
                return !"the only copy of the field notes".equals(Files.readString(mirror.resolve("notes.txt")));
            } catch (Exception e) {
                return true;
            }
        });
        assertFalse(replaced, "the landed only copy was replaced by sync");
    }

    /**
     * MED (case-insensitive mirror, e.g. macOS or Windows, of a case-sensitive Linux producer). The producer holds
     * Model.bin and model.bin with the same bytes. The mirror can keep one file for both; when the producer deletes
     * Model.bin the mirror deletes that one file, and the producer still marks model.bin as held there.
     */
    @Test
    void aCaseOnlyNameCollisionNeverDeletesAFileTheProducerStillOffers() throws Exception {
        assumeTrue(caseInsensitive(tmp), "needs a case-insensitive filesystem");
        subscriber = subscriber("plugin-s", mirror);
        subscriber.repo.rememberProducer(TestRig.REGION, TestRig.AGENT, "plugin-p");
        byte[] w = "weights".getBytes();
        String h = Hashes.content(w);
        Files.write(mirror.resolve("Model.bin"), w);   // synced earlier from the producer's Model.bin

        // the producer's model.bin (same bytes): the mirror answers "already held"
        Map<String, FileObject> files = new LinkedHashMap<>();
        files.put("/p/model.bin", new FileObject("model.bin", h, "/p/model.bin", 1L, w.length));
        assertEquals("10", fromProducer("1", gson.toJson(files), null));
        // then the producer deletes Model.bin (and keeps model.bin)
        assertEquals("10", fromProducer("2", "{}", gson.toJson(List.of(new DeleteObject("Model.bin", h, "/p/Model.bin", 1L)))));
        await(() -> subscriber.repo.getSyncQueued() == 0 && subscriber.repo.getSyncActive() == 0, "applied");
        Thread.sleep(200);
        assertTrue(Files.exists(mirror.resolve("model.bin")), "model.bin, which the producer still offers, is gone");
    }

    private String fromProducer(String tid, String files, String deletes) {
        MsgEvent in = subscriber.requestTo("repolistin", "transfer_id", tid);
        in.setParam(Callers.PARAM_CALLER, "plugin:" + Fabric.TENANT + "/" + TestRig.REGION + "/" + TestRig.AGENT + "/plugin-p");
        in.setParam("cresco_caller_tenant", Fabric.TENANT);
        in.setParam(Callers.PARAM_ROLES, "plugin");
        in.setCompressedParam("repolistin", files);
        if (deletes != null) in.setCompressedParam("repodeletes", deletes);
        return subscriber.call(in).getParam("status_code");
    }

    static boolean caseInsensitive(Path dir) throws Exception {
        Path probe = Files.write(dir.resolve("CaseProbe.tmp"), new byte[0]);
        try {
            return Files.exists(dir.resolve("caseprobe.tmp"));
        } finally {
            Files.deleteIfExists(probe);
        }
    }

    /**
     * MED (destructive primitive). putfiles decides copy-vs-move from the message's own src/dst fields, moves the named
     * source into staging, and deletes the staging file when the hash does not match: a file outside the repository
     * named in the file list is destroyed, even though nothing lands.
     */
    @Test
    void aRefusedPutfilesNeverDestroysItsSource() throws Exception {
        Path repo = Files.createDirectories(tmp.resolve("repo"));
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("repo_dir", repo.toString());
        TestRig r = rig(TestRig.PLUGIN_ID, cfg, false);
        Path victim = write(tmp.resolve("elsewhere/precious.db"), "not the repository's");
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, "region-2", "agent-2", "caller",
                TestRig.REGION, TestRig.AGENT, TestRig.PLUGIN_ID, false, false);
        m.setParam("action", "putfiles");
        m.setParam("repo_name", "any");
        m.addFile(victim.toString());
        m.setCompressedParam("file_hashes", gson.toJson(Map.of("precious.db", "0000")));
        MsgEvent reply = r.call(TestRig.as(m, "admin"));
        assertNotEquals("10", reply.getParam("status"));
        assertTrue(Files.exists(victim), "a putfiles that landed nothing deleted " + victim);
    }

    // =================================================================== AUTHORIZATION / CONTAINMENT

    /**
     * HIGH. putfilesremote reads ANY path on this agent the caller names (only live-tier roots are refused) and ships
     * it to any plugin the caller names: no containment to the repository and no file_access_roots (contract section
     * 4: remote file reads are denied unless file_access_roots is set). A same-tenant admin can take the agent's keys.
     */
    @Test
    void putfilesremoteSendsOnlyFilesOfThisRepository() throws Exception {
        Path repo = Files.createDirectories(tmp.resolve("repo"));
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("repo_dir", repo.toString());
        TestRig r = rig(TestRig.PLUGIN_ID, cfg, false);
        Path key = write(tmp.resolve("agent/cresco-data/agent-keystore.p12"), "PRIVATE KEY MATERIAL");
        MsgEvent m = r.requestTo("putfilesremote", "dst_region", "region-9", "dst_agent", "attacker", "dst_plugin", "plugin-0",
                "repo_name", "loot");
        m.setCompressedParam("file_list", gson.toJson(List.of(key.toString())));
        MsgEvent reply = r.call(TestRig.as(m, "admin"));
        boolean shipped = r.sent.stream().anyMatch(s -> "putfiles".equals(s.getParam("action"))
                && s.getFileList() != null && s.getFileList().contains(key.toString()));
        assertFalse(shipped, "putfilesremote shipped " + key + " to region-9/attacker (status " + reply.getParam("status") + ")");
    }

    /**
     * MED. putjar is the only door that checks signatures, but putfiles and sync land into the same directory
     * (FR-D9), and repolist/getjar advertise and serve whatever jar is there: an unsigned jar landed by putfiles is
     * served as a plugin.
     */
    @Test
    void getjarNeverServesAJarThatWasNotSignatureChecked() throws Exception {
        Path repo = Files.createDirectories(tmp.resolve("repo"));
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("repo_dir", repo.toString());
        TestRig r = rig(TestRig.PLUGIN_ID, cfg, false);
        byte[] jar = JarSigning.unsignedJar("io.cresco.sysinfo", "not signed by anyone");
        Path src = Files.write(Files.createDirectories(tmp.resolve("in")).resolve("sysinfo.jar"), jar);
        MsgEvent put = r.requestTo("putfiles", "repo_name", "any");
        put.addFile(src.toString());
        put.setCompressedParam("file_hashes", gson.toJson(Map.of("sysinfo.jar", Hashes.content(jar))));
        assertEquals("10", r.call(TestRig.as(put, "admin")).getParam("status"));

        MsgEvent got = r.call("getjar", "action_pluginname", "io.cresco.sysinfo", "action_pluginmd5", Hashes.content(jar));
        assertNotEquals("10", got.getParam("status"), "an unsigned jar was served as plugin io.cresco.sysinfo");
    }

    /**
     * LOW-MED (race, simulated with the verifier hook, as the author's tests simulate races with afterClearCheck). The
     * staged jar sits in the shared repository directory and is re-read by path by verify, getMD5, symbolicName and the
     * move: bytes that replace it after verification are what lands, under the SHA-384 of the verified bytes.
     */
    @Test
    void putjarLandsExactlyTheBytesItVerified() throws Exception {
        Path repo = Files.createDirectories(tmp.resolve("repo"));
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("repo_dir", repo.toString());
        TestRig r = rig(TestRig.PLUGIN_ID, cfg, false);
        byte[] good = JarSigning.signedJar("io.cresco.example", "v1");
        byte[] evil = JarSigning.unsignedJar("io.cresco.example", "evil");
        r.exec.setJarVerifier(p -> {
            String h = r.plugin.verifyJarSignature(p);
            // a writer of the repository directory renames its own file over the staged name
            Path other = Files.write(p.resolveSibling("swap.tmp"), evil);
            Files.move(other, p, StandardCopyOption.REPLACE_EXISTING);
            return h;
        });
        MsgEvent m = TestRig.request("putjar", "pluginname", "io.cresco.example", "jarfile", "example.jar");
        m.setDataParam("jardata", good);
        MsgEvent reply = r.call(m);
        if ("10".equals(reply.getParam("status"))) {
            assertEquals(reply.getParam("sha384"), sha384(Files.readAllBytes(repo.resolve("example.jar"))),
                    "the jar in place is not the jar whose SHA-384 putjar reported");
        }
    }

    // =================================================================== SYNC CORRECTNESS (silent gaps)

    /**
     * HIGH. One insync flag per file for N subscribers: when one subscriber fails a file (or answers busy) and another
     * confirms it, the file is never offered again, and the first mirror silently never gets it.
     */
    @Test
    void aFileOneSubscriberFailedIsOfferedAgainUntilItHasIt() throws Exception {
        Path mirror2 = Files.createDirectories(tmp.resolve("mirror2"));
        producer = producer("plugin-p", scan, "sync_offer_timeout_ms", "1000");
        subscriber = subscriber("plugin-s", mirror);
        TestRig s2 = subscriber("plugin-s2", mirror2);
        subscribeAll(producer, subscriber, s2);
        assertTrue(producer.repo.scanOnce());   // both full offers complete (nothing confirmed yet)

        // s2 cannot take late.txt this cycle (a directory is where it goes)
        Files.createDirectories(mirror2.resolve("late.txt/blocker"));
        write(scan.resolve("late.txt"), "late");
        assertTrue(producer.repo.scanOnce());
        await(() -> confirmed(producer), "plugin-s confirmed late.txt");
        await(() -> s2.sent.stream().anyMatch(m -> "repoconfirm".equals(m.getParam("action"))
                && m.getCompressedParam("failedfiles") != null), "plugin-s2 reported late.txt failed");

        // the obstruction is cleared; the producer keeps scanning
        Files.delete(mirror2.resolve("late.txt/blocker"));
        Files.delete(mirror2.resolve("late.txt"));
        boolean arrived = false;
        for (int i = 0; i < 6 && !arrived; i++) {
            Thread.sleep(400);
            assertTrue(producer.repo.scanOnce());
            arrived = within(400, () -> Files.isRegularFile(mirror2.resolve("late.txt")));
        }
        assertTrue(arrived, "plugin-s2 never receives late.txt: plugin-s's confirm marked it in sync for everyone");
    }

    /**
     * MED. A full offer pages the insync=1 rows by OFFSET over a set that changes under it: a row that leaves the set
     * (a changed file, insync=0) shifts the rest left and the late subscriber never gets the row that slid past.
     */
    @Test
    void aLateSubscribersFullOfferSkipsNothingWhileTheCatalogChanges() throws Exception {
        producer = producer("plugin-p", scan, "sync_diff_page", "2", "sync_full_pages_per_cycle", "1",
                "sync_offer_timeout_ms", "1000");
        subscriber = subscriber("plugin-s", mirror);
        subscribeAll(producer, subscriber);
        for (int i = 0; i < 6; i++) write(scan.resolve("f" + i + ".txt"), "file " + i);
        syncedAndConfirmed(producer);

        Path mirror2 = Files.createDirectories(tmp.resolve("late"));
        TestRig late = subscriber("plugin-late", mirror2);
        subscribeAll(producer, late);
        assertTrue(producer.repo.scanOnce());   // full page 1: f0, f1
        await(() -> Files.exists(mirror2.resolve("f1.txt")), "first full page");

        // f0 changes; this cycle's confirms are lost, so f0 is out of the synced set while page 2 is read
        write(scan.resolve("f0.txt"), "file 0, edited");
        bump(scan.resolve("f0.txt"), 5000);
        fabric.dropRoute = m -> "repoconfirm".equals(m.getParam("action"));
        assertTrue(producer.repo.scanOnce());
        Thread.sleep(800);
        fabric.dropRoute = m -> false;
        for (int i = 0; i < 6; i++) {
            Thread.sleep(400);
            assertTrue(producer.repo.scanOnce());
        }
        for (int i = 0; i < 6; i++) {
            Path f = mirror2.resolve("f" + i + ".txt");
            assertTrue(within(2000, () -> Files.exists(f)), "the late subscriber never received f" + i + ".txt");
        }
    }

    /**
     * HIGH (single subscriber). Transfer ids restart at 1 when the producer restarts, and a confirm is matched by id
     * alone: a subscriber's confirm of the pre-restart transfer 1 settles the post-restart transfer 1, marking in sync a
     * file the subscriber never fetched. When that fetch then fails, nothing ever offers the file again.
     */
    @Test
    void aConfirmFromBeforeAProducerRestartNeverMarksAnUnfetchedFileInSync() throws Exception {
        producer = producer("plugin-p", scan, "sync_offer_timeout_ms", "1000");
        subscriber = subscriber("plugin-s", mirror);
        subscribeAll(producer, subscriber);
        assertTrue(producer.repo.scanOnce());
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.atomic.AtomicInteger diffs = new java.util.concurrent.atomic.AtomicInteger();
        subscriber.repo.beforeApplyDiff = () -> {
            if (diffs.incrementAndGet() == 1) {
                try {
                    release.await(20, java.util.concurrent.TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                }
            }
        };
        write(scan.resolve("a.txt"), "a");
        assertTrue(producer.repo.scanOnce());                     // transfer 1 = {a}; the mirror is slow on it
        await(() -> diffs.get() == 1, "the mirror working on transfer 1");

        // the producer restarts (same plugin id, same catalog)
        producer.close();
        rigs.remove(producer);
        producer = producer("plugin-p", scan, "sync_offer_timeout_ms", "1000");
        subscribeAll(producer, subscriber);
        Files.createDirectories(mirror.resolve("b.txt/blocker"));   // b cannot land at the mirror this time
        write(scan.resolve("b.txt"), "b");
        assertTrue(producer.repo.scanOnce());                     // transfer 1 again = {a, b}
        release.countDown();
        await(() -> subscriber.sent.stream().filter(m -> "repoconfirm".equals(m.getParam("action"))).count() >= 2,
                "both transfers settled at the mirror");

        Files.delete(mirror.resolve("b.txt/blocker"));
        Files.delete(mirror.resolve("b.txt"));
        boolean arrived = false;
        for (int i = 0; i < 6 && !arrived; i++) {
            Thread.sleep(400);
            assertTrue(producer.repo.scanOnce());
            arrived = within(400, () -> Files.isRegularFile(mirror.resolve("b.txt")));
        }
        boolean bUnsynced = producer.db.getFilesNotInSync().stream()
                .anyMatch(r -> r.get("filepath").endsWith("b.txt"));
        assertTrue(arrived, "b.txt never reaches the mirror: the pre-restart confirm of transfer 1 marked it in sync"
                + " (b.txt insync=0 at the producer: " + bUnsynced + ")");
    }

    // =================================================================== RESOURCE EXHAUSTION

    /** MED. Subscribes are dataplane bodies anyone can send; 256 ghosts fill sync_max_peers and the real mirror is never added. */
    @Test
    void ghostSubscribersCannotLockARealMirrorOut() throws Exception {
        producer = producer("plugin-p", scan);
        for (int i = 0; i < 256; i++) {
            TextMessage t = FakeJms.text();
            Map<String, String> body = new HashMap<>();
            body.put("action", "subscribe");
            body.put("filerepo_name", "fr");
            body.put("sub_region_id", "ghost-region");
            body.put("sub_agent_id", "ghost-agent");
            body.put("sub_plugin_id", "ghost-" + i);
            t.setText(gson.toJson(body));
            t.setStringProperty("filerepo_name", "fr");
            t.setStringProperty("region_id", TestRig.REGION);
            t.setStringProperty("agent_id", TestRig.AGENT);
            t.setStringProperty("plugin_id", "plugin-p");
            fabric.publish(t);
        }
        subscriber = subscriber("plugin-s", mirror);
        producer.repo.repoBroadcast("fr", "discover", 0);
        assertTrue(within(2000, () -> producer.repo.isSubscriber(TestRig.REGION, TestRig.AGENT, "plugin-s")),
                "the real mirror was refused: 256 ghost subscribers hold every slot");
    }

    /** MED. knownProducers is bounded but never pruned: 256 discovers (or 256 redeploys) and no producer is ever subscribed again. */
    @Test
    void ghostProducersCannotStopAMirrorSubscribing() throws Exception {
        subscriber = subscriber("plugin-s", mirror);
        for (int i = 0; i < 256; i++) {
            Map<String, String> body = new HashMap<>();
            body.put("action", "discover");
            body.put("filerepo_name", "fr");
            body.put("repo_region_id", "ghost-region");
            body.put("repo_agent_id", "ghost-agent");
            body.put("repo_plugin_id", "ghost-" + i);
            subscriber.repo.onSubscriberMessage(body);
        }
        producer = producer("plugin-p", scan);
        producer.repo.repoBroadcast("fr", "discover", 0);
        assertTrue(within(2000, () -> producer.repo.isSubscriber(TestRig.REGION, TestRig.AGENT, "plugin-s")),
                "the mirror never subscribes to its real producer: knownProducers is full of ghosts");
    }

    /**
     * MED-HIGH (regression from FR-D9). repolist and getjar take no role; on a producer the repository directory is now
     * the scan dir, so one repolist hashes every top-level data file there (the old code inventoried repo_dir only).
     */
    @Test
    void repolistOnAProducerHashesOnlyJars() throws Exception {
        producer = producer("plugin-p", scan);
        byte[] blob = new byte[1 << 16];
        for (int i = 0; i < 40; i++) Files.write(scan.resolve("sample-" + i + ".dcm"), blob);
        MsgEvent r = producer.call("repolist");
        assertNotNull(r);
        assertEquals(0, producer.exec.jarInventory().hashed,
                "an open repolist hashed " + producer.exec.jarInventory().hashed + " data files of the scan dir");
    }

    /**
     * MED. clearrepo's walk throws on an unreadable subdirectory (Files.walk's UncheckedIOException): the files it
     * deleted before that are gone, the reply is a failure, and no new sync generation is started -- the producer, whose
     * rows are in sync, never refills them. A failed clear must either delete nothing or start a new generation.
     */
    @Test
    void aClearThatFailsPartwayStillRefillsWhatItDeleted() throws Exception {
        producer = producer("plugin-p", scan);
        subscriber = subscriber("plugin-s", mirror);
        subscribeAll(producer, subscriber);
        for (int i = 0; i < 30; i++) write(scan.resolve("c" + i + ".txt"), "c" + i);
        syncedAndConfirmed(producer);
        Path locked = Files.createDirectories(mirror.resolve("m-locked"));
        write(locked.resolve("x.txt"), "x");
        String gen = subscriber.repo.getSyncGen();
        MsgEvent c;
        try {
            assertTrue(locked.toFile().setReadable(false) && locked.toFile().setExecutable(false));
            c = subscriber.call(TestRig.as(subscriber.requestTo("clearrepo", "repo_name", "fr", "confirm", mirror.toString()), "admin"));
        } finally {
            locked.toFile().setReadable(true);
            locked.toFile().setExecutable(true);
        }
        long left = 0;
        for (int i = 0; i < 30; i++) if (Files.exists(mirror.resolve("c" + i + ".txt"))) left++;
        assumeTrue(!"10".equals(c.getParam("status")) && left < 30,
                "this filesystem listed the locked directory first: nothing was deleted before the failure");
        assertNotEquals(gen, subscriber.repo.getSyncGen(), "clearrepo failed (" + c.getParam("status_desc") + ") after deleting "
                + (30 - left) + " synced file(s), and started no new generation: they are never refilled");
    }

    /**
     * MED. transfer_max_per_caller (16) is larger than transfer_threads (8): one caller -- any user of the tenant, since
     * streamfile takes no role -- holds every transfer thread, and every subscriber's sync fetch queues behind it until
     * its receiver times out (sync_stream_idle_ms) and fails.
     */
    @Test
    void oneCallerCannotHoldEveryTransferThread() throws Exception {
        Path repo = Files.createDirectories(tmp.resolve("repo"));
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("repo_dir", repo.toString());
        TestRig r = rig(TestRig.PLUGIN_ID, cfg, false);
        byte[] big = new byte[1 << 20];
        Path f = Files.write(repo.resolve("big.bin"), big);
        r.db.addFile(f.toString(), Hashes.content(big), Files.getLastModifiedTime(f).toMillis(), big.length);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.atomic.AtomicInteger started = new java.util.concurrent.atomic.AtomicInteger();
        r.exec.beforeStream = () -> {
            started.incrementAndGet();
            try {
                release.await(20, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
        };
        try {
            for (int i = 0; i < 16; i++) {
                MsgEvent m = streamReq(r, f, "api:tenant-a/mallory", "user");
                assertEquals("10", r.call(m).getParam("status"), "mallory's transfer " + i);
            }
            await(() -> started.get() == 8, "mallory's transfers running");
            MsgEvent sync = streamReq(r, f, "plugin:tenant-a/region-1/agent-1/plugin-s", "plugin");
            assertEquals("10", r.call(sync).getParam("status"), "the mirror's fetch is accepted");
            assertTrue(within(2000, () -> started.get() >= 9),
                    "the mirror's fetch never starts: one user holds all 8 transfer threads");
        } finally {
            release.countDown();
        }
    }

    private static MsgEvent streamReq(TestRig r, Path f, String caller, String roles) {
        MsgEvent m = r.requestTo("streamfile", "file_path", f.toString(), "start_byte", "0",
                "byte_length", String.valueOf(1 << 20), "ident_key", "stream_name", "ident_id", "nobody-listens");
        m.setParam(Callers.PARAM_CALLER, caller);
        m.setParam("cresco_caller_tenant", "tenant-a");
        m.setParam(Callers.PARAM_ROLES, roles);
        return m;
    }

    // =================================================================== SCANNER REGRESSIONS

    /** MED (regression: the old scanner listed through it). A scan_dir that is itself a symlink scans nothing. */
    @Test
    void aSymlinkedScanDirIsScanned() throws Exception {
        Path real = Files.createDirectories(tmp.resolve("realdata"));
        for (int i = 0; i < 3; i++) write(real.resolve("r" + i + ".txt"), "r" + i);
        Path link = Files.createSymbolicLink(tmp.resolve("data-link"), real);
        producer = producer("plugin-p", link);
        assertTrue(producer.repo.scanOnce());
        assertEquals(3, producer.repo.getRepoCount(), "scan_dir " + link + " -> " + real + " cataloged nothing");
    }

    /** LOW. PluginBuilder.getMD5 treats a path containing '!' as a classpath resource: such a file is never cataloged or synced. */
    @Test
    void aFileWithAnExclamationMarkIsCataloged() throws Exception {
        producer = producer("plugin-p", scan);
        write(scan.resolve("plain.txt"), "plain");
        write(scan.resolve("urgent!.txt"), "urgent");
        assertTrue(producer.repo.scanOnce());
        assertEquals(2, producer.repo.getRepoCount(), "urgent!.txt was silently skipped");
    }
}
