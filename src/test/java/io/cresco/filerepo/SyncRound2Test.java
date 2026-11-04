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
 * The round-2 design, beyond the review's own tests: the sync_key binding (and pinned producers without one), the
 * repository's state outside the plugin's data directory (one plugin per directory, the old catalog and default
 * repository moved, never deleted), claims of a silent producer orphaned and taken back, the rewrite guard's release,
 * the rebalanced delete guard, resets rate limited, and subscribers kept (and dropped, with an alarm) for their deletes.
 */
class SyncRound2Test {

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

    // ------------------------------------------------------------------ the binding (sync_key, sync_producers)

    @Test
    void aMirrorWithoutPinsSyncsNothingAndListsWhoAnsweredUntilAnAdminPinsItsProducerByKey() throws Exception {
        TestRig p = producer("plugin-p", scan, "sync_key", "");
        // no pin configured, none set by an admin
        TestRig s = subscriber("plugin-s", mirror, "sync_key", "", "sync_producers", "");
        write(scan.resolve("a.txt"), "from p");
        subscribe(p, s);
        cycle(p, s);
        assertFalse(Files.exists(mirror.resolve("a.txt")), "a mirror without a pin took a file from the first producer that answered");
        MsgEvent st = s.call("repostate");
        assertTrue(st.getParam("pinned_producer").startsWith("none"), st.getParam("pinned_producer"));
        assertTrue(st.getParam("candidate_producers").contains(p.repo.producerPin()), st.getParam("candidate_producers"));
        // another producer of the name answers too: listed, nothing taken from either
        Path other = Files.createDirectories(tmp.resolve("other"));
        write(other.resolve("b.txt"), "from q");
        TestRig q = producer("plugin-q", other, "sync_key", "");
        subscribe(q, s);
        cycle(q, s);
        assertFalse(Files.exists(mirror.resolve("b.txt")), "an unpinned producer added a file");
        st = s.call("repostate");
        assertEquals("2", st.getParam("unpinned_producers"));
        assertTrue(st.getParam("candidate_producers").contains(q.repo.producerPin()), st.getParam("candidate_producers"));
        // an admin pins p by its key (confirmed with the mirror's directory): a new generation, p offers everything
        assertEquals("unconfirmed", s.call(TestRig.as(s.requestTo("syncpin", "producer", p.repo.producerPin()), "admin"))
                .getParam("refusal"));
        // a directory is no identity: a pin names the producer's key
        assertEquals("pin", s.call(TestRig.as(s.requestTo("syncpin", "confirm", mirror.toString(), "producer",
                TestRig.REGION + "/" + TestRig.AGENT + ":" + scan), "admin")).getParam("refusal"));
        MsgEvent pinned = s.call(TestRig.as(s.requestTo("syncpin", "confirm", mirror.toString(), "producer",
                p.repo.producerPin()), "admin"));
        assertEquals("10", pinned.getParam("status"), pinned.getParams().toString());
        subscribe(p, s);
        cycle(p, s);
        assertEquals("from p", read(mirror.resolve("a.txt")));
        // re-pinned to q, handing it what p delivered: q offers everything; p is now the unpinned one
        MsgEvent repin = s.call(TestRig.as(s.requestTo("syncpin", "confirm", mirror.toString(), "producer",
                q.repo.producerPin(), "adopt", "true"), "admin"));
        assertEquals("10", repin.getParam("status"), repin.getParams().toString());
        subscribe(q, s);
        cycle(q, s);
        assertEquals("from q", read(mirror.resolve("b.txt")));
        write(scan.resolve("c.txt"), "p again");
        subscribe(p, s);
        cycle(p, s);
        assertFalse(Files.exists(mirror.resolve("c.txt")));
        // reset: back to syncing nothing, never to whoever answers next
        MsgEvent reset = s.call(TestRig.as(s.requestTo("syncpin", "confirm", mirror.toString(), "reset", "true"), "admin"));
        assertEquals("10", reset.getParam("status"), reset.getParams().toString());
        write(other.resolve("d.txt"), "q after the reset");
        write(scan.resolve("e.txt"), "p after the reset");
        subscribe(p, s);
        cycle(p, s);
        subscribe(q, s);
        cycle(q, s);
        assertFalse(Files.exists(mirror.resolve("d.txt")) || Files.exists(mirror.resolve("e.txt")), "a producer was taken after a reset");
        // what q held here (a.txt, adopted from p, and b.txt) is kept, frozen, and shown with the command that hands it on
        st = s.call("repostate");
        assertTrue(st.getParam("pinned_producer").startsWith("none"), st.getParam("pinned_producer"));
        assertEquals("2", st.getParam("orphaned_files"), st.getParams().toString());
        assertTrue(st.getParam("orphaned_from").contains(q.repo.producerPin()), st.getParam("orphaned_from"));
        assertTrue(st.getParam("orphaned_resolve").contains("adopt=true from="), st.getParam("orphaned_resolve"));
        assertEquals("from p", read(mirror.resolve("a.txt")));
        assertEquals("from q", read(mirror.resolve("b.txt")));
    }

    @Test
    void aForgedDiscoverCostsNothingAndSendsNothing() throws Exception {
        TestRig s = subscriber("plugin-s", mirror);
        Map<String, String> d = new HashMap<>();
        d.put("action", "discover");
        d.put("filerepo_name", "fr");
        d.put("sync_proto", RepoEngine.SYNC_PROTO);
        d.put("repo_region_id", TestRig.REGION);
        d.put("repo_agent_id", TestRig.AGENT);
        d.put("repo_plugin_id", "plugin-forger");
        d.put("sync_ts", String.valueOf(System.currentTimeMillis()));
        d.put("sync_auth", "00".repeat(32));
        s.repo.onSubscriberMessage(d);
        // a real proof, but replayed long after: stale
        String old = String.valueOf(System.currentTimeMillis() - 3_600_000L);
        d.put("sync_ts", old);
        d.put("sync_auth", SyncAuth.mac(TestRig.SYNC_KEY.getBytes(), "discover", "fr",
                SyncAuth.addr(TestRig.REGION, TestRig.AGENT, "plugin-forger"), old));
        s.repo.onSubscriberMessage(d);
        assertTrue(s.sent.isEmpty(), "no reposubscribe for a discover without a fresh proof");
        assertEquals(2, s.repo.syncUnverified.get());
    }

    @Test
    void aKeyedProducerServesOnlyMirrorsThatHoldTheKey() throws Exception {
        TestRig p = producer("plugin-p", scan);
        // pinned to the producer, so it asks; but it has another key
        TestRig s = subscriber("plugin-s", mirror, "sync_key", SyncReceiver.token(), "sync_producers", p.repo.producerPin());
        MsgEvent sub = s.requestTo("reposubscribe", "filerepo_name", "fr", "sync_gen", "abc", "sync_proto", RepoEngine.SYNC_PROTO,
                "sync_nonce", SyncReceiver.token(), "sync_auth", "00");
        Fabric.stamp(sub, "plugin-s");
        p.call(sub);
        MsgEvent answer = p.sent.stream().filter(m -> "reposubscribed".equals(m.getParam("action"))).findFirst().orElseThrow();
        assertEquals("false", answer.getParam("accepted"));
        assertTrue(answer.getParam("refusal").contains("sync_key"), answer.getParam("refusal"));
        assertFalse(p.repo.isSubscriber(TestRig.REGION, TestRig.AGENT, "plugin-s"));
    }

    @Test
    void producersPinnedByAnAdminSyncWithoutAKey() throws Exception {
        TestRig p = producer("plugin-p", scan, "sync_key", "");
        TestRig s = subscriber("plugin-s", mirror, "sync_key", "", "sync_producers", p.repo.producerPin());
        write(scan.resolve("a.txt"), "pinned");
        subscribe(p, s);
        cycle(p, s);
        assertEquals("pinned", read(mirror.resolve("a.txt")));
        // another producer of the name on the same node, from another directory: not the pin; nothing it offers lands
        Path other = Files.createDirectories(tmp.resolve("other"));
        write(other.resolve("b.txt"), "not pinned");
        TestRig q = producer("plugin-q", other, "sync_key", "");
        subscribe(q, s);
        cycle(q, s);
        assertFalse(Files.exists(mirror.resolve("b.txt")));
        // and syncpin does not override configured pins
        assertEquals("pin", s.call(TestRig.as(s.requestTo("syncpin", "confirm", mirror.toString(), "reset", "true"), "admin"))
                .getParam("refusal"));
    }

    // ------------------------------------------------------------------ state outside the plugin's data directory

    @Test
    void oneRepositoryDirectoryIsServedByOnePluginAtATime() throws Exception {
        TestRig first = subscriber("plugin-a", mirror);
        TestRig second = subscriber("plugin-b", mirror);
        assertNull(first.db.openError());
        assertNotNull(second.db.openError(), "a second plugin over the same directory must not open its catalog");
        assertTrue(second.db.openError().contains("already served"), second.db.openError());
        assertFalse(second.repo.isCatalogHealthy());
        // the first one goes: the directory can be served again, with the same catalog
        first.close();
        rigs.remove(first);
        TestRig third = subscriber("plugin-c", mirror);
        assertNull(third.db.openError());
    }

    @Test
    void theOldCatalogAndDefaultRepositoryMoveOutOfThePluginsDataDirectoryAndNothingIsDeleted() throws Exception {
        // a plugin from before 1.3 hardening: its catalog and its default repository inside its own data directory,
        // a file landed there and cataloged under that path
        Map<String, Object> cfg = new HashMap<>();
        TestRig old = new TestRig(tmp.resolve("agent"), cfg, null, "plugin-old", null);
        Path pluginData = Path.of(old.plugin.getPluginDataDirectory());
        Path stable = old.repo.repoDirFile().toPath();
        Path stableState = old.db.stateDir();
        Path landed = write(pluginData.resolve("repo/landed.bin"), "landed before the upgrade");
        old.db.addFile(landed.toString(), Hashes.content(landed), 1L, Files.size(landed));
        old.close();
        Files.createDirectories(pluginData.resolve("derbydb-home"));
        Files.move(stableState.resolve("catalog"), pluginData.resolve("derbydb-home/filerepo-db"));
        Files.deleteIfExists(stable.resolve(RepoEngine.PRIVATE_STAGING));
        Files.deleteIfExists(stable);

        TestRig upgraded = new TestRig(tmp.resolve("agent"), cfg, null, "plugin-old", null);
        rigs.add(upgraded);
        assertTrue(Files.isDirectory(stableState.resolve("catalog")), "the catalog moved");
        assertFalse(Files.exists(pluginData.resolve("derbydb-home/filerepo-db")));
        Path moved = stable.resolve("landed.bin");
        assertEquals("landed before the upgrade", read(moved), "the default repository moved, its files with it");
        assertNotNull(upgraded.repo.getFileInfo(moved.toString()), "and the catalog names them where they are now");
        MsgEvent got = upgraded.call("getfile", "file_path", moved.toString());
        assertEquals("10", got.getParam("status"), got.getParams().toString());
    }

    @Test
    void anOldDefaultRepositoryIsNeverMergedIntoANewOneThatHoldsFiles() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        TestRig a = new TestRig(tmp.resolve("agent"), cfg, null, "plugin-x", null);
        Path pluginData = Path.of(a.plugin.getPluginDataDirectory());
        Path stable = a.repo.getRepoDir().toPath();
        a.close();
        write(stable.resolve("new.bin"), "new");
        Path legacy = write(pluginData.resolve("repo/old.bin"), "old").getParent();
        TestRig b = new TestRig(tmp.resolve("agent"), cfg, null, "plugin-x", null);
        rigs.add(b);
        assertEquals("old", read(legacy.resolve("old.bin")), "left where it was");
        assertEquals("new", read(stable.resolve("new.bin")));
        assertTrue(b.log.stream().anyMatch(l -> l.contains("both hold files")), b.log.toString());
    }

    // ------------------------------------------------------------------ claims of a producer that went silent

    @Test
    void aSilentProducerMayUpdateItsOrphanedFilesButItsDeletesWaitForAnAdmin() throws Exception {
        TestRig p = producer("plugin-p", scan);
        TestRig s = subscriber("plugin-s", mirror, "sync_holder_ttl_ms", "60000");
        Path f = write(scan.resolve("model.bin"), "v1");
        write(scan.resolve("notes.txt"), "notes");
        for (int i = 0; i < 10; i++) write(scan.resolve("keep" + i + ".txt"), "keep " + i);
        subscribe(p, s);
        cycle(p, s);
        String holder = s.db.holdings("model.bin").keySet().iterator().next();
        // silent for longer than the holder TTL: its claims are orphaned (still protecting the files)
        assertEquals(List.of(holder), s.repo.orphanSilentProducers(System.currentTimeMillis() + 1));
        assertTrue(s.db.holdings("model.bin").containsKey(DBEngine.ORPHAN + holder));
        // it is back: it (and only it) may update what it delivered
        subscribe(p, s);
        write(f, "v2");
        Files.setLastModifiedTime(f, FileTime.fromMillis(System.currentTimeMillis() + 10_000));
        cycle(p, s);
        assertEquals("v2", read(mirror.resolve("model.bin")));
        assertTrue(s.db.holdings("model.bin").containsKey(holder), "updated: its claim is live again");
        // but deleting orphaned content waits for an admin's release, with this mirror's hold id
        Files.delete(scan.resolve("notes.txt"));
        assertTrue(p.repo.scanOnce());
        await(() -> s.repo.getDeletesHeld() == 1, "the delete held at the mirror");
        assertTrue(Files.exists(mirror.resolve("notes.txt")));
        MsgEvent st = s.call("repostate");
        assertEquals("1", st.getParam("deletes_held"));
        assertEquals("hold-id-required", s.call(TestRig.as(s.requestTo("releasedeletes", "confirm", mirror.toString()), "admin"))
                .getParam("refusal"));
        MsgEvent rel = s.call(TestRig.as(s.requestTo("releasedeletes", "confirm", mirror.toString(), "hold_id",
                st.getParam("deletes_hold_id")), "admin"));
        assertEquals("1", rel.getParam("released"), rel.getParams().toString());
        assertFalse(Files.exists(mirror.resolve("notes.txt")));
    }

    @Test
    void anotherPinnedProducerNeverReplacesOrphanedContent() throws Exception {
        // two producers pinned by their keys: co-producers of the name
        TestRig p = producer("plugin-p", scan);
        Path other = Files.createDirectories(tmp.resolve("other"));
        write(other.resolve("model.bin"), "q's");
        TestRig q = producer("plugin-q", other);
        TestRig s = subscriber("plugin-s", mirror, "sync_producers", p.repo.producerPin() + "," + q.repo.producerPin());
        write(scan.resolve("model.bin"), "v1");
        subscribe(p, s);
        cycle(p, s);
        subscribe(q, s);
        // p goes silent beyond the holder TTL (q is heard from): its content is orphaned, and q may still not replace it
        String pHolder = s.db.holdings("model.bin").keySet().iterator().next();
        String qHolder = new RepoEngine.Identity(Fabric.TENANT, TestRig.REGION, TestRig.AGENT, "fr", q.repo.signingFingerprint(),
                null).holder();
        s.db.touchHolder(qHolder, System.currentTimeMillis() + 60_000L);
        assertEquals(List.of(pHolder), s.repo.orphanSilentProducers(System.currentTimeMillis() + 1));
        assertTrue(s.repo.isKnownProducer(TestRig.REGION, TestRig.AGENT, "plugin-q"), "precondition: q is still a live producer");
        assertTrue(s.db.holdings("model.bin").containsKey(DBEngine.ORPHAN + pHolder));
        for (int i = 0; i < 3; i++) {
            assertTrue(q.repo.scanOnce());
            Thread.sleep(300);
        }
        assertEquals("v1", read(mirror.resolve("model.bin")), "another producer replaced orphaned content");
    }

    // ------------------------------------------------------------------ the rewrite guard

    @Test
    void heldRewritesAreReleasedOnlyByTheirIdAndAContentRestoredIsNoLongerHeld() throws Exception {
        TestRig p = producer("plugin-p", scan);
        TestRig s = subscriber("plugin-s", mirror);
        subscribe(p, s);
        for (int i = 0; i < 20; i++) write(scan.resolve("d" + i + ".txt"), "real " + i);
        cycle(p, s);
        for (int i = 0; i < 20; i++) {
            Path f = write(scan.resolve("d" + i + ".txt"), "rewritten " + i);
            Files.setLastModifiedTime(f, FileTime.fromMillis(System.currentTimeMillis() + 10_000));
        }
        assertTrue(p.repo.scanOnce());
        assertEquals("20", p.call("repostate").getParam("rewrites_held"));
        String id = p.call("repostate").getParam("rewrites_hold_id");
        // one is restored: no longer held, and never offered (the mirror holds that content)
        Path back = write(scan.resolve("d3.txt"), "real 3");
        Files.setLastModifiedTime(back, FileTime.fromMillis(System.currentTimeMillis() + 20_000));
        assertTrue(p.repo.scanOnce());
        assertEquals("19", p.call("repostate").getParam("rewrites_held"));
        String id19 = p.call("repostate").getParam("rewrites_hold_id");
        assertNotEquals(id, id19);
        // a new subscriber is not offered the held content either
        Path mirror2 = Files.createDirectories(tmp.resolve("mirror2"));
        TestRig s2 = subscriber("plugin-s2", mirror2);
        subscribe(p, s2);
        assertTrue(p.repo.scanOnce());
        await(() -> Files.exists(mirror2.resolve("d3.txt")), "s2 receives what is not held");
        Thread.sleep(300);
        assertFalse(Files.exists(mirror2.resolve("d7.txt")), "a held rewrite was offered to a new subscriber");
        // releases: without an id, with the old id, then exactly
        assertEquals("hold-id-required", p.call(TestRig.as(p.requestTo("releaserewrites", "confirm", scan.toString()), "admin")).getParam("refusal"));
        assertEquals("hold-changed", p.call(TestRig.as(p.requestTo("releaserewrites", "confirm", scan.toString(), "hold_id", id), "admin")).getParam("refusal"));
        MsgEvent rel = p.call(TestRig.as(p.requestTo("releaserewrites", "confirm", scan.toString(), "hold_id", id19), "admin"));
        assertEquals("19", rel.getParam("released"));
        assertTrue(p.repo.scanOnce());
        await(() -> "rewritten 7".equals(read(mirror.resolve("d7.txt"))) && "rewritten 7".equals(read(mirror2.resolve("d7.txt"))),
                "released rewrites reach every mirror");
        assertEquals("real 3", read(mirror.resolve("d3.txt")));
        assertNull(p.call("repostate").getParam("rewrites_held"));
    }

    @Test
    void aFewEditsAreNotHeldAndATouchIsNotAChange() throws Exception {
        TestRig p = producer("plugin-p", scan);
        TestRig s = subscriber("plugin-s", mirror);
        subscribe(p, s);
        for (int i = 0; i < 20; i++) write(scan.resolve("e" + i + ".txt"), "e" + i);
        cycle(p, s);
        for (int i = 0; i < 3; i++) {
            Path f = write(scan.resolve("e" + i + ".txt"), "edited " + i);
            Files.setLastModifiedTime(f, FileTime.fromMillis(System.currentTimeMillis() + 10_000));
        }
        // touched only: same content, a new time
        Files.setLastModifiedTime(scan.resolve("e9.txt"), FileTime.fromMillis(System.currentTimeMillis() + 10_000));
        assertTrue(p.repo.scanOnce());
        assertNull(p.call("repostate").getParam("rewrites_held"), "three edits and a touch are not a mass rewrite");
        await(() -> "edited 2".equals(read(mirror.resolve("e2.txt"))), "edits arrive");
        await(() -> p.db.pendingCount(subKey(s)) == 0, "settled");
    }

    // ------------------------------------------------------------------ the delete guard, rebalanced

    @Test
    void anOrdinarySmallSubtreeDeletionGoesThroughInALargeCatalog() throws Exception {
        TestRig p = producer("plugin-p", scan);
        for (int i = 0; i < 200; i++) write(scan.resolve("keep/k" + i + ".txt"), "k" + i);
        for (int i = 0; i < 5; i++) write(scan.resolve("old/o" + i + ".txt"), "o" + i);
        assertTrue(p.repo.scanOnce());
        try (var st = Files.list(scan.resolve("old"))) {
            for (Path f : (Iterable<Path>) st::iterator) Files.delete(f);
        }
        Files.delete(scan.resolve("old"));
        assertTrue(p.repo.scanOnce());
        assertEquals(0, p.repo.getDeletesHeld(), "rm -rf of a 5-file directory in a 205-file catalog");
        assertEquals(5, p.repo.getTombstoneCount());
        // a whole subtree of 20 (above sync_delete_guard_subtree_min and 1% of the catalog) is held
        for (int i = 0; i < 20; i++) write(scan.resolve("vol/v" + i + ".txt"), "v" + i);
        assertTrue(p.repo.scanOnce());
        try (var st = Files.list(scan.resolve("vol"))) {
            for (Path f : (Iterable<Path>) st::iterator) Files.delete(f);
        }
        assertTrue(p.repo.scanOnce());
        assertEquals(20, p.repo.getDeletesHeld());
    }

    @Test
    void aLossSpreadOverSeveralCyclesIsHeldOnceItAddsUp() throws Exception {
        TestRig p = producer("plugin-p", scan);
        for (int i = 0; i < 100; i++) write(scan.resolve("f" + i + ".txt"), "f" + i);
        assertTrue(p.repo.scanOnce());
        // a slow drip: 4 files a cycle, each cycle well under 10% of the catalog
        int recorded = 0;
        for (int cycle = 0; cycle < 5; cycle++) {
            for (int i = 0; i < 4; i++) Files.delete(scan.resolve("f" + (cycle * 4 + i) + ".txt"));
            assertTrue(p.repo.scanOnce());
            recorded = (int) p.repo.getTombstoneCount();
            if (p.repo.getDeletesHeld() > 0) break;
        }
        assertTrue(p.repo.getDeletesHeld() > 0, "a drip of deletes adds up within sync_delete_guard_window_ms");
        assertTrue(recorded <= 10, "what got through unheld is bounded by the threshold: " + recorded);
        // released by its id: the window starts over, an ordinary delete then goes through
        String id = p.repo.getHoldId();
        assertTrue(p.repo.releaseHeldDeletes(id) > 0);
        assertTrue(p.repo.scanOnce());
        assertEquals(0, p.repo.getDeletesHeld());
        Files.delete(scan.resolve("f99.txt"));
        assertTrue(p.repo.scanOnce());
        assertEquals(0, p.repo.getDeletesHeld(), "after a release, one delete is ordinary again");
    }

    @Test
    void twoOrdinaryDeletesInASmallRepositoryAreNotHeldAThirdSoonAfterIs() throws Exception {
        TestRig p = producer("plugin-p", scan);
        for (int i = 0; i < 6; i++) write(scan.resolve("s" + i + ".txt"), "s" + i);
        assertTrue(p.repo.scanOnce());
        Files.delete(scan.resolve("s0.txt"));
        assertTrue(p.repo.scanOnce());
        Files.delete(scan.resolve("s1.txt"));
        assertTrue(p.repo.scanOnce());
        assertEquals(0, p.repo.getDeletesHeld(), "two deletes, one at a time, in a six-file repository");
        assertEquals(2, p.repo.getTombstoneCount());
        Files.delete(scan.resolve("s2.txt"));
        assertTrue(p.repo.scanOnce());
        assertEquals(1, p.repo.getDeletesHeld(), "a third within sync_delete_guard_window_ms: half the repository is going");
    }

    // ------------------------------------------------------------------ resets, and subscribers kept for their deletes

    @Test
    void aSubscriberResetsAFewTimesAtOnceThenAtTheResetInterval() throws Exception {
        TestRig p = producer("plugin-p", scan);
        TestRig s = subscriber("plugin-s", mirror);
        subscribe(p, s);
        int accepted = 0;
        for (int i = 0; i < 6; i++) {
            String gen = SyncReceiver.token();
            String nonce = SyncReceiver.token();
            MsgEvent sub = s.requestTo("reposubscribe", "filerepo_name", "fr", "sync_gen", gen, "sync_proto",
                    RepoEngine.SYNC_PROTO, "sync_nonce", nonce, "sync_auth", SyncAuth.mac(TestRig.SYNC_KEY.getBytes(),
                            "reposubscribe", "fr", SyncAuth.addr(TestRig.REGION, TestRig.AGENT, "plugin-s"),
                            SyncAuth.addr(TestRig.REGION, TestRig.AGENT, "plugin-p"), gen, nonce));
            Fabric.stamp(sub, "plugin-s");
            p.sent.clear();
            p.call(sub);
            MsgEvent answer = p.sent.stream().filter(m -> "reposubscribed".equals(m.getParam("action"))).findFirst().orElseThrow();
            if ("true".equals(answer.getParam("accepted"))) accepted++;
            else assertTrue(answer.getParam("refusal").contains("too soon"), answer.getParam("refusal"));
        }
        assertEquals(3, accepted, "three resets at once, then one per sync_reset_interval_ms");
    }

    @Test
    void aSilentSubscriberIsKeptForItsDeletesThenDroppedWithAnAlarm() throws Exception {
        TestRig p = producer("plugin-p", scan, "sync_subscriber_ttl_ms", "60000", "sync_tombstone_ttl_ms", "1",
                "sync_rpc_timeout_ms", "300");
        TestRig s = subscriber("plugin-s", mirror);
        for (int i = 0; i < 30; i++) write(scan.resolve("t" + i + ".txt"), "t" + i);
        subscribe(p, s);
        cycle(p, s);
        // the mirror goes away; a file is deleted at the producer
        fabric.dropRoute = m -> "repolistin".equals(m.getParam("action"));
        Files.delete(scan.resolve("t0.txt"));
        assertTrue(p.repo.scanOnce());
        assertEquals(1, p.repo.getTombstoneCount());
        Thread.sleep(20);
        // older than the tombstone TTL, but the kept subscriber still needs it: not purged
        assertTrue(p.repo.scanOnce());
        assertEquals(1, p.repo.getTombstoneCount(), "a delete a kept subscriber has not applied is never purged");
        assertTrue(p.repo.isSubscriber(TestRig.REGION, TestRig.AGENT, "plugin-s"));
        // silent beyond sync_subscriber_ttl_ms: dropped, with an alarm; then nothing keeps the tombstone
        p.repo.clock = () -> System.currentTimeMillis() + 120_000L;
        assertTrue(p.repo.scanOnce());
        assertFalse(p.repo.isSubscriber(TestRig.REGION, TestRig.AGENT, "plugin-s"));
        assertTrue(p.log.stream().anyMatch(l -> l.contains("dropped") && l.contains("clear it")), p.log.toString());
        assertEquals(1, p.repo.subscribersDropped.get());
        assertTrue(p.repo.scanOnce());
        assertEquals(0, p.repo.getTombstoneCount(), "purged once no kept subscriber needs it");
    }
}
