package io.cresco.filerepo;

import io.cresco.library.messaging.MsgEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Adversarial review, round 2 (review/plugins-20261104), of filerepo 3a45a47: sync protocol 2, the device/subtree
 * guard with hold ids, per-producer mirror bookkeeping, concurrent offers. Every test FAILS on 3a45a47. Local only.
 */
class ReviewRound2Test {

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

    /** Discover, then the mirror's reposubscribe and the producer's answer, as protocol 2 does it. */
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

    /** One scan, then wait until the mirror has settled everything it was offered. */
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

    // =================================================================== DATA LOSS

    /**
     * HIGH. The design is that a release names the hold it releases. releasedeletes takes hold_id as OPTIONAL: without
     * it, it arms whatever is held at that instant. An admin who read hold A (three stray deletes) and releases
     * without naming it, after the hold became B (the volume went away), releases B: every mirror is emptied.
     */
    @Test
    void aReleaseThatDoesNotNameItsHoldIsRefused() throws Exception {
        TestRig p = producer("plugin-p", scan);
        TestRig s = subscriber("plugin-s", mirror);
        subscribe(p, s);
        for (int i = 0; i < 20; i++) write(scan.resolve("f" + i + ".dat"), "f" + i);
        cycle(p, s);

        for (int i = 0; i < 3; i++) Files.delete(scan.resolve("f" + i + ".dat"));
        assertTrue(p.repo.scanOnce());
        String seen = p.call("repostate").getParam("deletes_hold_id");
        assertNotNull(seen, "precondition: three of twenty held");

        // the volume goes away before the admin acts
        try (var st = Files.list(scan)) {
            for (Path f : (Iterable<Path>) st::iterator) Files.delete(f);
        }
        assertTrue(p.repo.scanOnce());
        assertNotEquals(seen, p.repo.getHoldId(), "precondition: the hold changed");

        // the admin releases what it read about (three files), without naming it
        MsgEvent rel = p.call(TestRig.as(p.requestTo("releasedeletes", "confirm", scan.toString()), "admin"));
        assertTrue(p.repo.scanOnce());
        boolean emptied = within(5000, () -> !Files.exists(mirror.resolve("f19.dat")));
        assertNotEquals("10", rel.getParam("status"),
                "a release without hold_id armed hold " + rel.getParam("hold_id") + " (" + rel.getParam("released")
                        + " deletes), which the admin never saw (mirror emptied: " + emptied + ")");
        assertFalse(emptied, "the mirror was emptied");
    }

    /**
     * MED (design gap). The guard holds a mass DELETE, but a mass CHANGE propagates at once: a scan dir whose files are
     * all rewritten (ransomware, or the wrong volume or an old snapshot mounted on the mount point) overwrites every
     * mirror's copy in place, and the mirror keeps no earlier version.
     */
    @Test
    void aMassRewriteIsHeldLikeAMassDelete() throws Exception {
        TestRig p = producer("plugin-p", scan);
        TestRig s = subscriber("plugin-s", mirror);
        subscribe(p, s);
        for (int i = 0; i < 20; i++) write(scan.resolve("doc" + i + ".txt"), "the real document " + i);
        cycle(p, s);
        for (int i = 0; i < 20; i++) {
            Path f = write(scan.resolve("doc" + i + ".txt"), "ENCRYPTED-" + i);
            Files.setLastModifiedTime(f, FileTime.fromMillis(System.currentTimeMillis() + 10_000));
        }
        assertTrue(p.repo.scanOnce());
        boolean overwritten = within(5000, () -> "ENCRYPTED-7".equals(read(mirror.resolve("doc7.txt"))));
        assertFalse(overwritten, "every mirror copy was overwritten by a rewrite of 100% of the catalog in one cycle");
    }

    /**
     * HIGH (FR-D9 x the controller; confirmed live). With no repo_dir and no scan_dir the repository defaults to
     * <plugin data dir>/repo. The controller deletes a plugin's whole plugin-data directory when it stops a plugin
     * added at runtime (persistence_code 0, the default) -- on pluginremove AND on every graceful agent shutdown
     * (PluginAdmin.stopAllPlugins -> stopPlugin(pid, false): "Removing stale plugin-data"). Everything landed there
     * (repostate ONLY_COPY: putjar, putfiles) is deleted with no clearrepo, no confirm and no force.
     */
    @Test
    void theDefaultRepositoryIsNotInsideTheDirectoryTheControllerDeletesOnStop() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        TestRig r = new TestRig(tmp.resolve("agent"), cfg, null, "plugin-ondemand", null);
        rigs.add(r);
        Path repoDir = r.repo.repoDirFile().toPath().toAbsolutePath();
        Path pluginData = java.nio.file.Paths.get(r.plugin.getPluginDataDirectory()).toAbsolutePath();
        assertFalse(repoDir.startsWith(pluginData), "the default repository " + repoDir + " is inside " + pluginData
                + ", which the controller deletes when it stops a runtime-added plugin");
    }

    // =================================================================== AUTHORIZATION (sync_producers unset: the default)

    /**
     * HIGH. With sync_producers unset (the default), a mirror subscribes to anything that broadcasts a discover for its
     * name and answers reposubscribe as a producer. Content is first-writer-wins per name: a rogue that offers a name
     * first (here release-2.bin, before the real producer publishes it) owns it in every mirror, and the real
     * producer's file is refused as "another producer's" forever.
     */
    @Test
    void aMirrorWithDefaultsTakesNamesOnlyFromItsRealProducer() throws Exception {
        TestRig p = producer("plugin-p", scan);
        TestRig s = subscriber("plugin-s", mirror);
        subscribe(p, s);
        write(scan.resolve("release-1.bin"), "good release 1");
        cycle(p, s);

        Path rogueDir = Files.createDirectories(tmp.resolve("rogue"));
        write(rogueDir.resolve("release-2.bin"), "MALICIOUS release 2");
        // (adapted, round 2 fix: a mirror takes a producer only by the binding an admin provisions, the repository's
        // sync_key, which the test fabric gives its real producer and mirror; a tenant plugin does not have it. The
        // rogue announces itself and is never taken: subscribe(rogue, s) cannot succeed, and must not)
        TestRig rogue = producer("plugin-x", rogueDir, "sync_key", "");
        rogue.repo.repoBroadcast("fr", "discover", 0);
        assertFalse(within(1000, () -> s.repo.isKnownProducer(TestRig.REGION, TestRig.AGENT, "plugin-x")),
                "the mirror took a plugin without the repository's sync_key as its producer");
        TestRig rogueWithAGuess = producer("plugin-y", rogueDir, "sync_key", SyncReceiver.token());
        rogueWithAGuess.repo.repoBroadcast("fr", "discover", 0);
        assertFalse(within(1000, () -> s.repo.isKnownProducer(TestRig.REGION, TestRig.AGENT, "plugin-y")),
                "the mirror took a plugin with another sync_key as its producer");
        assertTrue(rogue.repo.scanOnce());
        assertTrue(rogueWithAGuess.repo.scanOnce());

        write(scan.resolve("release-2.bin"), "good release 2");
        assertTrue(p.repo.scanOnce());
        Thread.sleep(1000);
        assertEquals("good release 2", read(mirror.resolve("release-2.bin")),
                "the mirror serves a rogue plugin's release-2.bin; the real producer's is refused (conflicts="
                        + s.repo.syncConflicts.get() + ")");
    }

    /**
     * MED. Recording a co-holder needs no bytes: a producer that offers a name with the content hash the mirror already
     * holds is recorded as holding it. From then on the real producer's updates are refused ("another producer's"),
     * forever. Two legitimate producers that share a file freeze it the same way.
     */
    @Test
    void aCoHolderNeverFreezesTheRealProducersUpdates() throws Exception {
        TestRig p = producer("plugin-p", scan);
        TestRig s = subscriber("plugin-s", mirror);
        subscribe(p, s);
        Path f = write(scan.resolve("model.bin"), "weights v1");
        cycle(p, s);

        Path other = Files.createDirectories(tmp.resolve("other"));
        write(other.resolve("model.bin"), "weights v1");
        TestRig second = producer("plugin-q", other);
        subscribe(second, s);
        cycle(second, s);

        write(f, "weights v2");
        Files.setLastModifiedTime(f, FileTime.fromMillis(System.currentTimeMillis() + 10_000));
        for (int i = 0; i < 3; i++) {
            assertTrue(p.repo.scanOnce());
            Thread.sleep(300);
        }
        assertEquals("weights v2", read(mirror.resolve("model.bin")),
                "plugin-p's update of model.bin is refused because plugin-q also 'holds' v1 (conflicts="
                        + s.repo.syncConflicts.get() + ")");
    }

    /**
     * HIGH (ordinary operations). A mirror records content per producer plugin id, and those claims never expire. A
     * producer that is redeployed gets a new plugin id (pluginadd gives plugin-<uuid>); the new instance is recorded as a
     * co-holder of everything the old one delivered, so every later update (and every delete) of every file is refused
     * at every mirror, because the old id still "holds" it. Sync silently freezes after the first redeploy.
     */
    @Test
    void aRedeployedProducerKeepsItsMirrorsUpToDate() throws Exception {
        TestRig p = producer("plugin-p", scan);
        TestRig s = subscriber("plugin-s", mirror);
        subscribe(p, s);
        Path f = write(scan.resolve("model.bin"), "weights v1");
        Path g = write(scan.resolve("notes.txt"), "notes");
        cycle(p, s);

        // the producer plugin is redeployed: same scan dir, new plugin id
        p.close();
        rigs.remove(p);
        TestRig p2 = producer("plugin-p-redeployed", scan);
        subscribe(p2, s);
        cycle(p2, s);

        write(f, "weights v2");
        Files.setLastModifiedTime(f, FileTime.fromMillis(System.currentTimeMillis() + 10_000));
        Files.delete(g);
        for (int i = 0; i < 3; i++) {
            assertTrue(p2.repo.scanOnce());
            Thread.sleep(300);
        }
        assertEquals("weights v2", read(mirror.resolve("model.bin")),
                "after a redeploy the mirror refuses the producer's update (conflicts=" + s.repo.syncConflicts.get()
                        + "; notes.txt deleted at the producer still at the mirror: " + Files.exists(mirror.resolve("notes.txt")) + ")");
        assertFalse(Files.exists(mirror.resolve("notes.txt")), "a delete after a redeploy is never applied");
    }

    /**
     * HIGH. clearrepo renews the mirror's generation first, but a transfer already being applied (sync takes no gate)
     * goes on and confirms the files it fetched before the clear deleted them. A confirm is bound to the producer's
     * epoch and the subscriber, not to the subscriber's generation: arriving after the producer reset the mirror's
     * pending set for the new generation, it settles those files again, and they are never offered to the mirror.
     */
    @Test
    void aTransferFromBeforeAClearNeverSettlesFilesTheClearDeleted() throws Exception {
        TestRig p = producer("plugin-p", scan);
        TestRig s = subscriber("plugin-s", mirror, "sync_rpc_timeout_ms", "3000", "sync_stream_rounds", "1");
        subscribe(p, s);
        assertTrue(p.repo.scanOnce());
        write(scan.resolve("one.txt"), "one");
        write(scan.resolve("two.txt"), "two");
        // the mirror's second fetch of this transfer stalls (an unanswered streamfile, 3 s)
        AtomicInteger fetches = new AtomicInteger();
        fabric.dropRoute = m -> "streamfile".equals(m.getParam("action")) && fetches.incrementAndGet() == 2;
        assertTrue(p.repo.scanOnce());
        await(() -> Files.exists(mirror.resolve("one.txt")) || Files.exists(mirror.resolve("two.txt")), "the first file fetched");
        Path first = Files.exists(mirror.resolve("one.txt")) ? mirror.resolve("one.txt") : mirror.resolve("two.txt");

        // while the second fetch stalls: the mirror is cleared, and subscribes again with its new generation
        MsgEvent c = s.call(TestRig.as(s.requestTo("clearrepo", "repo_name", "fr", "confirm", mirror.toString()), "admin"));
        assertEquals("10", c.getParam("status"), c.getParams().toString());
        assertFalse(Files.exists(first));
        p.repo.repoBroadcast("fr", "discover", 0);
        await(() -> p.db.pendingCount(subKey(s)) == 2, "the producer reset the cleared mirror: both files pending");
        // the stalled transfer ends and is confirmed (the first file "held", the second failed)
        await(() -> s.sent.stream().anyMatch(m -> "repoconfirm".equals(m.getParam("action"))), "the old transfer confirmed");
        fabric.dropRoute = m -> false;
        Thread.sleep(300);

        for (int i = 0; i < 4; i++) {
            assertTrue(p.repo.scanOnce());
            Thread.sleep(300);
        }
        assertTrue(within(3000, () -> Files.exists(first)), first.getFileName() + " never returns to the cleared mirror:"
                + " a confirm from before the clear settled it (pending at the producer: " + p.db.pendingCount(subKey(s)) + ")");
    }

    // =================================================================== RESOURCE EXHAUSTION

    /**
     * MED. One token bucket (512 burst, 64/s) is shared by every discover, real or forged; anyone on the tenant's
     * dataplane can keep it empty, and the real producer's discover is then dropped: the mirror never subscribes (or,
     * once subscribed, stops refreshing and its producer expires after sync_peer_ttl_ms).
     */
    @Test
    void forgedDiscoversNeverStopAMirrorSubscribingToItsProducer() throws Exception {
        TestRig s = subscriber("plugin-s", mirror);
        TestRig p = producer("plugin-p", scan);
        for (int i = 0; i < 600; i++) {
            Map<String, String> d = new HashMap<>();
            d.put("action", "discover");
            d.put("filerepo_name", "fr");
            d.put("sync_proto", RepoEngine.SYNC_PROTO);
            d.put("repo_region_id", "ghost-region");
            d.put("repo_agent_id", "ghost-agent");
            d.put("repo_plugin_id", "ghost-" + i);
            s.repo.onSubscriberMessage(d);
        }
        p.repo.repoBroadcast("fr", "discover", 0);
        assertTrue(within(1000, () -> s.repo.isKnownProducer(TestRig.REGION, TestRig.AGENT, "plugin-p")),
                "the real producer's discover was dropped: 600 forged discovers emptied the shared attempt bucket");
    }

    /**
     * MED. A discover that is not protocol 2 records its sender in oldProtocolWarned (to warn once an hour) with no
     * bound on the map (the producer side caps its own at 1024): a stream of forged old-protocol discovers with fresh
     * ids grows the mirror's heap without limit, and logs a WARN per message.
     */
    @Test
    void forgedOldProtocolDiscoversUseBoundedMemory() throws Exception {
        TestRig s = subscriber("plugin-s", mirror);
        for (int i = 0; i < 20_000; i++) {
            Map<String, String> d = new HashMap<>();
            d.put("action", "discover");
            d.put("filerepo_name", "fr");
            d.put("repo_region_id", "r");
            d.put("repo_agent_id", "a");
            d.put("repo_plugin_id", "old-" + i);
            s.repo.onSubscriberMessage(d);
        }
        java.lang.reflect.Field f = RepoEngine.class.getDeclaredField("oldProtocolWarned");
        f.setAccessible(true);
        int size = ((Map<?, ?>) f.get(s.repo)).size();
        assertTrue(size <= 1024, "oldProtocolWarned holds " + size + " entries after 20,000 forged discovers ("
                + s.log.size() + " WARN lines logged)");
    }

    /**
     * MED (scale gate). A mirror's sync generation is new at every start, so a plain restart of a mirror makes its
     * producer mark the whole catalog pending for it again and re-offer it, and the mirror re-reads (hashes) every file
     * it already holds. For a large mirror every restart is a full re-read.
     */
    @Test
    void aMirrorRestartDoesNotReofferTheWholeCatalog() throws Exception {
        TestRig p = producer("plugin-p", scan);
        TestRig s = subscriber("plugin-s", mirror);
        subscribe(p, s);
        for (int i = 0; i < 50; i++) write(scan.resolve("big" + i + ".bin"), "content " + i);
        cycle(p, s);
        assertEquals(0, p.db.pendingCount(subKey(s)));

        // the mirror's agent restarts: same plugin, same directory, same catalog
        s.close();
        rigs.remove(s);
        TestRig again = subscriber("plugin-s", mirror);
        subscribe(p, again);
        long pending = p.db.pendingCount(subKey(again));
        assertTrue(pending < 50, "after a plain restart all " + pending + " files are pending again for the mirror,"
                + " which already holds every one of them");
    }

    /**
     * LOW-MED (introduced by 3a45a47). offerAll waits on every offer future with no timeout; shutdown() calls
     * offerPool.shutdownNow(), which drains queued offers without cancelling their futures. A scan cycle that is
     * waiting when the plugin stops waits forever: the timer thread (and the bundle's classloader) leak on every reload.
     */
    @Test
    void stoppingThePluginNeverStrandsAScanCycle() throws Exception {
        TestRig p = producer("plugin-p", scan, "sync_offer_threads", "1", "sync_rpc_timeout_ms", "20000");
        TestRig s1 = subscriber("plugin-s1", mirror);
        TestRig s2 = subscriber("plugin-s2", Files.createDirectories(tmp.resolve("mirror2")));
        subscribe(p, s1);
        subscribe(p, s2);
        assertTrue(p.repo.scanOnce());
        write(scan.resolve("x.txt"), "x");
        // neither mirror answers this cycle's offers: the first offer waits, the second is queued behind it
        fabric.dropRoute = m -> "repolistin".equals(m.getParam("action"));
        AtomicInteger offers = new AtomicInteger();
        Thread scanner = new Thread(() -> p.repo.scanOnce(), "review-scan");
        scanner.setDaemon(true);
        scanner.start();
        await(() -> fabric.routed.stream().filter("repolistin"::equals).count() >= 1, "the first offer in flight");
        Thread.sleep(200);
        p.repo.shutdown();
        scanner.join(5000);
        assertFalse(scanner.isAlive(), "the scan cycle is still waiting on an offer that shutdown() drained");
    }

    // =================================================================== TOCTOU (macOS path fallback)

    /**
     * MED (macOS, documented residual). Where no SecureDirectoryStream exists, getfile checks by path around the open.
     * A writer of the repository that flips a directory on the path between a link and the real directory gets the
     * agent to read and return a file outside the repository: the inline getfile caller verifies nothing.
     */
    @Test
    void getfileNeverReturnsAFileOutsideTheRepository() throws Exception {
        try (var ds = Files.newDirectoryStream(tmp)) {
            assumeTrue(!(ds instanceof java.nio.file.SecureDirectoryStream), "the descriptor path is used here");
        }
        Path repo = Files.createDirectories(tmp.resolve("repo"));
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("repo_dir", repo.toString());
        TestRig r = rig(TestRig.PLUGIN_ID, cfg);
        Path d = Files.createDirectories(repo.resolve("d"));
        Path pub = write(d.resolve("f.txt"), "public....");
        r.db.addFile(pub.toString(), Hashes.content(pub), Files.getLastModifiedTime(pub).toMillis(), Files.size(pub));
        Path secretDir = Files.createDirectories(tmp.resolve("outside"));
        write(secretDir.resolve("f.txt"), "SECRET-KEY");
        Path link = Files.createSymbolicLink(repo.resolve("d.link"), secretDir);
        Path real = repo.resolve("d.real");

        AtomicBoolean stop = new AtomicBoolean();
        Thread flipper = new Thread(() -> {
            while (!stop.get()) {
                try {
                    Files.move(d, real, StandardCopyOption.ATOMIC_MOVE);
                    Files.move(link, d, StandardCopyOption.ATOMIC_MOVE);
                    Files.move(d, link, StandardCopyOption.ATOMIC_MOVE);
                    Files.move(real, d, StandardCopyOption.ATOMIC_MOVE);
                } catch (Exception ignore) {
                }
            }
        }, "review-flipper");
        flipper.setDaemon(true);
        flipper.start();
        String leaked = null;
        int served = 0;
        long deadline = System.currentTimeMillis() + 20_000;
        try {
            while (leaked == null && System.currentTimeMillis() < deadline) {
                MsgEvent m = r.call("getfile", "file_path", pub.toString());
                if ("10".equals(m.getParam("status"))) {
                    served++;
                    String got = new String(m.getDataParam("file_data"));
                    if (got.contains("SECRET")) leaked = got;
                }
            }
        } finally {
            stop.set(true);
            flipper.join(2000);
        }
        assertNull(leaked, "getfile of a cataloged repository file returned " + leaked + " from " + secretDir
                + " (" + served + " served)");
    }
}
