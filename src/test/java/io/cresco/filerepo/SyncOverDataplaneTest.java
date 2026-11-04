package io.cresco.filerepo;

import io.cresco.library.messaging.MsgEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import jakarta.jms.Message;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A producer and a subscriber on one stand-in fabric, syncing for real: discover and subscribe over the dataplane,
 * paged offers, files pulled with the producer's own streamfile over the dataplane (FR-D7), every file verified
 * (SHA-384 from the terminal frame and the offered content hash) before it is committed, deletes propagated with the
 * mass-delete guard (FR-D4), and the bounded queues refusing as busy (FR-D5).
 */
class SyncOverDataplaneTest {

    @TempDir Path tmp;
    final Fabric fabric = new Fabric();
    TestRig producer, subscriber;
    Path scan, mirror;
    final List<TestRig> extra = new ArrayList<>();

    @BeforeEach
    void up() throws Exception {
        tmp = tmp.toRealPath();
        scan = Files.createDirectories(tmp.resolve("scan"));
        mirror = Files.createDirectories(tmp.resolve("mirror"));
    }

    @AfterEach
    void down() {
        for (TestRig r : extra) r.close();
        if (subscriber != null) subscriber.close();
        if (producer != null) producer.close();
    }

    TestRig producer(String... kv) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("filerepo_name", "fr");
        cfg.put("scan_dir", scan.toString());
        cfg.put("enable_scan", "true");
        cfg.put("scan_delay", "3600000");
        cfg.put("scan_period", "3600000");
        for (int i = 0; i + 1 < kv.length; i += 2) cfg.put(kv[i], kv[i + 1]);
        producer = new TestRig(tmp.resolve("agent"), cfg, null, "plugin-p", fabric);
        producer.repo.start();
        return producer;
    }

    TestRig subscriber(String... kv) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("filerepo_name", "fr");
        cfg.put("repo_dir", mirror.toString());
        cfg.put("sync_stream_buffer", "4096");
        for (int i = 0; i + 1 < kv.length; i += 2) cfg.put(kv[i], kv[i + 1]);
        subscriber = new TestRig(tmp.resolve("agent"), cfg, null, "plugin-s", fabric);
        subscriber.repo.start();
        return subscriber;
    }

    /** Discover and subscribe, as the broadcast timer and the subscriber's answer do. */
    void subscribe() throws Exception {
        producer.repo.repoBroadcast("fr", "discover", 0);
        await(() -> producer.repo.isSubscriber(TestRig.REGION, TestRig.AGENT, "plugin-s"), "the subscriber subscribed");
    }

    static void await(BooleanSupplier cond, String what) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) fail("timed out waiting: " + what);
            Thread.sleep(20);
        }
    }

    static byte[] random(int n, long seed) {
        byte[] b = new byte[n];
        new Random(seed).nextBytes(b);
        return b;
    }

    boolean allConfirmed() {
        return producer.db.getFilesNotInSync().isEmpty();
    }

    void syncedAndConfirmed() throws Exception {
        assertTrue(producer.repo.scanOnce());
        await(this::allConfirmed, "every offered file confirmed");
    }

    // ------------------------------------------------------------------ adds and updates

    @Test
    void filesSyncOverTheDataplaneVerifiedAndNestedDirectoriesKept() throws Exception {
        producer();
        subscriber();
        subscribe();
        byte[] big = random(100_000, 1);
        Files.write(scan.resolve("big.bin"), big);
        Files.write(Files.createDirectories(scan.resolve("sub/deeper")).resolve("nested.txt"), "nested".getBytes());
        Files.write(scan.resolve("empty.txt"), new byte[0]);

        syncedAndConfirmed();

        assertArrayEquals(big, Files.readAllBytes(mirror.resolve("big.bin")));
        assertEquals("nested", Files.readString(mirror.resolve("sub/deeper/nested.txt")));
        assertEquals(0, Files.size(mirror.resolve("empty.txt")));
        // the bytes rode the dataplane: streamfile requests, never getfiledata/getfileinfo
        assertTrue(fabric.routed.contains("streamfile"));
        assertFalse(fabric.routed.contains("getfiledata"));
        assertFalse(fabric.routed.contains("getfileinfo"));
        assertTrue(subscriber.downloads.isEmpty(), "the control-plane download path is not used");
        assertEquals(3, subscriber.repo.syncFetched.get());
        assertNoStaging(mirror);

        // a change is offered and fetched again; an unchanged file is not
        Files.write(scan.resolve("big.bin"), random(100_000, 2));
        Files.setLastModifiedTime(scan.resolve("big.bin"), java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() + 5000));
        syncedAndConfirmed();
        assertArrayEquals(Files.readAllBytes(scan.resolve("big.bin")), Files.readAllBytes(mirror.resolve("big.bin")));
        assertEquals(4, subscriber.repo.syncFetched.get());
    }

    @Test
    void aLostFrameIsAskedForAgainAndTheFileIsStillExact() throws Exception {
        producer();
        subscriber();
        subscribe();
        byte[] data = random(40_000, 3);
        Files.write(scan.resolve("lossy.bin"), data);
        AtomicBoolean dropped = new AtomicBoolean();
        fabric.drop = m -> isData(m, "4096") && dropped.compareAndSet(false, true);

        syncedAndConfirmed();

        assertTrue(dropped.get());
        assertArrayEquals(data, Files.readAllBytes(mirror.resolve("lossy.bin")));
        long rangeRequests = producer.sent.size() >= 0 ? subscriber.sent.stream()
                .filter(m -> "streamfile".equals(m.getParam("action")) && !"0".equals(m.getParam("start_byte"))).count() : 0;
        assertEquals(1, rangeRequests, "exactly the missing range was asked for again");
    }

    @Test
    void aCorruptedFrameIsNeverCommittedAndTheNextOfferRepairsIt() throws Exception {
        producer();
        subscriber();
        subscribe();
        byte[] data = random(20_000, 4);
        Files.write(scan.resolve("tampered.bin"), data);
        AtomicBoolean tampered = new AtomicBoolean();
        fabric.drop = m -> {
            if (isData(m, "8192") && tampered.compareAndSet(false, true)) {
                FakeJms.State s = FakeJms.state(m);
                byte[] body = s.written.toByteArray();
                body[17] ^= 0x5a;
                s.written.reset();
                s.written.write(body, 0, body.length);
                s.body = null;
            }
            return false;
        };
        assertTrue(producer.repo.scanOnce());
        await(() -> subscriber.repo.syncVerifyFailed.get() == 1, "the corrupt file refused");
        assertFalse(Files.exists(mirror.resolve("tampered.bin")), "never visible under its name");
        assertNoStaging(mirror);
        await(() -> subscriber.sent.stream().anyMatch(m -> "repoconfirm".equals(m.getParam("action"))
                && m.getCompressedParam("failedfiles") != null), "reported failed");
        assertFalse(allConfirmed(), "the producer keeps it unconfirmed");

        // the next cycle offers it again (the failed confirm cleared its in-flight mark) and it arrives exact
        syncedAndConfirmed();
        assertArrayEquals(data, Files.readAllBytes(mirror.resolve("tampered.bin")));
    }

    @Test
    void aLostTerminalFrameStallsAndTheWholeFileIsFetchedAgain() throws Exception {
        producer();
        subscriber("sync_stream_idle_ms", "300");
        subscribe();
        byte[] data = random(10_000, 5);
        Files.write(scan.resolve("noend.bin"), data);
        AtomicBoolean dropped = new AtomicBoolean();
        fabric.drop = m -> isTerminal(m) && dropped.compareAndSet(false, true);
        syncedAndConfirmed();
        assertArrayEquals(data, Files.readAllBytes(mirror.resolve("noend.bin")));
        assertTrue(fabric.routed.contains("streamfilecancel"), "the stalled transfer was cancelled");
    }

    @Test
    void aFileChangedSinceTheOfferIsRefusedNotCommitted() throws Exception {
        producer();
        subscriber();
        subscribe();
        Path f = Files.write(scan.resolve("moving.txt"), "version one".getBytes());
        // the catalog takes version one; then the file changes, same size, before the subscriber fetches
        subscriber.repo.beforeApplyDiff = () -> {
            try {
                Files.write(f, "version two".getBytes());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        assertTrue(producer.repo.scanOnce());
        // refused by the offered content hash at the mirror; or, where the producer proves what it opened by its
        // content (no descriptor-relative open: macOS), already refused by the producer
        await(() -> subscriber.repo.syncVerifyFailed.get() == 1 || subscriber.repo.syncFetchFailed.get() >= 1,
                "refused by the offered content hash");
        assertFalse(Files.exists(mirror.resolve("moving.txt")));
    }

    @Test
    void offersArePagedAndEachPageConfirmed() throws Exception {
        producer("sync_diff_page", "2");
        subscriber();
        subscribe();
        for (int i = 0; i < 5; i++) Files.write(scan.resolve("f" + i + ".txt"), ("file " + i).getBytes());
        syncedAndConfirmed();
        assertEquals(3, fabric.routed.stream().filter("repolistin"::equals).count(), "5 files in pages of 2");
        for (int i = 0; i < 5; i++) assertEquals("file " + i, Files.readString(mirror.resolve("f" + i + ".txt")));
        // nothing unconfirmed is offered again while confirmed
        assertTrue(producer.repo.scanOnce());
        assertEquals(3, fabric.routed.stream().filter("repolistin"::equals).count());
    }

    @Test
    void aSubscriberThatJoinsLateReceivesWhatOthersAlreadyConfirmed() throws Exception {
        producer("sync_diff_page", "2", "sync_full_pages_per_cycle", "1");
        subscriber();
        subscribe();
        for (int i = 0; i < 5; i++) Files.write(scan.resolve("early" + i + ".txt"), ("early " + i).getBytes());
        syncedAndConfirmed();

        // a second mirror subscribes once everything is confirmed: the insync flags alone would never offer it again
        Path mirror2 = Files.createDirectories(tmp.resolve("mirror2"));
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("filerepo_name", "fr");
        cfg.put("repo_dir", mirror2.toString());
        TestRig late = new TestRig(tmp.resolve("agent"), cfg, null, "plugin-late", fabric);
        extra.add(late);
        late.repo.start();
        producer.repo.repoBroadcast("fr", "discover", 0);
        await(() -> producer.repo.isSubscriber(TestRig.REGION, TestRig.AGENT, "plugin-late"), "the late subscriber");

        // paced: one page of two files per cycle
        int cycles = 0;
        while (cycles < 10 && !(Files.exists(mirror2.resolve("early4.txt")) && Files.exists(mirror2.resolve("early0.txt")))) {
            assertTrue(producer.repo.scanOnce());
            cycles++;
            Thread.sleep(150);
        }
        for (int i = 0; i < 5; i++) {
            Path f = mirror2.resolve("early" + i + ".txt");
            await(() -> Files.exists(f), "early file at the late subscriber");
            assertEquals("early " + i, Files.readString(f));
        }
        assertTrue(cycles >= 3, "five files in pages of two, one page a cycle: " + cycles);
        // the first mirror was not sent them again
        assertEquals(5, subscriber.repo.syncFetched.get());
    }

    @Test
    void aClearedMirrorRefillsFromItsProducer() throws Exception {
        producer();
        subscriber();
        subscribe();
        for (int i = 0; i < 3; i++) Files.write(scan.resolve("r" + i + ".txt"), ("refill " + i).getBytes());
        syncedAndConfirmed();
        String gen = subscriber.repo.getSyncGen();
        MsgEvent c = subscriber.call(TestRig.as(subscriber.requestTo("clearrepo", "repo_name", "fr", "confirm", mirror.toString()), "admin"));
        assertEquals("10", c.getParam("status"), c.getParams().toString());
        assertFalse(Files.exists(mirror.resolve("r0.txt")));
        assertNotEquals(gen, subscriber.repo.getSyncGen(), "a clear starts a new generation");
        subscribe();
        producer.repo.repoBroadcast("fr", "discover", 0);
        Thread.sleep(100);
        assertTrue(producer.repo.scanOnce());
        for (int i = 0; i < 3; i++) {
            Path f = mirror.resolve("r" + i + ".txt");
            await(() -> Files.exists(f), "refilled");
            assertEquals("refill " + i, Files.readString(f));
        }
    }

    // ------------------------------------------------------------------ deletes (FR-D4)

    @Test
    void aDeleteAtTheProducerDeletesTheSubscribersCopyAndIsConfirmed() throws Exception {
        producer();
        subscriber();
        subscribe();
        for (int i = 0; i < 4; i++) Files.write(scan.resolve("d" + i + ".txt"), ("keep " + i).getBytes());
        syncedAndConfirmed();
        assertTrue(Files.exists(mirror.resolve("d1.txt")));

        Files.delete(scan.resolve("d1.txt"));
        assertTrue(producer.repo.scanOnce());
        await(() -> !Files.exists(mirror.resolve("d1.txt")), "the subscriber's copy deleted");
        await(() -> producer.db.getTombstonesNotInSync(10).isEmpty(), "the delete confirmed");
        assertNull(producer.repo.getFileInfo(scan.resolve("d1.txt").toString()), "no ghost row");
        assertEquals(3, producer.repo.getRepoCount());
        assertTrue(Files.exists(mirror.resolve("d0.txt")));
        assertEquals(1, subscriber.repo.syncDeleted.get());
    }

    @Test
    void removefileOnAProducerIsADeleteForItsSubscribers() throws Exception {
        producer();
        subscriber();
        subscribe();
        Files.write(scan.resolve("withdrawn.jar"), "withdrawn".getBytes());
        syncedAndConfirmed();
        MsgEvent r = producer.call(TestRig.as(producer.requestTo("removefile", "repo_name", "fr", "file_name", "withdrawn.jar"), "admin"));
        assertEquals("10", r.getParam("status"));
        assertEquals(1, producer.db.getTombstonesNotInSync(10).size());
        assertTrue(producer.repo.scanOnce());
        await(() -> !Files.exists(mirror.resolve("withdrawn.jar")), "the withdrawn file left the mirror too");
    }

    @Test
    void aSubscriberNeverDeletesContentTheProducerDidNotHave() throws Exception {
        producer();
        subscriber();
        subscribe();
        Files.write(scan.resolve("same-name.txt"), "producer's".getBytes());
        syncedAndConfirmed();
        // the subscriber's copy is replaced by other content (landed there directly)
        Files.write(mirror.resolve("same-name.txt"), "subscriber's own".getBytes());
        Files.delete(scan.resolve("same-name.txt"));
        assertTrue(producer.repo.scanOnce());
        await(() -> producer.db.getTombstonesNotInSync(10).isEmpty(), "the delete settled");
        assertEquals("subscriber's own", Files.readString(mirror.resolve("same-name.txt")));
    }

    @Test
    void theMassDeleteGuardHoldsAnEmptiedScanDirUntilAnAdminReleasesIt() throws Exception {
        producer();
        subscriber();
        subscribe();
        for (int i = 0; i < 6; i++) Files.write(scan.resolve("m" + i + ".txt"), ("m" + i).getBytes());
        syncedAndConfirmed();

        // the scan dir is suddenly empty (an unmount looks like this)
        for (int i = 0; i < 6; i++) Files.delete(scan.resolve("m" + i + ".txt"));
        assertTrue(producer.repo.scanOnce());
        assertEquals(6, producer.repo.getDeletesHeld());
        assertEquals(0, producer.repo.getTombstoneCount(), "nothing recorded");
        assertEquals(6, producer.repo.getRepoCount(), "the catalog keeps the rows");
        assertEquals("6", producer.call("repostate").getParam("deletes_held"));
        for (int i = 0; i < 6; i++) assertTrue(Files.exists(mirror.resolve("m" + i + ".txt")));
        assertTrue(producer.repo.scanOnce());
        assertEquals(6, producer.repo.getDeletesHeld(), "held every cycle, not released by waiting");

        // released: refused without the exact directory, then confirmed
        assertEquals("unconfirmed", producer.call(producer.requestTo("releasedeletes", "confirm", "/elsewhere")).getParam("refusal"));
        assertEquals("hold-id-required", producer.call(producer.requestTo("releasedeletes", "confirm", scan.toString()))
                .getParam("refusal"), "a release names the set it releases");
        MsgEvent rel = producer.call(producer.requestTo("releasedeletes", "confirm", scan.toString(),
                "hold_id", producer.repo.getHoldId()));
        assertEquals("10", rel.getParam("status"));
        assertEquals("6", rel.getParam("released"));
        assertTrue(producer.repo.scanOnce());
        assertEquals(0, producer.repo.getDeletesHeld());
        await(() -> producer.db.getTombstonesNotInSync(10).isEmpty() && producer.repo.getTombstoneCount() == 6, "applied");
        for (int i = 0; i < 6; i++) assertFalse(Files.exists(mirror.resolve("m" + i + ".txt")));
        assertEquals("9", producer.call(producer.requestTo("releasedeletes", "confirm", scan.toString(), "hold_id", "0".repeat(32)))
                .getParam("status"), "nothing held, nothing released");
    }

    @Test
    void theGuardHoldsALargeFractionButNotAFewDeletes() throws Exception {
        producer("sync_delete_guard_min", "5", "sync_delete_guard_fraction", "0.5");
        for (int i = 0; i < 12; i++) Files.write(scan.resolve("g" + i + ".txt"), ("g" + i).getBytes());
        assertTrue(producer.repo.scanOnce());
        Files.delete(scan.resolve("g0.txt"));
        Files.delete(scan.resolve("g1.txt"));
        assertTrue(producer.repo.scanOnce());
        assertEquals(0, producer.repo.getDeletesHeld(), "two of twelve: recorded");
        assertEquals(2, producer.repo.getTombstoneCount());
        for (int i = 2; i < 9; i++) Files.delete(scan.resolve("g" + i + ".txt"));
        assertTrue(producer.repo.scanOnce());
        assertEquals(7, producer.repo.getDeletesHeld(), "seven of ten (over half, at least five): held");
        // the files come back: nothing is held any more
        for (int i = 2; i < 9; i++) Files.write(scan.resolve("g" + i + ".txt"), ("g" + i).getBytes());
        assertTrue(producer.repo.scanOnce());
        assertEquals(0, producer.repo.getDeletesHeld());
    }

    @Test
    void anIncompleteScanTakesNoFileForDeleted() throws Exception {
        producer();
        Path locked = Files.createDirectories(scan.resolve("locked"));
        Files.write(locked.resolve("inside.txt"), "x".getBytes());
        Files.write(scan.resolve("outside.txt"), "y".getBytes());
        assertTrue(producer.repo.scanOnce());
        assertEquals(2, producer.repo.getRepoCount());
        try {
            assertTrue(locked.toFile().setReadable(false) && locked.toFile().setExecutable(false));
            Files.delete(scan.resolve("outside.txt"));
            assertTrue(producer.repo.scanOnce());
            assertEquals(0, producer.repo.getTombstoneCount(), "a scan that could not read part of the tree deletes nothing");
            assertEquals(2, producer.repo.getRepoCount());
        } finally {
            locked.toFile().setReadable(true);
            locked.toFile().setExecutable(true);
        }
        assertTrue(producer.repo.scanOnce());
        assertEquals(1, producer.repo.getTombstoneCount(), "once readable, the delete is seen");
    }

    @Test
    void aVanishedMountIsHeldEvenForASingleFileAndAReleaseNamesExactlyTheHeldSet() throws Exception {
        producer();
        subscriber();
        subscribe();
        // vol is a mount point: another device than its parent (simulated device ids)
        java.util.concurrent.atomic.AtomicBoolean mounted = new java.util.concurrent.atomic.AtomicBoolean(true);
        producer.repo.devOf = p -> p.getFileName() != null && p.getFileName().toString().equals("vol") && mounted.get() ? 99L : 1L;
        for (int i = 0; i < 20; i++) Files.write(scan.resolve("keep" + i + ".txt"), ("keep " + i).getBytes());
        Files.write(Files.createDirectories(scan.resolve("vol")).resolve("ckpt.bin"), "checkpoint".getBytes());
        syncedAndConfirmed();
        assertTrue(Files.exists(mirror.resolve("vol/ckpt.bin")));

        // unmounted: the mount point is an ordinary directory again, its one file gone (1 of 21: no fraction, no subtree)
        mounted.set(false);
        Files.delete(scan.resolve("vol/ckpt.bin"));
        assertTrue(producer.repo.scanOnce());
        assertEquals(1, producer.repo.getDeletesHeld(), "a delete under a vanished mount is held");
        Thread.sleep(300);
        assertTrue(Files.exists(mirror.resolve("vol/ckpt.bin")), "never propagated");
        String holdId = producer.call("repostate").getParam("deletes_hold_id");
        assertNotNull(holdId);
        assertTrue(producer.repo.scanOnce());
        assertEquals(1, producer.repo.getDeletesHeld(), "held every cycle");

        // a release names exactly that set: a stale id is refused, the right one releases it once
        assertEquals("hold-changed", producer.call(producer.requestTo("releasedeletes", "confirm", scan.toString(),
                "hold_id", "0123456789abcdef0123456789abcdef")).getParam("refusal"));
        assertEquals("1", producer.call(producer.requestTo("releasedeletes", "confirm", scan.toString(), "hold_id", holdId))
                .getParam("released"));
        assertTrue(producer.repo.scanOnce());
        assertEquals(0, producer.repo.getDeletesHeld());
        await(() -> !Files.exists(mirror.resolve("vol/ckpt.bin")), "released and applied");
    }

    @Test
    void theScanDirLeavingItsDeviceHoldsEverythingGone() throws Exception {
        producer();
        java.util.concurrent.atomic.AtomicLong rootDev = new java.util.concurrent.atomic.AtomicLong(7);
        producer.repo.devOf = p -> p.equals(scan) || p.equals(scanReal()) ? rootDev.get() : 7L;
        for (int i = 0; i < 30; i++) Files.write(scan.resolve("f" + i + ".txt"), ("f" + i).getBytes());
        assertTrue(producer.repo.scanOnce());
        // the volume under the scan dir is replaced by another (a remount of the wrong device), one file missing
        rootDev.set(8);
        Files.delete(scan.resolve("f0.txt"));
        assertTrue(producer.repo.scanOnce());
        assertEquals(1, producer.repo.getDeletesHeld(), "anything gone from a scan dir that changed device is held");
        assertEquals(0, producer.repo.getTombstoneCount());
    }

    private Path scanReal() {
        try {
            return scan.toRealPath();
        } catch (Exception e) {
            return scan;
        }
    }

    @Test
    void peersOfAnotherSyncProtocolAreRefusedAndToldWhy() throws Exception {
        producer();
        subscriber();
        // an old producer's discover (no sync_proto): the subscriber does not subscribe to it
        Map<String, String> old = new HashMap<>();
        old.put("action", "discover");
        old.put("filerepo_name", "fr");
        old.put("repo_region_id", TestRig.REGION);
        old.put("repo_agent_id", TestRig.AGENT);
        old.put("repo_plugin_id", "plugin-p");
        subscriber.repo.onSubscriberMessage(old);
        Thread.sleep(200);
        assertFalse(producer.repo.isSubscriber(TestRig.REGION, TestRig.AGENT, "plugin-s"));
        assertTrue(subscriber.log.stream().anyMatch(l -> l.contains("speaks sync protocol 1")), subscriber.log.toString());
        // an old subscriber's reposubscribe (sync_proto 1): refused, with the reason in the answer
        MsgEvent sub = new MsgEvent(MsgEvent.Type.EXEC, TestRig.REGION, TestRig.AGENT, "plugin-old", TestRig.REGION,
                TestRig.AGENT, "plugin-p", false, false);
        sub.setParam("action", "reposubscribe");
        sub.setParam("filerepo_name", "fr");
        sub.setParam("sync_gen", "abc");
        sub.setParam("sync_proto", "1");
        Fabric.stamp(sub, "plugin-old");
        producer.call(sub);
        assertFalse(producer.repo.isSubscriber(TestRig.REGION, TestRig.AGENT, "plugin-old"));
        MsgEvent answer = producer.sent.stream().filter(m -> "reposubscribed".equals(m.getParam("action"))).reduce((a, b) -> b).orElse(null);
        assertNotNull(answer);
        assertEquals("false", answer.getParam("accepted"));
        assertTrue(answer.getParam("refusal").contains("sync protocol 1"), answer.getParam("refusal"));
        // a protocol 1 subscribe over the dataplane is never taken
        producer.repo.onRepoMessage(Map.of("action", "subscribe", "filerepo_name", "fr", "sub_region_id", TestRig.REGION,
                "sub_agent_id", TestRig.AGENT, "sub_plugin_id", "plugin-old"));
        assertFalse(producer.repo.isSubscriber(TestRig.REGION, TestRig.AGENT, "plugin-old"));
        assertTrue(producer.log.stream().anyMatch(l -> l.contains("sync protocol 1")), producer.log.toString());
    }

    @Test
    void aMirrorPinnedToItsProducersSubscribesToNoOther() throws Exception {
        producer();
        subscriber("sync_producers", TestRig.REGION + "/" + TestRig.AGENT + "/plugin-elsewhere");
        producer.repo.repoBroadcast("fr", "discover", 0);
        Thread.sleep(300);
        assertFalse(producer.repo.isSubscriber(TestRig.REGION, TestRig.AGENT, "plugin-s"), "plugin-p is not a pinned producer");
    }

    // ------------------------------------------------------------------ sources and bounds (FR-D5)

    @Test
    void aDiffFromAnAgentThatIsNotASubscribedProducerIsRefused() throws Exception {
        subscriber();
        MsgEvent in = subscriber.requestTo("repolistin", "transfer_id", "1");
        in.setCompressedParam("repolistin", "{\"/etc/passwd\":{\"fileName\":\"x\",\"MD5\":\"a\",\"filePath\":\"/etc/passwd\",\"lastModified\":1,\"filesize\":3}}");
        assertEquals("9", subscriber.call(in).getParam("status_code"), "no verified caller at all");
        MsgEvent in2 = subscriber.requestTo("repolistin", "transfer_id", "2");
        in2.setCompressedParam("repolistin", "{}");
        Fabric.stamp(in2, "plugin-x");      // a verified plugin, but not a producer this mirror subscribed to
        MsgEvent r = subscriber.call(in2);
        assertEquals("9", r.getParam("status_code"));
        Thread.sleep(200);
        assertTrue(subscriber.sent.isEmpty(), "nothing fetched, nothing confirmed");
        assertEquals(1, subscriber.repo.syncRefused.get());
    }

    @Test
    void aConfirmFromANonSubscriberIsIgnored() throws Exception {
        producer("sync_rpc_timeout_ms", "300");
        subscriber();
        subscribe();
        fabric.dropRoute = m -> "repolistin".equals(m.getParam("action"));   // the real subscriber never hears it
        Files.write(scan.resolve("c.txt"), "c".getBytes());
        assertTrue(producer.repo.scanOnce());
        assertFalse(allConfirmed());
        // someone else claims to hold every file of every recent transfer
        for (int t = 1; t <= 3; t++) {
            MsgEvent forged = new MsgEvent(MsgEvent.Type.EXEC, TestRig.REGION, TestRig.AGENT, "intruder",
                    TestRig.REGION, TestRig.AGENT, "plugin-p", false, false);
            forged.setParam("action", "repoconfirm");
            forged.setParam("transfer_id", String.valueOf(t));
            producer.call(forged);
        }
        assertFalse(allConfirmed(), "still offered: no subscriber confirmed it");
    }

    @Test
    void subscribersThatWentAwayNeverStallTheOthersForMoreThanOneTimeout() throws Exception {
        producer("sync_rpc_timeout_ms", "1500");
        subscriber();
        for (String id : new String[]{"plugin-d1", "plugin-d2", "plugin-d3"}) {
            Map<String, Object> cfg = new HashMap<>();
            cfg.put("filerepo_name", "fr");
            cfg.put("repo_dir", Files.createDirectories(tmp.resolve("mirror-" + id)).toString());
            TestRig r = new TestRig(tmp.resolve("agent"), cfg, null, id, fabric);
            r.repo.start();
            extra.add(r);
        }
        producer.repo.repoBroadcast("fr", "discover", 0);
        for (String id : new String[]{"plugin-s", "plugin-d1", "plugin-d2", "plugin-d3"}) {
            await(() -> producer.repo.isSubscriber(TestRig.REGION, TestRig.AGENT, id), id + " subscribed");
        }
        // three subscribers go away: their offers are never answered
        fabric.dropRoute = m -> "repolistin".equals(m.getParam("action")) && m.getDstPlugin().startsWith("plugin-d");
        Files.write(scan.resolve("a.txt"), "a".getBytes());
        long t0 = System.nanoTime();
        assertTrue(producer.repo.scanOnce());
        long ms = (System.nanoTime() - t0) / 1_000_000;
        // one after the other they would cost 3 x 1.5 s; concurrently, the slowest one
        assertTrue(ms < 2_900, "the cycle took " + ms + " ms: the unanswered offers ran one after the other");
        await(() -> Files.isRegularFile(mirror.resolve("a.txt")), "the subscriber still there received a.txt");
        // the next cycle skips them (until they subscribe again) and costs nothing like a timeout
        Files.write(scan.resolve("b.txt"), "b".getBytes());
        t0 = System.nanoTime();
        assertTrue(producer.repo.scanOnce());
        ms = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(ms < 1_000, "the next cycle took " + ms + " ms");
        await(() -> Files.isRegularFile(mirror.resolve("b.txt")), "b.txt arrived");
    }

    @Test
    void diffsBeyondTheBoundsAreRefusedAsBusy() throws Exception {
        subscriber("sync_pending_per_peer", "1", "sync_threads", "1", "sync_queue_max", "1");
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger entered = new AtomicInteger();
        subscriber.repo.beforeApplyDiff = () -> {
            entered.incrementAndGet();
            try {
                release.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
        };
        for (String p : new String[]{"pA", "pB", "pC"}) subscriber.repo.rememberProducer(TestRig.REGION, TestRig.AGENT, p);
        // producer A: its updater runs (blocked), one more diff waits, the next is busy
        assertEquals("10", diff("pA", "1"));
        await(() -> entered.get() == 1, "A's updater running");
        assertEquals("10", diff("pA", "2"));
        assertEquals("11", diff("pA", "3"), "one waiting per producer");
        // producer B's updater waits in the pool's queue (one thread); producer C finds the pool full
        assertEquals("10", diff("pB", "1"));
        assertEquals("11", diff("pC", "1"), "the pool's queue is full");
        assertEquals(2, subscriber.repo.syncRejected.get());
        release.countDown();
        await(() -> subscriber.repo.getSyncQueued() == 0 && subscriber.repo.getSyncActive() == 0, "drained");
        assertEquals("10", diff("pC", "2"), "accepted once there is room");
    }

    private String diff(String producerId, String tid) {
        MsgEvent in = new MsgEvent(MsgEvent.Type.EXEC, TestRig.REGION, TestRig.AGENT, producerId,
                TestRig.REGION, TestRig.AGENT, "plugin-s", false, false);
        in.setParam("action", "repolistin");
        in.setParam("transfer_id", tid);
        in.setParam("sync_gen", subscriber.repo.getSyncGen());
        in.setCompressedParam("repolistin", "{}");
        Fabric.stamp(in, producerId);
        return subscriber.call(in).getParam("status_code");
    }

    // ------------------------------------------------------------------ helpers

    static boolean isData(Message m, String offset) {
        try {
            return !"true".equals(m.getStringProperty("transfer_end")) && offset.equals(m.getStringProperty("offset"))
                    && m.getStringProperty(SyncReceiver.IDENT_KEY) != null;
        } catch (Exception e) {
            return false;
        }
    }

    static boolean isTerminal(Message m) {
        try {
            return "true".equals(m.getStringProperty("transfer_end")) && m.getStringProperty(SyncReceiver.IDENT_KEY) != null;
        } catch (Exception e) {
            return false;
        }
    }

    static void assertNoStaging(Path dir) throws Exception {
        try (java.util.stream.Stream<Path> s = Files.walk(dir)) {
            // the owner-only staging directory may stay; no staging file may, in it or anywhere
            assertTrue(s.noneMatch(p -> !p.getFileName().toString().equals(RepoEngine.PRIVATE_STAGING)
                    && RepoEngine.isStaging(p.getFileName().toString())), "no staging file is left");
        }
    }
}
