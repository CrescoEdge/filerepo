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
 * The round-5 design beyond the review's own tests: every delete counts against the day and the total (additions never
 * enlarge their base, and join it once a day old), rolling retention, a bounded ledger, candidates by first seen.
 */
class SyncRound5Test {

    static final long DAY = DeleteGuard.DAY_MS;
    static final long HOUR = 3_600_000L;

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

    TestRig producer(String pluginId, Path dir, String... kv) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("filerepo_name", "fr");
        cfg.put("scan_dir", dir.toString());
        cfg.put("scan_delay", "3600000");
        cfg.put("scan_period", "3600000");
        // the short window off: these tests are about the longer budgets
        cfg.put("sync_delete_guard_window_ms", "1");
        for (int i = 0; i + 1 < kv.length; i += 2) cfg.put(kv[i], kv[i + 1]);
        TestRig r = new TestRig(tmp.resolve("agent"), cfg, null, pluginId, fabric);
        rigs.add(r);
        r.repo.start();
        return r;
    }

    TestRig subscriber(String pluginId, Path dir, String... kv) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("filerepo_name", "fr");
        cfg.put("repo_dir", dir.toString());
        cfg.put("sync_stream_buffer", "4096");
        for (int i = 0; i + 1 < kv.length; i += 2) cfg.put(kv[i], kv[i + 1]);
        TestRig r = new TestRig(tmp.resolve("agent"), cfg, null, pluginId, fabric);
        rigs.add(r);
        r.repo.start();
        return r;
    }

    static void await(BooleanSupplier cond, String what) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) fail("timed out waiting: " + what);
            Thread.sleep(20);
        }
    }

    static void write(Path f, String s) throws Exception {
        Files.createDirectories(f.getParent());
        Files.write(f, s.getBytes());
    }

    static void scanned(TestRig p) throws Exception {
        Thread.sleep(3);
        assertTrue(p.repo.scanOnce());
    }

    // =================================================================== every delete counts; the base never grows by additions

    @Test
    void deletesOfFilesAddedTodayCountAndAdditionsNeverEnlargeTheBase() throws Exception {
        TestRig p = producer("plugin-p", scan);
        for (int i = 0; i < 40; i++) write(scan.resolve("old" + i), "old " + i);
        scanned(p);
        // a thousand files arrive: they do not enlarge the day's base (40) ...
        for (int i = 0; i < 1000; i++) write(scan.resolve("junk/j" + i), "junk " + i);
        scanned(p);
        // ... and deleting them counts: 11 of the files added today are more than a quarter of the day's base
        for (int i = 0; i < 11; i++) Files.delete(scan.resolve("junk/j" + i));
        scanned(p);
        assertEquals(11, p.repo.getDeletesHeld(), "deletes of files added today were not counted, or the additions diluted the base");
        // the same for originals, x replaced by x.locked
        assertTrue(p.repo.releaseHeldDeletes(p.repo.getHoldId()) > 0);
        scanned(p);
        assertEquals(0, p.repo.getDeletesHeld());
        TestRig q = producer("plugin-q", Files.createDirectories(tmp.resolve("q")));
        for (int i = 0; i < 40; i++) write(tmp.resolve("q/doc" + i), "doc " + i);
        scanned(q);
        for (int i = 0; i < 1000; i++) write(tmp.resolve("q/junk/j" + i), "junk " + i);
        scanned(q);
        for (int i = 0; i < 11; i++) {
            Files.delete(tmp.resolve("q/doc" + i));
            write(tmp.resolve("q/doc" + i + ".locked"), "ENCRYPTED");
        }
        scanned(q);
        assertEquals(11, q.repo.getDeletesHeld(), "a thousand additions diluted the day's budget");
    }

    @Test
    void filesAddedSinceTheLastReleaseJoinTheBaseOnceADayOld() throws Exception {
        TestRig p = producer("plugin-p", scan);
        for (int i = 0; i < 10; i++) write(scan.resolve("old" + i), "old " + i);
        scanned(p);
        for (int i = 0; i < 100; i++) write(scan.resolve("new" + i), "new " + i);
        scanned(p);
        // a day later the hundred are part of the day's and the total's base (110): 20 deletes are ordinary
        p.repo.ledger().advance(DAY + HOUR);
        int next = 0;
        for (int c = 0; c < 4; c++) {
            for (int i = 0; i < 5; i++) Files.delete(scan.resolve("new" + (next++)));
            scanned(p);
            assertEquals(0, p.repo.getDeletesHeld(), "after " + next);
        }
        // and past a quarter of them within the day (20 + 8 > 27.5), they are held
        for (int i = 0; i < 8; i++) Files.delete(scan.resolve("new" + (next++)));
        scanned(p);
        assertEquals(8, p.repo.getDeletesHeld());
    }

    @Test
    void aDripOverManyDaysIsHeldByTheTotalAndTheLedgerStaysADayLong() throws Exception {
        TestRig p = producer("plugin-p", scan, "sync_delete_guard_fraction", "0.5");
        for (int i = 0; i < 100; i++) write(scan.resolve("f" + i), "f " + i);
        scanned(p);
        int next = 0, days = 0;
        // 20 a day (the window and the day raised to half: only the total can catch it): the ledger folds each day's
        // records, and the total still sees all of them
        while (p.repo.getDeletesHeld() == 0 && days < 10) {
            for (int i = 0; i < 20 && next < 100; i++) Files.delete(scan.resolve("f" + (next++)));
            scanned(p);
            assertTrue(p.db.guardRows() <= 20, "the ledger keeps a day of records: " + p.db.guardRows());
            if (p.repo.getDeletesHeld() == 0) {
                p.repo.ledger().advance(DAY + HOUR);
                scanned(p);
                days++;
            }
        }
        assertTrue(p.repo.getDeletesHeld() > 0, "a drip of 20 a day for " + days + " days was never held");
        assertTrue(p.repo.getTombstoneCount() <= 50, "deleted before the hold: " + p.repo.getTombstoneCount());
        assertEquals(40, p.repo.ledger().folded('D', GuardLedger.DELETE), "folded: the days before the last");
        // a restart keeps the folded counts
        p.close();
        rigs.remove(p);
        TestRig again = producer("plugin-p", scan, "sync_delete_guard_fraction", "0.5");
        assertEquals(40, again.repo.ledger().folded('D', GuardLedger.DELETE));
        scanned(again);
        assertTrue(again.repo.getDeletesHeld() > 0);
        // a release of the total drops what it had folded
        assertTrue(again.repo.releaseHeldDeletes(again.repo.getHoldId()) > 0);
        scanned(again);
        assertEquals(0, again.repo.ledger().folded('D', GuardLedger.DELETE));
    }

    // =================================================================== rolling retention

    @Test
    void aRollingRetentionRepositoryNeedsNoReleaseWhileRecentFilesStayGuarded() throws Exception {
        // keep three days; each "hour" a dated directory of 10 files arrives, and directories over three days go
        TestRig p = producer("plugin-p", scan, "sync_retention_days", "3");
        TestRig s = subscriber("plugin-s", mirror);
        p.repo.repoBroadcast("fr", "discover", 0);
        await(() -> p.repo.isSubscriber(TestRig.REGION, TestRig.AGENT, "plugin-s"), "subscribed");
        List<Integer> live = new ArrayList<>();
        int hour = 0, removed = 0;
        for (; hour < 24 * 5; hour++) {
            for (int i = 0; i < 10; i++) write(scan.resolve("h" + hour + "/f" + i), "hour " + hour + " file " + i);
            live.add(hour);
            // the cleanup job: whatever is over three days old
            while (live.get(0) <= hour - 72 - 1) {
                int h = live.remove(0);
                for (int i = 0; i < 10; i++) Files.delete(scan.resolve("h" + h + "/f" + i));
                Files.delete(scan.resolve("h" + h));
                removed += 10;
            }
            scanned(p);
            assertEquals(0, p.repo.getDeletesHeld(), "hour " + hour + ": rolling retention held " + p.repo.getDeletesHeld()
                    + " delete(s) (" + removed + " removed so far) " + p.log);
            p.repo.ledger().advance(HOUR);
        }
        assertTrue(removed >= 400, "the cleanup removed " + removed);
        assertTrue(p.db.guardRows() <= 24 * 10 + 10, "the ledger keeps a day of records: " + p.db.guardRows());
        // a delete of recent files (an hour old) is no retention: 25% of the day's base within the day is held
        int recent = live.get(live.size() - 2);
        int n = 0;
        for (int h = live.get(live.size() - 1); n < 200 && h >= live.get(0); h--) {
            for (int i = 0; i < 10; i++, n++) Files.deleteIfExists(scan.resolve("h" + h + "/f" + i));
        }
        scanned(p);
        assertTrue(p.repo.getDeletesHeld() > 0, "200 deletes of recent files were not held");
        assertNotNull(recent);
    }

    @Test
    void retentionDeletesBeyondTheirOwnBudgetAreHeld() throws Exception {
        // keep four days: a day turns over a quarter of the files; retention deletes may reach twice that, 50 of 100
        TestRig p = producer("plugin-p", scan, "sync_retention_days", "4");
        for (int i = 0; i < 100; i++) write(scan.resolve("f" + i), "f " + i);
        scanned(p);
        p.repo.ledger().advance(4 * DAY + HOUR);
        for (int i = 0; i < 40; i++) Files.delete(scan.resolve("f" + i));
        scanned(p);
        assertEquals(0, p.repo.getDeletesHeld(), "40 deletes of files past the retention age");
        for (int i = 40; i < 60; i++) Files.delete(scan.resolve("f" + i));
        scanned(p);
        assertEquals(20, p.repo.getDeletesHeld(), "60 retention deletes in a day, past twice what four days' retention turns over");
        assertTrue(p.log.stream().anyMatch(l -> l.contains("past the retention age")), p.log.toString());
        // the admin releases it: the retention budget starts over; the others never counted it
        assertEquals(20, p.repo.releaseHeldDeletes(p.repo.getHoldId()));
        scanned(p);
        assertEquals(0, p.repo.getDeletesHeld());
        for (int i = 60; i < 70; i++) Files.delete(scan.resolve("f" + i));
        scanned(p);
        assertEquals(0, p.repo.getDeletesHeld());
    }

    // =================================================================== candidates

    @Test
    void candidatesAreListedByFirstSeenWithHowOftenTheyAnswered() throws Exception {
        TestRig p = producer("plugin-p", scan, "sync_key", "");
        TestRig s = subscriber("plugin-s", mirror, "sync_key", "", "sync_producers", "",
                "sync_subscribe_retry_ms", "100");
        p.repo.repoBroadcast("fr", "discover", 0);
        await(() -> s.repo.isKnownProducer(TestRig.REGION, TestRig.AGENT, "plugin-p"), "p answered");
        Path rogueDir = Files.createDirectories(tmp.resolve("rogue"));
        write(rogueDir.resolve("x"), "x");
        TestRig x = producer("plugin-x", rogueDir, "sync_key", "");
        // the rogue keeps answering
        for (int i = 0; i < 5; i++) {
            x.repo.repoBroadcast("fr", "discover", 0);
            Thread.sleep(150);
        }
        await(() -> s.repo.isKnownProducer(TestRig.REGION, TestRig.AGENT, "plugin-x"), "x answered");
        String listed = s.call("repostate").getParam("candidate_producers");
        int pAt = listed.indexOf(p.repo.producerPin()), xAt = listed.indexOf(x.repo.producerPin());
        assertTrue(pAt >= 0 && xAt > pAt, "first seen first, whoever answers since: " + listed);
        assertTrue(listed.contains("first seen ") && listed.contains("answer(s)"), listed);
        assertTrue(listed.substring(xAt).matches("(?s).*\\b[2-9] answer\\(s\\).*"), "x answered more than once: " + listed);
    }
}
