package io.cresco.filerepo;

import io.cresco.library.messaging.MsgEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MIRROR and ONLY_COPY from configuration and the catalog, and clearrepo: it takes the repository gate before it
 * checks, refuses an only copy without force, and no file filerepo lands can arrive between its check and its delete
 * (the race of the unmerged 40519ff, which checked first and took the gate after).
 */
class ClearRepoTest {

    @TempDir Path tmp;
    TestRig rig;
    final List<Thread> threads = Collections.synchronizedList(new ArrayList<>());

    @AfterEach
    void down() throws Exception {
        for (Thread t : threads) t.join(10_000);
        if (rig != null) rig.close();
    }

    /** A subscriber mirror: filerepo_name and repo_dir, no scan_dir. Its synced files are not cataloged. */
    TestRig mirror() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("filerepo_name", "fr");
        cfg.put("repo_dir", Files.createDirectories(tmp.resolve("mirror")).toString());
        return rig = new TestRig(tmp.resolve("agent"), cfg, null);
    }

    TestRig onDemand() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("repo_dir", Files.createDirectories(tmp.resolve("ondemand")).toString());
        return rig = new TestRig(tmp.resolve("agent"), cfg, null);
    }

    TestRig producer() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("filerepo_name", "fr");
        cfg.put("scan_dir", Files.createDirectories(tmp.resolve("scan")).toString());
        cfg.put("enable_scan", "true");
        cfg.put("scan_delay", "3600000");
        cfg.put("scan_period", "3600000");
        return rig = new TestRig(tmp.resolve("agent"), cfg, null);
    }

    static Path write(Path file, String content) throws Exception {
        Files.createDirectories(file.getParent());
        return Files.write(file, content.getBytes());
    }

    /** A putfiles of one local file, landing it (a copy) into the repository. */
    MsgEvent putfiles(String name) throws Exception {
        Path src = write(tmp.resolve("incoming").resolve(name), "landed " + name);
        MsgEvent m = TestRig.request("putfiles", "repo_name", "fr");
        m.addFile(src.toString());
        return m;
    }

    // ------------------------------------------------------------------ states and refusals

    @Test
    void aMirrorIsMIRRORAndIsClearedAsBefore() throws Exception {
        mirror();
        Path dir = tmp.resolve("mirror");
        write(dir.resolve("synced.txt"), "from the producer");
        write(dir.resolve("sub").resolve("nested.txt"), "from the producer");

        MsgEvent s = rig.call("repostate");
        assertEquals("10", s.getParam("status"));
        assertEquals("MIRROR", s.getParam("state"));
        assertEquals("local", s.getParam("backing"));
        assertEquals("subscriber", s.getParam("role"));
        assertEquals("false", s.getParam("landed"));

        MsgEvent c = rig.call("clearrepo", "repo_name", "fr");
        assertEquals("10", c.getParam("status"), c.getParams().toString());
        assertEquals("repo cleared", c.getParam("status_desc"));
        assertEquals("MIRROR", c.getParam("state"));
        assertFalse(Files.exists(dir.resolve("synced.txt")));
        assertFalse(Files.exists(dir.resolve("sub").resolve("nested.txt")));
    }

    @Test
    void anOnDemandRepositoryIsAnOnlyCopyAndOnlyForceClearsIt() throws Exception {
        onDemand();
        Path f = write(tmp.resolve("ondemand").resolve("work.txt"), "only here");
        MsgEvent s = rig.call("repostate");
        assertEquals("ONLY_COPY", s.getParam("state"));
        assertEquals("on-demand", s.getParam("role"));

        MsgEvent refused = rig.call("clearrepo", "repo_name", "fr");
        assertEquals("9", refused.getParam("status"));
        assertEquals("only-copy", refused.getParam("refusal"));
        assertEquals("ONLY_COPY", refused.getParam("state"));
        assertTrue(refused.getParam("status_desc").contains("force=true"), refused.getParam("status_desc"));
        assertTrue(Files.exists(f));

        assertEquals("9", rig.call("clearrepo", "repo_name", "fr", "force", "false").getParam("status"));
        assertEquals("9", rig.call("clearrepo", "repo_name", "fr", "force", "yes").getParam("status"), "only force=true forces");
        assertTrue(Files.exists(f));

        MsgEvent forced = rig.call("clearrepo", "repo_name", "fr", "force", "true");
        assertEquals("10", forced.getParam("status"));
        assertFalse(Files.exists(f));
    }

    @Test
    void aProducerIsAnOnlyCopy() throws Exception {
        producer();
        rig.repo.start();
        Path f = write(tmp.resolve("scan").resolve("origin.txt"), "the origin");
        MsgEvent s = rig.call("repostate");
        assertEquals("ONLY_COPY", s.getParam("state"));
        assertEquals("producer", s.getParam("role"));
        assertEquals("only-copy", rig.call("clearrepo", "repo_name", "fr").getParam("refusal"));
        assertTrue(Files.exists(f));
    }

    @Test
    void aMirrorThatFilesWereLandedInIsAnOnlyCopyUntilAForcedClear() throws Exception {
        mirror();
        MsgEvent put = rig.call(putfiles("landed.bin"));
        assertEquals("10", put.getParam("status"), put.getParams().toString());
        Path landed = tmp.resolve("mirror").resolve("landed.bin");
        assertTrue(Files.exists(landed));

        MsgEvent s = rig.call("repostate");
        assertEquals("ONLY_COPY", s.getParam("state"));
        assertEquals("true", s.getParam("landed"));
        assertEquals("only-copy", rig.call("clearrepo", "repo_name", "fr").getParam("refusal"));
        assertTrue(Files.exists(landed));

        // removing the landed file does not make it a mirror again: only a clear that removed everything does
        assertEquals("10", rig.call("removefile", "repo_name", "fr", "file_name", "landed.bin").getParam("status"));
        assertEquals("ONLY_COPY", rig.call("repostate").getParam("state"));

        assertEquals("10", rig.call("clearrepo", "repo_name", "fr", "force", "true").getParam("status"));
        assertEquals("MIRROR", rig.call("repostate").getParam("state"));
    }

    @Test
    void aJarLandedInAMirrorMakesItAnOnlyCopyAndTheRecordSurvivesARestart() throws Exception {
        mirror();
        byte[] jar = "not really a jar".getBytes();
        Path probe = write(tmp.resolve("probe.jar"), "not really a jar");
        MsgEvent putjar = TestRig.request("putjar", "pluginname", "p", "md5", rig.plugin.getMD5(probe.toString()),
                "jarfile", "p.jar", "version", "1");
        putjar.setDataParam("jardata", jar);
        assertEquals("10", rig.call(putjar).getParam("status"));
        assertEquals(0, rig.repo.getRepoCount(), "putjar does not catalog the jar (as before)");
        assertEquals("ONLY_COPY", rig.call("repostate").getParam("state"));

        rig.close();
        rig = null;
        mirror();                                            // the same catalog, a fresh engine
        MsgEvent s = rig.call("repostate");
        assertEquals("ONLY_COPY", s.getParam("state"), "the landing is recorded in the catalog, not in memory");
        assertEquals("only-copy", rig.call("clearrepo", "repo_name", "fr").getParam("refusal"));
        assertTrue(Files.exists(tmp.resolve("mirror").resolve("p.jar")));
    }

    @Test
    void repostateNeverLooksAtTheTree() throws Exception {
        mirror();
        Path dir = tmp.resolve("mirror");
        for (int i = 0; i < 50; i++) write(dir.resolve("f" + i), "x");
        try (java.util.stream.Stream<Path> s = Files.list(dir)) {
            for (Path p : (Iterable<Path>) s::iterator) Files.delete(p);
        }
        Files.delete(dir);
        assertEquals("MIRROR", rig.call("repostate").getParam("state"), "answered from configuration and the catalog");
        assertFalse(Files.exists(dir), "and the tree was not even created");
    }

    // ------------------------------------------------------------------ the gate

    @Test
    void theCheckRunsUnderTheGateSoALandingInProgressIsSeen() throws Exception {
        mirror();
        Path landed = tmp.resolve("mirror").resolve("arriving.bin");
        CountDownLatch entered = new CountDownLatch(1);
        Thread landing = new Thread(() -> {
            assertTrue(rig.repo.enterGate());
            try {
                entered.countDown();
                Thread.sleep(300);                           // the clear arrives while this landing holds the gate
                rig.repo.markLanded();
                write(landed, "the only copy");
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                rig.repo.exitGate();
            }
        });
        threads.add(landing);
        landing.start();
        assertTrue(entered.await(5, TimeUnit.SECONDS));

        MsgEvent c = rig.call("clearrepo", "repo_name", "fr");
        assertEquals("9", c.getParam("status"), "a check made before the gate would have seen MIRROR and deleted the file");
        assertEquals("only-copy", c.getParam("refusal"));
        assertTrue(Files.exists(landed));
    }

    @Test
    void aPutfilesThatArrivesBetweenTheCheckAndTheDeleteWaitsAndItsFileSurvives() throws Exception {
        mirror();
        Path synced = write(tmp.resolve("mirror").resolve("synced.txt"), "from the producer");
        MsgEvent put = putfiles("arrived-mid-clear.bin");
        AtomicReference<MsgEvent> putReply = new AtomicReference<>();
        AtomicBoolean putWaited = new AtomicBoolean();
        rig.repo.afterClearCheck = () -> {
            // the clear has checked (MIRROR) and holds the gate; a putfiles now must wait for the whole delete
            Thread t = new Thread(() -> putReply.set(rig.call(put)));
            threads.add(t);
            t.start();
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            putWaited.set(t.isAlive() && putReply.get() == null);
        };

        MsgEvent c = rig.call("clearrepo", "repo_name", "fr");
        rig.repo.afterClearCheck = null;
        assertEquals("10", c.getParam("status"));
        assertEquals("MIRROR", c.getParam("state"));
        for (Thread t : threads) t.join(10_000);

        assertTrue(putWaited.get(), "the putfiles was held at the gate while the clear was between check and delete");
        assertEquals("10", putReply.get().getParam("status"));
        assertFalse(Files.exists(synced), "the mirror's files were cleared");
        assertTrue(Files.exists(tmp.resolve("mirror").resolve("arrived-mid-clear.bin")), "the landed file was not");
        assertEquals("ONLY_COPY", rig.call("repostate").getParam("state"));
    }

    @Test
    void twoConcurrentClearAndPutfilesSequencesNeverLoseALandedFile() throws Exception {
        mirror();
        Path dir = tmp.resolve("mirror");
        // widen the window between check and delete: a landing that could slip in would
        rig.repo.afterClearCheck = () -> {
            try {
                Thread.sleep(3);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        int cleared = 0, refused = 0;
        for (int round = 0; round < 25; round++) {
            rig.repo.afterClearCheck = null;
            assertEquals("10", rig.call("clearrepo", "repo_name", "fr", "force", "true").getParam("status"));
            write(dir.resolve("synced-" + round + ".txt"), "from the producer");
            assertEquals("MIRROR", rig.call("repostate").getParam("state"));
            rig.repo.afterClearCheck = () -> {
                try {
                    Thread.sleep(3);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            };

            CyclicBarrier go = new CyclicBarrier(2);
            List<MsgEvent> clears = Collections.synchronizedList(new ArrayList<>());
            List<String> landedOk = Collections.synchronizedList(new ArrayList<>());
            final int r = round;
            Runnable sequence1 = () -> sequence(go, clears, landedOk, "a-" + r);
            Runnable sequence2 = () -> sequence(go, clears, landedOk, "b-" + r);
            Thread t1 = new Thread(sequence1), t2 = new Thread(sequence2);
            t1.start();
            t2.start();
            t1.join(30_000);
            t2.join(30_000);

            assertEquals(2, landedOk.size(), "both putfiles landed in round " + round);
            for (String name : landedOk) {
                assertTrue(Files.exists(dir.resolve(name)), "round " + round + ": a landed file was deleted: " + name);
            }
            for (MsgEvent c : clears) {
                if ("10".equals(c.getParam("status"))) cleared++;
                else {
                    assertEquals("only-copy", c.getParam("refusal"), c.getParams().toString());
                    refused++;
                }
            }
            assertEquals("ONLY_COPY", rig.call("repostate").getParam("state"));
        }
        assertEquals(50, cleared + refused);
        assertTrue(cleared > 0, "some clears ran before any landing in their round");
    }

    /** One of two concurrent sequences: clearrepo, then putfiles of a file of its own. */
    private void sequence(CyclicBarrier go, List<MsgEvent> clears, List<String> landedOk, String name) {
        try {
            MsgEvent put = putfiles(name);
            go.await(10, TimeUnit.SECONDS);
            clears.add(rig.call("clearrepo", "repo_name", "fr"));
            MsgEvent r = rig.call(put);
            if ("10".equals(r.getParam("status"))) landedOk.add(name);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void aClearThatCannotHaveTheGateSaysSoAndALandingThatCannotSaysSoToo() throws Exception {
        onDemand();
        rig.repo.gateWaitMs = 200;
        Path f = write(tmp.resolve("ondemand").resolve("kept.txt"), "x");

        CountDownLatch holding = new CountDownLatch(1), release = new CountDownLatch(1);
        Thread landing = new Thread(() -> {
            assertTrue(rig.repo.enterGate());
            try {
                holding.countDown();
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            } finally {
                rig.repo.exitGate();
            }
        });
        threads.add(landing);
        landing.start();
        assertTrue(holding.await(5, TimeUnit.SECONDS));
        MsgEvent busy = rig.call("clearrepo", "repo_name", "fr", "force", "true");
        assertEquals("9", busy.getParam("status"));
        assertTrue(busy.getParam("status_desc").startsWith("repo not cleared"), busy.getParam("status_desc"));
        assertTrue(Files.exists(f));
        release.countDown();
        landing.join(5000);

        // and the other way: a landing while a clear holds the gate past the wait fails, landing nothing
        CountDownLatch inClear = new CountDownLatch(1), letGo = new CountDownLatch(1);
        rig.repo.afterClearCheck = () -> {
            inClear.countDown();
            try {
                letGo.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
        };
        Thread clear = new Thread(() -> rig.call("clearrepo", "repo_name", "fr", "force", "true"));
        threads.add(clear);
        clear.start();
        assertTrue(inClear.await(5, TimeUnit.SECONDS));
        MsgEvent put = rig.call(putfiles("late.bin"));
        assertEquals("9", put.getParam("status"));
        assertFalse(Files.exists(tmp.resolve("ondemand").resolve("late.bin")));
        letGo.countDown();
        clear.join(5000);
        rig.repo.afterClearCheck = null;
    }
}
