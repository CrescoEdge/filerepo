package io.cresco.filerepo;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** The transfer pool's fairness: per-caller shares, and threads kept for sync that no set of other callers can take. */
class TransferSchedulerTest {

    @Test
    void noSetOfOtherCallersStarvesSync() throws Exception {
        // 8 threads: at most 4 for one caller, 2 kept for sync, so 6 for everyone else together
        TransferScheduler t = new TransferScheduler(8, 0.5, 0.25, 16, 64);
        assertEquals(4, t.perCallerRunning());
        assertEquals(2, t.syncReserve());
        CountDownLatch hold = new CountDownLatch(1);
        AtomicInteger running = new AtomicInteger();
        Runnable task = () -> {
            running.incrementAndGet();
            try {
                hold.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
        };
        try {
            for (int i = 0; i < 4; i++) assertEquals(TransferScheduler.Admit.STARTED, t.submit("alice", task));
            assertEquals(TransferScheduler.Admit.STARTED, t.submit("bob", task));
            assertEquals(TransferScheduler.Admit.STARTED, t.submit("bob", task));
            // two callers have every thread but the sync reserve: a third waits, holding no thread
            assertEquals(TransferScheduler.Admit.QUEUED, t.submit("bob", task));
            assertEquals(TransferScheduler.Admit.QUEUED, t.submit("carol", task));
            // a subscriber's fetch starts at once
            assertEquals(TransferScheduler.Admit.STARTED, t.submit("plugin:tenant/r/a/mirror-1", true, task));
            assertEquals(TransferScheduler.Admit.STARTED, t.submit("plugin:tenant/r/a/mirror-2", true, task));
            // the pool is full now: the next sync fetch waits its turn too (never refused)
            assertEquals(TransferScheduler.Admit.QUEUED, t.submit("plugin:tenant/r/a/mirror-3", true, task));
            long deadline = System.currentTimeMillis() + 5000;
            while (running.get() < 8 && System.currentTimeMillis() < deadline) Thread.sleep(10);
            assertEquals(8, running.get());
            assertEquals(8, t.runningCount());
            assertEquals(3, t.queued());
        } finally {
            hold.countDown();
        }
        long deadline = System.currentTimeMillis() + 5000;
        while ((t.runningCount() > 0 || t.queued() > 0) && System.currentTimeMillis() < deadline) Thread.sleep(10);
        assertEquals(11, running.get(), "every waiting transfer ran once threads were free");
        t.shutdown();
    }
}
