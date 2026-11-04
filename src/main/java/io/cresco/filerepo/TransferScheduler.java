package io.cresco.filerepo;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Runs streamfile transfers fairly (FR-D5): {@code threads} at once; at most {@code perCallerRunning} of them for any
 * one caller (a small fraction of the pool, so filling it takes several callers); and {@code syncReserve} threads that
 * only sync (a subscriber of this producer fetching what it was offered) may use, so no set of other callers, however
 * many, ever starves the mirrors. A caller's further transfers wait in its own queue, holding no thread, up to
 * {@code perCallerMax} in flight; at most {@code threads + queueMax} in flight in all. Anything beyond a bound is refused
 * (busy), never queued without limit. When a transfer ends, the next one to start is taken round-robin from the callers
 * that have one waiting and may start one.
 */
final class TransferScheduler {

    enum Admit { STARTED, QUEUED, CALLER_FULL, FULL }

    private final ThreadPoolExecutor pool;
    private final int threads;
    private final int perCallerRunning;
    private final int perCallerMax;
    private final int totalMax;
    private final int syncReserve;

    private final Map<String, Deque<Runnable>> waiting = new LinkedHashMap<>();
    private final Map<String, Boolean> isSync = new HashMap<>();
    private final Map<String, Integer> running = new HashMap<>();
    private final Map<String, Integer> inFlight = new HashMap<>();
    private int runningTotal;
    private int runningOther;
    private int inFlightTotal;
    private boolean shut;

    TransferScheduler(int threads, double callerFraction, int perCallerMax, int queueMax) {
        this(threads, callerFraction, 0.25, perCallerMax, queueMax);
    }

    TransferScheduler(int threads, double callerFraction, double syncFraction, int perCallerMax, int queueMax) {
        this.threads = Math.max(2, threads);
        this.perCallerRunning = Math.max(1, Math.min(this.threads - 1, (int) Math.floor(this.threads * callerFraction)));
        this.syncReserve = Math.max(1, Math.min(this.threads - 1, (int) Math.floor(this.threads * syncFraction)));
        this.perCallerMax = Math.max(1, perCallerMax);
        this.totalMax = this.threads + Math.max(0, queueMax);
        ThreadPoolExecutor tp = new ThreadPoolExecutor(this.threads, this.threads, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                r -> { Thread t = new Thread(r, "filerepo-transfer"); t.setDaemon(true); return t; });
        tp.allowCoreThreadTimeOut(true);
        this.pool = tp;
    }

    int perCallerRunning() {
        return perCallerRunning;
    }

    int syncReserve() {
        return syncReserve;
    }

    Admit submit(String caller, Runnable task) {
        return submit(caller, false, task);
    }

    /** Admit {@code task} for {@code caller} ({@code sync}: a subscriber's fetch): started now, queued, or refused. */
    synchronized Admit submit(String caller, boolean sync, Runnable task) {
        if (shut) return Admit.FULL;
        String key = (sync ? "s|" : "o|") + caller;
        int mine = inFlight.getOrDefault(key, 0);
        if (mine >= perCallerMax) return Admit.CALLER_FULL;
        if (inFlightTotal >= totalMax) return Admit.FULL;
        inFlight.put(key, mine + 1);
        inFlightTotal++;
        isSync.put(key, sync);
        if (mayStart(key)) {
            start(key, task);
            return Admit.STARTED;
        }
        waiting.computeIfAbsent(key, k -> new ArrayDeque<>()).add(task);
        return Admit.QUEUED;
    }

    private boolean mayStart(String key) {
        if (runningTotal >= threads) return false;
        if (running.getOrDefault(key, 0) >= perCallerRunning) return false;
        // everyone but sync leaves the reserve free
        return Boolean.TRUE.equals(isSync.get(key)) || runningOther < threads - syncReserve;
    }

    private void start(String key, Runnable task) {
        running.merge(key, 1, Integer::sum);
        runningTotal++;
        if (!Boolean.TRUE.equals(isSync.get(key))) runningOther++;
        pool.execute(() -> {
            try {
                task.run();
            } finally {
                finished(key);
            }
        });
    }

    private synchronized void finished(String key) {
        running.merge(key, -1, Integer::sum);
        if (running.get(key) <= 0) running.remove(key);
        runningTotal--;
        if (!Boolean.TRUE.equals(isSync.get(key))) runningOther--;
        inFlight.merge(key, -1, Integer::sum);
        if (inFlight.get(key) <= 0) {
            inFlight.remove(key);
            if (!waiting.containsKey(key)) isSync.remove(key);
        }
        inFlightTotal--;
        dispatch();
    }

    /** Start waiting transfers, round-robin over callers that may start one, while threads are free. */
    private void dispatch() {
        boolean progress = true;
        while (!shut && runningTotal < threads && progress) {
            progress = false;
            List<String> callers = new ArrayList<>(waiting.keySet());
            for (String c : callers) {
                if (runningTotal >= threads) break;
                if (!mayStart(c)) continue;
                Deque<Runnable> q = waiting.get(c);
                Runnable next = q.poll();
                if (q.isEmpty()) waiting.remove(c);
                if (next == null) continue;
                // rotate: this caller goes to the back of the line
                Deque<Runnable> rest = waiting.remove(c);
                if (rest != null) waiting.put(c, rest);
                start(c, next);
                progress = true;
            }
        }
    }

    synchronized int queued() {
        int n = 0;
        for (Deque<Runnable> q : waiting.values()) n += q.size();
        return n;
    }

    synchronized int runningCount() {
        return runningTotal;
    }

    /** Stop: no more starts; queued transfers are dropped (their callers see no terminal frame). */
    synchronized List<Runnable> shutdown() {
        shut = true;
        List<Runnable> dropped = new ArrayList<>();
        for (Iterator<Deque<Runnable>> it = waiting.values().iterator(); it.hasNext(); ) {
            dropped.addAll(it.next());
            it.remove();
        }
        pool.shutdownNow();
        return dropped;
    }
}
