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
 * Runs streamfile transfers fairly (FR-D5): {@code threads} at once, at most {@code perCallerRunning} of them for any
 * one caller (a fraction of the pool), so one caller can never hold every thread; a caller's further transfers wait in
 * its own queue, holding no thread, up to {@code perCallerMax} in flight; at most {@code threads + queueMax} in flight in
 * all. Anything beyond a bound is refused (busy), never queued without limit. When a transfer ends, the next one to start
 * is taken round-robin from the callers that have one waiting and are below their running cap.
 */
final class TransferScheduler {

    enum Admit { STARTED, QUEUED, CALLER_FULL, FULL }

    private final ThreadPoolExecutor pool;
    private final int threads;
    private final int perCallerRunning;
    private final int perCallerMax;
    private final int totalMax;

    private final Map<String, Deque<Runnable>> waiting = new LinkedHashMap<>();
    private final Map<String, Integer> running = new HashMap<>();
    private final Map<String, Integer> inFlight = new HashMap<>();
    private int runningTotal;
    private int inFlightTotal;
    private boolean shut;

    TransferScheduler(int threads, double callerFraction, int perCallerMax, int queueMax) {
        this.threads = Math.max(2, threads);
        this.perCallerRunning = Math.max(1, Math.min(this.threads - 1, (int) Math.floor(this.threads * callerFraction)));
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

    /** Admit {@code task} for {@code caller}: started now, queued (its turn comes), or refused. */
    synchronized Admit submit(String caller, Runnable task) {
        if (shut) return Admit.FULL;
        int mine = inFlight.getOrDefault(caller, 0);
        if (mine >= perCallerMax) return Admit.CALLER_FULL;
        if (inFlightTotal >= totalMax) return Admit.FULL;
        inFlight.put(caller, mine + 1);
        inFlightTotal++;
        if (running.getOrDefault(caller, 0) < perCallerRunning && runningTotal < threads) {
            start(caller, task);
            return Admit.STARTED;
        }
        waiting.computeIfAbsent(caller, k -> new ArrayDeque<>()).add(task);
        return Admit.QUEUED;
    }

    private void start(String caller, Runnable task) {
        running.merge(caller, 1, Integer::sum);
        runningTotal++;
        pool.execute(() -> {
            try {
                task.run();
            } finally {
                finished(caller);
            }
        });
    }

    private synchronized void finished(String caller) {
        running.merge(caller, -1, Integer::sum);
        if (running.get(caller) <= 0) running.remove(caller);
        runningTotal--;
        inFlight.merge(caller, -1, Integer::sum);
        if (inFlight.get(caller) <= 0) inFlight.remove(caller);
        inFlightTotal--;
        dispatch();
    }

    /** Start waiting transfers, round-robin over callers below their running cap, while threads are free. */
    private void dispatch() {
        boolean progress = true;
        while (!shut && runningTotal < threads && progress) {
            progress = false;
            List<String> callers = new ArrayList<>(waiting.keySet());
            for (String c : callers) {
                if (runningTotal >= threads) break;
                if (running.getOrDefault(c, 0) >= perCallerRunning) continue;
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
