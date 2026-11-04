package io.cresco.filerepo;

import io.cresco.library.utilities.CLogger;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * What the delete and rewrite guards have let through, kept in the catalog so a restart forgets nothing: every path
 * whose delete was recorded ('D') or whose rewrite was offered ('R'), with when, on the guards' own clock. The guards'
 * windows (15 minutes, a day) and their budgets since an admin's acknowledgement count these records.
 *
 * <p>The clock is monotonic while the plugin runs (a wall-clock jump never ages a record), and is carried across a
 * restart from the catalog: the time the plugin was down counts as elapsed only up to {@link #DOWNTIME_CREDIT_MS}, and
 * a wall clock that moved backwards, or further than that, is logged (budgets are never cleared by a clock).
 *
 * <p>Acknowledgements are per guard and per budget ({@code winD}, {@code dayD}, {@code totD} for deletes; {@code dayR},
 * {@code totR} for rewrites): a release consumes only the budgets that held what it releases; every other budget keeps
 * counting.
 */
final class GuardLedger {

    static final char DELETE = 'D', REWRITE = 'R';
    static final String WIN_D = "winD", DAY_D = "dayD", TOT_D = "totD", DAY_R = "dayR", TOT_R = "totR";
    static final long DOWNTIME_CREDIT_MS = 3_600_000L;
    static final String META_CLOCK = "guard_clock", META_WALL = "guard_wall", META_ACKS = "guard_acks";

    private final DBEngine db;
    private final CLogger logger;
    private final long base;
    private final long nanoBase = System.nanoTime();
    private final Map<String, Long> acks = new LinkedHashMap<>();
    private volatile long lastPersist = Long.MIN_VALUE;

    GuardLedger(DBEngine db, CLogger logger) {
        this.db = db;
        this.logger = logger;
        long clock = 0, wall = -1;
        if (db.openError() == null) try {
            String c = db.getMeta(META_CLOCK), w = db.getMeta(META_WALL);
            if (c != null) clock = Math.max(0, Long.parseLong(c));
            if (w != null) wall = Long.parseLong(w);
            String a = db.getMeta(META_ACKS);
            if (a != null) {
                for (String kv : a.split(",")) {
                    int eq = kv.indexOf('=');
                    if (eq > 0) acks.put(kv.substring(0, eq), Long.parseLong(kv.substring(eq + 1)));
                }
            }
        } catch (Exception ex) {
            if (logger != null) logger.error("the guards' clock or acknowledgements are unreadable: " + ex.getMessage());
        }
        long credit = 0;
        if (wall >= 0) {
            long gap = System.currentTimeMillis() - wall;
            if (gap < 0) {
                if (logger != null) logger.warn("the wall clock is " + (-gap / 1000) + " s behind the guards' last record:"
                        + " the time down counts as nothing; the guards' records are kept");
            } else if (gap > DOWNTIME_CREDIT_MS) {
                credit = DOWNTIME_CREDIT_MS;
                if (logger != null) logger.warn("the wall clock moved " + (gap / 60000) + " min past the guards' last record"
                        + " (the plugin was down, or the clock jumped): the guards count only " + (DOWNTIME_CREDIT_MS / 60000)
                        + " min of it as elapsed, so no budget is cleared by a clock; an admin's release acknowledges a hold");
            } else {
                credit = gap;
            }
        }
        // never behind what was recorded (the clock is recorded at most once a second)
        base = Math.max(clock + credit, db.openError() == null ? db.guardMaxAt() : 0L);
    }

    /** The guards' clock, ms: monotonic while the plugin runs. */
    long now() {
        return base + (System.nanoTime() - nanoBase) / 1_000_000L;
    }

    /** Record the clock (at most once a second), so a restart resumes it. */
    void tick() {
        long now = now();
        if (lastPersist != Long.MIN_VALUE && now - lastPersist < 1000) return;
        lastPersist = now;
        try {
            db.setMeta(META_CLOCK, String.valueOf(now));
            db.setMeta(META_WALL, String.valueOf(System.currentTimeMillis()));
        } catch (Exception ex) {
            if (logger != null) logger.error("the guards' clock could not be recorded: " + ex.getMessage());
        }
    }

    /** When an admin last acknowledged budget {@code name} (-1: never; everything recorded counts). */
    synchronized long ack(String name) {
        Long t = acks.get(name);
        return t == null ? -1L : t;
    }

    /** Budget {@code name} acknowledged at {@code at}: it counts only what comes after. Kept in the catalog. */
    synchronized void acknowledge(String name, long at) {
        acks.put(name, at);
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Long> e : acks.entrySet()) {
            if (sb.length() > 0) sb.append(',');
            sb.append(e.getKey()).append('=').append(e.getValue());
        }
        try {
            db.setMeta(META_ACKS, sb.toString());
        } catch (Exception ex) {
            if (logger != null) logger.error("an acknowledgement could not be recorded: " + ex.getMessage());
        }
        tick();
    }

    /** Records of {@code kinds} (e.g. "D", "DR") made after {@code after}, whatever the files' age. */
    int count(String kinds, long after) {
        return db.guardCount(kinds, after, Long.MAX_VALUE);
    }

    /** Records of {@code kinds} made after {@code after}, of originals: files first cataloged by {@code bornBy}. */
    int count(String kinds, long after, long bornBy) {
        return db.guardCount(kinds, after, bornBy);
    }

    /** Paths (path -> when first cataloged) whose delete was recorded, or rewrite offered, now. */
    void record(char kind, Map<String, Long> paths) {
        if (paths.isEmpty()) return;
        db.guardRecord(kind, paths, now());
        tick();
    }

    /**
     * Start every budget that was never acknowledged now (the first scan of a repository, or of one from an earlier
     * build): what is cataloged now is what those budgets protect.
     */
    void initAcks() {
        long now = now();
        for (String b : new String[]{DAY_D, TOT_D, DAY_R, TOT_R}) {
            if (ack(b) < 0) acknowledge(b, now);
        }
    }

    /** Where the day's budgets start: a day ago, or their last acknowledgement if later. */
    long dayStart(String ackName) {
        return Math.max(now() - DeleteGuard.DAY_MS, ack(ackName));
    }

    /**
     * The originals of a period starting at {@code start}: the files first cataloged by then, those still cataloged
     * ({@code catalog}: path -> when first cataloged) and those whose delete was recorded since.
     */
    long originals(Map<String, Long> catalog, long start) {
        long n = 0;
        for (Long born : catalog.values()) if (born == null || born <= start) n++;
        return n + count("D", start, start);
    }

    /** How many of {@code paths} are recorded under {@code kind} after {@code after} (each path counts once). */
    int known(char kind, Collection<String> paths, long after) {
        return paths.isEmpty() ? 0 : db.guardKnown(kind, paths, after);
    }

    /**
     * Forget records no budget counts any more: older than a day, and acknowledged by each budget since the last
     * acknowledgement that is on ({@code totalD}, {@code totalR}).
     */
    void prune(boolean totalD, boolean totalR) {
        long keepAfter = now() - DeleteGuard.DAY_MS;
        synchronized (this) {
            if (totalD) keepAfter = Math.min(keepAfter, ack(TOT_D));
            if (totalR) keepAfter = Math.min(keepAfter, ack(TOT_R));
        }
        if (keepAfter >= 0) db.guardPrune(keepAfter);
    }
}
