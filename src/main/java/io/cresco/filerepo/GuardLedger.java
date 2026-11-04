package io.cresco.filerepo;

import io.cresco.library.utilities.CLogger;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * What the delete and rewrite guards have let through, kept in the catalog so a restart forgets nothing.
 *
 * <p>For the last day, one record per path: a delete recorded ('D'), a delete of a file older than the retention age
 * an admin configured ('T', counted against the retention budget only), or a rewrite offered ('R'), with when, and when
 * the file was first cataloged ("born"), on the guards' own clock. The 15-minute window and the rolling day count these
 * records. Records older than a day are folded into counters, per guard, of what was let through since that guard's last
 * acknowledgement of its total (and dropped when it is acknowledged): the ledger holds at most a day of records and a
 * few counters, however long a repository runs between releases.
 *
 * <p>The clock is monotonic while the plugin runs (a wall-clock jump never ages a record), and is carried across a
 * restart from the catalog: the time the plugin was down counts as elapsed only up to {@link #DOWNTIME_CREDIT_MS}, and
 * a wall clock that moved backwards, or further than that, is logged (budgets are never cleared by a clock).
 *
 * <p>Acknowledgements are per guard and per budget ({@code winD}, {@code dayD}, {@code totD}, {@code retT} for deletes;
 * {@code dayR}, {@code totR} for rewrites): a release consumes only the budgets that held what it releases; every other
 * budget keeps counting.
 */
final class GuardLedger {

    static final char DELETE = 'D', REWRITE = 'R', RETAINED = 'T';
    static final String WIN_D = "winD", DAY_D = "dayD", TOT_D = "totD", RET_T = "retT", DAY_R = "dayR", TOT_R = "totR";
    static final long DOWNTIME_CREDIT_MS = 3_600_000L;
    static final String META_CLOCK = "guard_clock", META_WALL = "guard_wall", META_ACKS = "guard_acks",
            META_FOLDED = "guard_folded";

    private final DBEngine db;
    private final CLogger logger;
    private final long base;
    private final long nanoBase = System.nanoTime();
    private final Map<String, Long> acks = new LinkedHashMap<>();
    // records older than a day, folded: "<guard>.<kind>" -> how many that guard's total still counts
    private final Map<String, Long> folded = new LinkedHashMap<>();
    private volatile long lastPersist = Long.MIN_VALUE;
    // tests move the clock forward (a day passes)
    private volatile long skew;

    GuardLedger(DBEngine db, CLogger logger) {
        this.db = db;
        this.logger = logger;
        long clock = 0, wall = -1;
        if (db.openError() == null) try {
            String c = db.getMeta(META_CLOCK), w = db.getMeta(META_WALL);
            if (c != null) clock = Math.max(0, Long.parseLong(c));
            if (w != null) wall = Long.parseLong(w);
            readMap(db.getMeta(META_ACKS), acks);
            readMap(db.getMeta(META_FOLDED), folded);
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

    private static void readMap(String v, Map<String, Long> into) {
        if (v == null) return;
        for (String kv : v.split(",")) {
            int eq = kv.indexOf('=');
            if (eq > 0) into.put(kv.substring(0, eq), Long.parseLong(kv.substring(eq + 1)));
        }
    }

    private static String writeMap(Map<String, Long> m) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Long> e : m.entrySet()) {
            if (sb.length() > 0) sb.append(',');
            sb.append(e.getKey()).append('=').append(e.getValue());
        }
        return sb.toString();
    }

    /** The guards' clock, ms: monotonic while the plugin runs. */
    long now() {
        return base + skew + (System.nanoTime() - nanoBase) / 1_000_000L;
    }

    /** Tests: as if {@code ms} passed. */
    void advance(long ms) {
        skew += ms;
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

    /**
     * Budget {@code name} acknowledged at {@code at}: it counts only what comes after. A total's acknowledgement drops
     * what that guard's total had folded. Kept in the catalog.
     */
    synchronized void acknowledge(String name, long at) {
        acks.put(name, at);
        if (TOT_D.equals(name) || TOT_R.equals(name)) {
            String guard = TOT_D.equals(name) ? "D" : "R";
            folded.keySet().removeIf(k -> k.startsWith(guard + "."));
            persistFolded();
        }
        try {
            db.setMeta(META_ACKS, writeMap(acks));
        } catch (Exception ex) {
            if (logger != null) logger.error("an acknowledgement could not be recorded: " + ex.getMessage());
        }
        tick();
    }

    private void persistFolded() {
        try {
            if (folded.isEmpty()) db.deleteMeta(META_FOLDED);
            else db.setMeta(META_FOLDED, writeMap(folded));
        } catch (Exception ex) {
            if (logger != null) logger.error("the guards' folded counts could not be recorded: " + ex.getMessage());
        }
    }

    /**
     * Records of {@code kinds} (letters of D, T, R) made after {@code after}, of files first cataloged by {@code bornBy}
     * (Long.MAX_VALUE: whatever their age). The last day's only: older ones are folded.
     */
    int count(String kinds, long after, long bornBy) {
        return db.guardCount(kinds, after, bornBy);
    }

    /** Records of {@code kinds} made after {@code after}, whatever the files' age. */
    int count(String kinds, long after) {
        return count(kinds, after, Long.MAX_VALUE);
    }

    /** Records of {@code kind} older than a day that guard {@code guard} ('D' or 'R') still counts toward its total. */
    synchronized long folded(char guard, char kind) {
        Long n = folded.get(guard + "." + kind);
        return n == null ? 0L : n;
    }

    /** Paths (path -> when first cataloged) whose delete was recorded, or rewrite offered, now. */
    void record(char kind, Map<String, Long> paths) {
        if (paths.isEmpty()) return;
        db.guardRecord(kind, paths, now());
        tick();
    }

    /**
     * Start every budget that was never acknowledged now (the first scan of a repository, or of one from an earlier
     * build): what is cataloged now is what those budgets start from.
     */
    void initAcks() {
        long now = now();
        for (String b : new String[]{DAY_D, TOT_D, RET_T, DAY_R, TOT_R}) {
            if (ack(b) < 0) acknowledge(b, now);
        }
    }

    /** Where the day's budget {@code ackName} starts: a day ago, or its last acknowledgement if later. */
    long dayStart(String ackName) {
        return Math.max(now() - DeleteGuard.DAY_MS, ack(ackName));
    }

    /**
     * Where a total's base is cut: the files cataloged by then are its base. The last acknowledgement, or, once they are
     * a day old, the files added since too (they fall under the long-horizon cap after a day; files added less than a
     * day ago never enlarge it).
     */
    long totalCut(String ackName) {
        return Math.max(ack(ackName), now() - DeleteGuard.DAY_MS);
    }

    /**
     * The day's base from {@code start}: the files first cataloged by then, those still cataloged ({@code catalog}:
     * path -> when first cataloged) and those deleted since (by any budget). Additions since never enlarge it.
     */
    long dayBase(Map<String, Long> catalog, long start) {
        return present(catalog, start) + count("DT", start, start);
    }

    /**
     * A total's base for guard {@code guard} acknowledged at {@code ack}, cut at {@code cut}: the files first cataloged
     * by the cut, still cataloged or deleted since the acknowledgement (files a retention delete removed have left it).
     */
    long totalBase(Map<String, Long> catalog, char guard, long ack, long cut) {
        return present(catalog, cut) + count("D", ack, cut) + folded(guard, DELETE);
    }

    private static long present(Map<String, Long> catalog, long start) {
        long n = 0;
        for (Long born : catalog.values()) if (born == null || born <= start) n++;
        return n;
    }

    /** How many of {@code paths} are recorded under {@code kind} after {@code after} (each path counts once). */
    int known(char kind, Collection<String> paths, long after) {
        return paths.isEmpty() ? 0 : db.guardKnown(kind, paths, after);
    }

    /**
     * Fold the records older than a day into each guard's total ({@code totalD}, {@code totalR}: whether that total is
     * on), counting only what came after that guard's acknowledgement, and drop them: the ledger keeps a day of records.
     */
    synchronized void prune(boolean totalD, boolean totalR) {
        long cut = now() - DeleteGuard.DAY_MS;
        if (cut <= 0) return;
        if (db.guardCount("DTR", -1L, Long.MAX_VALUE) == db.guardCount("DTR", cut, Long.MAX_VALUE)) return;
        boolean changed = false;
        for (char guard : new char[]{'D', 'R'}) {
            if (guard == 'D' ? !totalD : !totalR) continue;
            long ack = ack(guard == 'D' ? TOT_D : TOT_R);
            for (char kind : new char[]{DELETE, REWRITE}) {
                int n = db.guardCountBetween(kind, ack, cut);
                if (n > 0) {
                    folded.merge(guard + "." + kind, (long) n, Long::sum);
                    changed = true;
                }
            }
        }
        if (changed) persistFolded();
        db.guardPrune(cut);
    }
}
