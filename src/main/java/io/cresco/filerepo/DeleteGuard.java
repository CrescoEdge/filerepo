package io.cresco.filerepo;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The mass-delete guard (FR-D4): which of the files a scan found gone may be recorded as deletes for the subscribers,
 * and which are held for an admin. Held, never propagated:
 * <ul>
 *   <li>always, every file under a <b>mount point that vanished</b> (the scan remembers the directories of the tree
 *   that sit on another device than their parent; one that is gone, or no longer on its own device, was unmounted),
 *   and every file when the scan dir itself is no longer on the device it was;</li>
 *   <li>always, everything when the scan dir is suddenly empty;</li>
 *   <li>every file of a <b>whole subtree</b> that lost all its cataloged files, when they are at least
 *   {@code subtreeMin} files AND more than {@code subtreeFraction} of the catalog: what a nested volume going away
 *   looks like where device ids cannot tell it, while an ordinary {@code rm -rf} of a small directory goes through
 *   (and a subtree whose files are all past the retention age goes through as retention);</li>
 *   <li>everything, when the loss (beyond the above) is at least {@code min} files AND more than {@code fraction} of
 *   the catalog, counting the deletes recorded within the last {@code windowMs} too;</li>
 *   <li>everything, when every file deleted within the last 24 hours (whenever it was added) and every original
 *   rewritten, with this cycle's loss, are at least {@code min} AND more than {@code dayFraction} of the day's base:
 *   the files cataloged when the day began. Gross, so a delete replaced by another file (x by x.locked) counts, and
 *   files added during the day count when they are deleted but never enlarge the base;</li>
 *   <li>everything, when the same since the last admin acknowledgement are more than {@code totalFraction} of the
 *   total's base: the files cataloged at the acknowledgement, and those added since once they are a day old. However
 *   slowly a catalog is drained or churned, never past that without an admin;</li>
 *   <li>with a retention age configured (an admin's {@code sync_retention_days}), deletes of files filerepo first
 *   cataloged longer ago than that are expected: they count against none of the above, only against a separate,
 *   generous budget ({@code retentionFraction} of the day's base within a day); deletes of younger files count as
 *   any.</li>
 * </ul>
 * What was let through is kept in the catalog ({@link GuardLedger}), on a clock that is monotonic while the plugin runs,
 * so neither a restart nor a jump of the wall clock clears a budget. What is held is identified by a digest of exactly
 * those deletes. An admin's release must name that digest, is spent by the next evaluation whatever it finds, and
 * expires: it never applies to a later or another loss. A release consumes only the budgets that held what it releases
 * (the 15-minute window, the day, the total since the last acknowledgement, the retention budget); the released deletes
 * themselves are acknowledged and count toward nothing; every other budget, and the rewrite guard's, keeps counting.
 */
final class DeleteGuard {

    /** Why a delete is held. */
    enum Budget { MOUNT, SUBTREE, EMPTY, WINDOW, DAY, TOTAL, RETENTION }

    /** What one evaluation decided. */
    static final class Decision {
        final List<DBEngine.Tombstone> record;
        final List<DBEngine.Tombstone> held;
        final String holdId;
        final String why;
        final boolean released;

        Decision(List<DBEngine.Tombstone> record, List<DBEngine.Tombstone> held, String holdId, String why, boolean released) {
            this.record = record;
            this.held = held;
            this.holdId = holdId;
            this.why = why;
            this.released = released;
        }
    }

    private final double fraction;
    private final int min;
    private final int subtreeMin;
    private final double subtreeFraction;
    private final long releaseTtlMs;
    private final long windowMs;
    static final long DAY_MS = 24L * 3600 * 1000;
    private double dayFraction;
    private double totalFraction;
    // deletes of files first cataloged longer ago than this are retention (0: no retention), against their own budget
    private long retentionAgeMs;
    private double retentionFraction;

    // the hold now in force (from the last evaluation), and an admin's armed release
    private volatile String holdId;
    private volatile int heldCount;
    private volatile String releaseId;
    private volatile long releaseExpires;

    DeleteGuard(double fraction, int min, int subtreeMin, double subtreeFraction, long releaseTtlMs) {
        this(fraction, min, subtreeMin, subtreeFraction, releaseTtlMs, 0L);
    }

    DeleteGuard(double fraction, int min, int subtreeMin, double subtreeFraction, long releaseTtlMs, long windowMs) {
        this.fraction = fraction;
        this.min = Math.max(1, min);
        this.subtreeMin = Math.max(1, subtreeMin);
        this.subtreeFraction = subtreeFraction;
        this.releaseTtlMs = releaseTtlMs;
        this.windowMs = Math.max(0L, windowMs);
    }

    /** The longer-horizon budgets: a rolling day, and the total since an admin's acknowledgement. */
    synchronized void setBudgets(double dayFraction, double totalFraction) {
        // a day allows at least what one window does, and the total at least what a day does (0 turns one off)
        this.dayFraction = dayFraction <= 0 ? 0 : Math.max(dayFraction, fraction);
        this.totalFraction = totalFraction <= 0 ? 0 : Math.max(totalFraction, Math.max(this.dayFraction, fraction));
    }

    boolean totalOn() {
        return totalFraction > 0;
    }

    /**
     * Rolling retention: deletes of files first cataloged at least {@code ageMs} ago (0: none) are expected, and count
     * only against their own budget: within a rolling day, {@code fraction} times what such a retention turns over in a
     * day (the catalog, or the day's base if larger, times a day over the retention age).
     */
    synchronized void setRetention(long ageMs, double fraction) {
        this.retentionAgeMs = Math.max(0L, ageMs);
        this.retentionFraction = fraction;
    }

    int heldCount() {
        return heldCount;
    }

    String holdId() {
        return holdId;
    }

    /** A release that named no hold: refused (an admin releases exactly the set it read). */
    static final int RELEASE_NEEDS_ID = -2;

    /**
     * Arm a release of the current hold, which {@code id} must name, until {@code now} (the guards' clock) plus the
     * release's time to live. The count released, 0 when nothing is held, -1 when {@code id} names another set,
     * {@link #RELEASE_NEEDS_ID} when it names none.
     */
    synchronized int release(String id, long now) {
        if (id == null || id.isEmpty()) return RELEASE_NEEDS_ID;
        if (holdId == null || heldCount == 0) return 0;
        if (!id.equals(holdId)) return -1;
        releaseId = holdId;
        releaseExpires = now + releaseTtlMs;
        return heldCount;
    }

    /**
     * Decide what to record of {@code gone} (files under {@code root} cataloged and no longer found by a complete
     * walk). {@code catalogUnderRoot} holds every cataloged path under root; {@code seen} is how many files the walk
     * found; {@code vanishedMounts} the remembered mount points (absolute paths) that vanished, with {@code root}
     * itself meaning the whole scan dir. What is recorded is noted in {@code ledger}; a release acknowledges there the
     * budgets that held it.
     */
    synchronized Decision evaluate(Path root, Map<String, Long> catalogUnderRoot, List<DBEngine.Tombstone> gone, int seen,
                                   Collection<String> vanishedMounts, GuardLedger ledger) {
        long now = ledger.now();
        // a release is spent by the first evaluation after it was armed, whatever that finds
        String armed = (releaseId != null && now <= releaseExpires) ? releaseId : null;
        releaseId = null;
        if (gone.isEmpty()) {
            holdId = null;
            heldCount = 0;
            return new Decision(gone, List.of(), null, null, false);
        }
        Map<String, DBEngine.Tombstone> byPath = new LinkedHashMap<>();
        for (DBEngine.Tombstone t : gone) byPath.put(t.filepath, t);
        Set<String> held = new HashSet<>();
        List<String> why = new ArrayList<>();
        EnumSet<Budget> budgets = EnumSet.noneOf(Budget.class);

        // 1. vanished mounts (or the scan dir itself)
        for (String m : vanishedMounts) {
            Path mp = Paths.get(m);
            boolean all = mp.equals(root);
            int n = 0;
            for (String p : byPath.keySet()) {
                if (all || Paths.get(p).startsWith(mp)) {
                    if (held.add(p)) n++;
                }
            }
            if (n > 0) {
                budgets.add(Budget.MOUNT);
                why.add(n + " under " + (all ? "the scan dir, which is no longer on its device" : "the vanished mount " + m));
            }
        }

        // which of the deletes are retention: of files first cataloged at least the retention age ago
        Set<String> retained = new HashSet<>();
        if (retentionAgeMs > 0) {
            for (String p : byPath.keySet()) {
                Long b = catalogUnderRoot.get(p);
                if (now - (b == null ? 0L : b) >= retentionAgeMs) retained.add(p);
            }
        }

        // 2. whole subtrees that lost every cataloged file (unless every one of them is retention: an expired dated
        // directory going away whole)
        Map<Path, int[]> dirs = new HashMap<>();   // dir -> {cataloged, gone, gone and not retention}
        for (String p : catalogUnderRoot.keySet()) count(dirs, root, Paths.get(p), 0);
        for (String p : byPath.keySet()) {
            count(dirs, root, Paths.get(p), 1);
            if (!retained.contains(p)) count(dirs, root, Paths.get(p), 2);
        }
        List<Path> lost = new ArrayList<>();
        int size = catalogUnderRoot.size();
        for (Map.Entry<Path, int[]> e : dirs.entrySet()) {
            int[] c = e.getValue();
            if (c[1] == c[0] && c[2] > 0 && c[0] >= subtreeMin && c[0] > subtreeFraction * size) lost.add(e.getKey());
        }
        lost.sort(Comparator.comparingInt(Path::getNameCount));
        List<Path> outermost = new ArrayList<>();
        for (Path d : lost) {
            boolean inner = false;
            for (Path o : outermost) if (d.startsWith(o)) inner = true;
            if (!inner) outermost.add(d);
        }
        for (Path d : outermost) {
            int n = 0;
            for (String p : byPath.keySet()) {
                if (Paths.get(p).startsWith(d) && held.add(p)) n++;
            }
            if (n > 0) {
                budgets.add(Budget.SUBTREE);
                why.add("the whole subtree " + root.relativize(d) + " (" + n + " files)");
            }
        }

        // 3. an empty scan dir, or the budgets (what was let through is in the ledger, kept across restarts)
        int rest = 0, restRetained = 0;
        for (String p : byPath.keySet()) {
            if (held.contains(p)) continue;
            if (retained.contains(p)) restRetained++;
            else rest++;
        }
        boolean empty = seen == 0 && !catalogUnderRoot.isEmpty();
        // the 15-minute window: what it recorded, with this cycle's loss, against the catalog as it was before them
        int earlier = windowMs > 0 && rest > 0 ? ledger.count("D", Math.max(now - windowMs, ledger.ack(GuardLedger.WIN_D))) : 0;
        int loss = rest + earlier;
        boolean large = rest > 0 && loss >= min && loss > fraction * (catalogUnderRoot.size() + earlier);
        // the day: every file deleted (whenever it was added) and every original rewritten since the day began,
        // against the files cataloged when it began: additions count when deleted, never enlarge the base
        long dayStart = ledger.dayStart(GuardLedger.DAY_D);
        long dayBase = 0;
        int today = 0;
        boolean dayLarge = false;
        if (dayFraction > 0 && rest > 0) {
            today = ledger.count("D", dayStart) + ledger.count("R", dayStart, dayStart);
            dayBase = ledger.dayBase(catalogUnderRoot, dayStart);
            dayLarge = rest + today >= min && rest + today > dayFraction * dayBase;
        }
        // the total since the last acknowledgement: the same, against the files cataloged then and those added since
        // once a day old (so files added since the last release fall under it too, and fresh additions never dilute it)
        long totAck = ledger.ack(GuardLedger.TOT_D), totCut = ledger.totalCut(GuardLedger.TOT_D);
        long totBase = 0, sinceAck = 0;
        boolean totalLarge = false;
        if (totalFraction > 0 && rest > 0) {
            sinceAck = ledger.count("D", totAck) + ledger.count("R", totAck, totCut)
                    + ledger.folded('D', GuardLedger.DELETE) + ledger.folded('D', GuardLedger.REWRITE);
            totBase = ledger.totalBase(catalogUnderRoot, 'D', totAck, totCut);
            totalLarge = rest + sinceAck >= min && rest + sinceAck > totalFraction * totBase;
        }
        // retention: deletes of files past the retention age within a day, against what a rolling retention turns over
        // in a day (the catalog over the retention age in days), times retentionFraction
        int retainedToday = 0;
        long retBase = 0;
        double retAllowance = 0;
        boolean retentionLarge = false;
        if (restRetained > 0) {
            long retStart = Math.max(now - DAY_MS, ledger.ack(GuardLedger.RET_T));
            retainedToday = ledger.count("T", retStart);
            retBase = Math.max(ledger.dayBase(catalogUnderRoot, ledger.dayStart(GuardLedger.DAY_D)), catalogUnderRoot.size());
            retAllowance = retentionFraction * retBase * (DAY_MS / (double) retentionAgeMs);
            retentionLarge = restRetained + retainedToday >= min && restRetained + retainedToday > retAllowance;
        }
        if (empty || large || dayLarge || totalLarge || retentionLarge) {
            held.addAll(byPath.keySet());
            if (empty) budgets.add(Budget.EMPTY);
            if (large) budgets.add(Budget.WINDOW);
            if (dayLarge) budgets.add(Budget.DAY);
            if (totalLarge) budgets.add(Budget.TOTAL);
            if (retentionLarge) budgets.add(Budget.RETENTION);
            why.add(empty ? "the scan dir is empty"
                    : large ? rest + " of " + catalogUnderRoot.size() + " cataloged files"
                            + (earlier > 0 ? ", after " + earlier + " recorded in the last " + (windowMs / 60000) + " min" : "")
                            + " (more than " + Math.round(fraction * 100) + "%)"
                    : dayLarge ? rest + " now, after " + today + " deleted or rewritten in the last 24 h (more than "
                            + Math.round(dayFraction * 100) + "% of the " + dayBase + " files cataloged when the day began)"
                    : totalLarge ? rest + " now, after " + sinceAck + " deleted or rewritten since the last acknowledgement"
                            + " (more than " + Math.round(totalFraction * 100) + "% of the " + totBase + " files cataloged then,"
                            + " or a day old)"
                    : restRetained + " past the retention age now, after " + retainedToday + " in the last 24 h (more than "
                            + Math.round(retAllowance) + ": " + retentionFraction + " times what a " + retentionAgeMs / 3_600_000.0
                            + " h retention of " + retBase + " files turns over in a day)");
        }

        List<DBEngine.Tombstone> record = new ArrayList<>();
        List<DBEngine.Tombstone> holding = new ArrayList<>();
        for (Map.Entry<String, DBEngine.Tombstone> e : byPath.entrySet()) {
            (held.contains(e.getKey()) ? holding : record).add(e.getValue());
        }
        if (holding.isEmpty()) {
            holdId = null;
            heldCount = 0;
            note(ledger, record, catalogUnderRoot, retained);
            return new Decision(record, holding, null, null, false);
        }
        String id = digest(holding);
        if (id.equals(armed)) {
            // exactly the set an admin released: recorded, once. It consumes the budgets that held it, and only them;
            // the deletes it releases are acknowledged and count toward nothing
            if (budgets.contains(Budget.WINDOW)) ledger.acknowledge(GuardLedger.WIN_D, now);
            if (budgets.contains(Budget.DAY)) ledger.acknowledge(GuardLedger.DAY_D, now);
            if (budgets.contains(Budget.TOTAL)) ledger.acknowledge(GuardLedger.TOT_D, now);
            if (budgets.contains(Budget.RETENTION)) ledger.acknowledge(GuardLedger.RET_T, now);
            note(ledger, record, catalogUnderRoot, retained);
            holdId = null;
            heldCount = 0;
            record.addAll(holding);
            return new Decision(record, List.of(), id, String.join("; ", why) + " " + budgets, true);
        }
        note(ledger, record, catalogUnderRoot, retained);
        holdId = id;
        heldCount = holding.size();
        return new Decision(record, holding, id, String.join("; ", why), false);
    }

    /** Note the deletes recorded in the ledger: retention ones apart, each with when its file was first cataloged. */
    private static void note(GuardLedger ledger, List<DBEngine.Tombstone> ts, Map<String, Long> catalog, Set<String> retained) {
        Map<String, Long> regular = new LinkedHashMap<>(), retention = new LinkedHashMap<>();
        for (DBEngine.Tombstone t : ts) {
            (retained.contains(t.filepath) ? retention : regular).put(t.filepath, catalog.getOrDefault(t.filepath, 0L));
        }
        ledger.record(GuardLedger.DELETE, regular);
        ledger.record(GuardLedger.RETAINED, retention);
    }

    private static void count(Map<Path, int[]> dirs, Path root, Path file, int slot) {
        Path rel;
        try {
            rel = root.relativize(file);
        } catch (IllegalArgumentException ex) {
            return;
        }
        Path d = file.getParent();
        // every directory strictly under root that holds the file
        for (int i = rel.getNameCount() - 1; i >= 1 && d != null; i--) {
            dirs.computeIfAbsent(d, k -> new int[3])[slot]++;
            d = d.getParent();
        }
    }

    /** A digest of exactly these deletes (path and content), so a release names one set and no other. */
    static String digest(List<DBEngine.Tombstone> held) {
        try {
            List<DBEngine.Tombstone> sorted = new ArrayList<>(held);
            sorted.sort(Comparator.comparing(t -> t.filepath));
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            for (DBEngine.Tombstone t : sorted) {
                md.update(t.filepath.getBytes(StandardCharsets.UTF_8));
                md.update((byte) 0);
                md.update(String.valueOf(t.md5).getBytes(StandardCharsets.UTF_8));
                md.update((byte) 0);
            }
            return Hashes.hex(md.digest()).substring(0, 32);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
