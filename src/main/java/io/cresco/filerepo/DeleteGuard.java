package io.cresco.filerepo;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
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
 *   looks like where device ids cannot tell it, while an ordinary {@code rm -rf} of a small directory goes through;</li>
 *   <li>everything, when the loss (beyond the above) is at least {@code min} files AND more than {@code fraction} of
 *   the catalog.</li>
 * </ul>
 * What is held is identified by a digest of exactly those deletes. An admin's release must name that digest, is
 * spent by the next evaluation whatever it finds, and expires: it never applies to a later or another loss.
 */
final class DeleteGuard {

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

    // the hold now in force (from the last evaluation), and an admin's armed release
    private volatile String holdId;
    private volatile int heldCount;
    private volatile String releaseId;
    private volatile long releaseExpires;

    DeleteGuard(double fraction, int min, int subtreeMin, double subtreeFraction, long releaseTtlMs) {
        this.fraction = fraction;
        this.min = Math.max(1, min);
        this.subtreeMin = Math.max(1, subtreeMin);
        this.subtreeFraction = subtreeFraction;
        this.releaseTtlMs = releaseTtlMs;
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
     * Arm a release of the current hold, which {@code id} must name. The count released, 0 when nothing is held, -1 when
     * {@code id} names another set, {@link #RELEASE_NEEDS_ID} when it names none.
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
     * itself meaning the whole scan dir.
     */
    synchronized Decision evaluate(Path root, Collection<String> catalogUnderRoot, List<DBEngine.Tombstone> gone, int seen,
                                   Collection<String> vanishedMounts, long now) {
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
            if (n > 0) why.add(n + " under " + (all ? "the scan dir, which is no longer on its device" : "the vanished mount " + m));
        }

        // 2. whole subtrees that lost every cataloged file
        Map<Path, int[]> dirs = new HashMap<>();   // dir -> {cataloged, gone}
        for (String p : catalogUnderRoot) count(dirs, root, Paths.get(p), 0);
        for (String p : byPath.keySet()) count(dirs, root, Paths.get(p), 1);
        List<Path> lost = new ArrayList<>();
        int size = catalogUnderRoot.size();
        for (Map.Entry<Path, int[]> e : dirs.entrySet()) {
            int[] c = e.getValue();
            if (c[1] == c[0] && c[0] >= subtreeMin && c[0] > subtreeFraction * size) lost.add(e.getKey());
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
            if (n > 0) why.add("the whole subtree " + root.relativize(d) + " (" + n + " files)");
        }

        // 3. an empty scan dir, or a large fraction of the catalog
        int rest = byPath.size() - held.size();
        boolean empty = seen == 0 && !catalogUnderRoot.isEmpty();
        // at least min files and more than the fraction of the catalog
        boolean large = rest >= min && rest > fraction * catalogUnderRoot.size();
        if (empty || (rest > 0 && large)) {
            held.addAll(byPath.keySet());
            why.add(empty ? "the scan dir is empty" : rest + " of " + catalogUnderRoot.size() + " cataloged files (more than "
                    + Math.round(fraction * 100) + "%)");
        }

        List<DBEngine.Tombstone> record = new ArrayList<>();
        List<DBEngine.Tombstone> holding = new ArrayList<>();
        for (Map.Entry<String, DBEngine.Tombstone> e : byPath.entrySet()) {
            (held.contains(e.getKey()) ? holding : record).add(e.getValue());
        }
        if (holding.isEmpty()) {
            holdId = null;
            heldCount = 0;
            return new Decision(record, holding, null, null, false);
        }
        String id = digest(holding);
        if (id.equals(armed)) {
            // exactly the set an admin released: recorded, once
            holdId = null;
            heldCount = 0;
            record.addAll(holding);
            return new Decision(record, List.of(), id, String.join("; ", why), true);
        }
        holdId = id;
        heldCount = holding.size();
        return new Decision(record, holding, id, String.join("; ", why), false);
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
            dirs.computeIfAbsent(d, k -> new int[2])[slot]++;
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
