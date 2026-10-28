package io.cresco.filerepo;

import io.cresco.library.messaging.MsgEvent;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * What a repository is, as {@code repostate} answers and {@code clearrepo} checks it: what would be lost with it.
 * It comes from the live-tier provider's cached answers (a live-backed repository) or from configuration and filerepo's
 * own catalog (any other), never from markers in the tree or a walk, so its cost does not grow with the number of
 * files.
 * <table>
 *   <caption>repository states</caption>
 *   <tr><td>{@code LIVE}</td><td>backed by a live copy: read-only here, recreatable by a new realise</td></tr>
 *   <tr><td>{@code WORKSPACE_CLEAN}</td><td>backed by a workspace with no unpublished change</td></tr>
 *   <tr><td>{@code WORKSPACE_DIRTY}</td><td>backed by a workspace with unpublished changes, or changes not yet seen
 *   (never counted clean unseen)</td></tr>
 *   <tr><td>{@code MIRROR}</td><td>a subscriber mirror: what it holds, its producer holds</td></tr>
 *   <tr><td>{@code ONLY_COPY}</td><td>the origin of what it holds: a producer, an on-demand repository, or a mirror
 *   that files were landed in directly</td></tr>
 * </table>
 * Its facts are states, counts and ids, never a path: a reply crosses the fabric.
 */
final class RepoState {

    static final String LIVE = "LIVE", WORKSPACE_CLEAN = "WORKSPACE_CLEAN", WORKSPACE_DIRTY = "WORKSPACE_DIRTY",
            MIRROR = "MIRROR", ONLY_COPY = "ONLY_COPY";

    /** repostate statuses: a state; the live row is not held at this node; the live-tier provider did not answer. */
    static final String ANSWERED = "10", NOT_HELD = "9", UNAVAILABLE = "11";

    /** One of the five states; null when it cannot be told now. */
    final String state;
    final String status;
    final String desc;
    final Map<String, String> facts;

    private RepoState(String state, String status, String desc, Map<String, String> facts) {
        this.state = state;
        this.status = status;
        this.desc = desc;
        this.facts = facts;
    }

    static RepoState of(String state, Map<String, String> facts, String desc) {
        return new RepoState(state, ANSWERED, desc, facts);
    }

    /**
     * A live-backed repository's state, from the provider: its row in {@code list} (O(rows held at this node)) and, for
     * a workspace, the cached {@code changes} summary (no walk). A workspace whose summary cannot be read is counted
     * dirty: it may hold the only copy of unpublished work.
     */
    static RepoState live(String row, LiveTierClient client) {
        Map<String, String> f = new LinkedHashMap<>();
        f.put("backing", "live");
        f.put("live_row", row);
        if (client == null) {
            f.put("provider", LiveTierClient.ABSENT);
            return new RepoState(null, UNAVAILABLE, "no live-tier provider admits this bundle", f);
        }
        LiveTierClient.Answer list = client.list();
        if (!list.answered()) {
            f.put("provider", list.failure);
            return new RepoState(null, UNAVAILABLE, "the live-tier provider did not answer: " + list.desc(), f);
        }
        if (!list.ok()) {
            f.put("provider_status", list.status());
            return new RepoState(null, UNAVAILABLE, "the live-tier provider refused list (status " + list.status() + ")", f);
        }
        Map<String, Object> r = LiveTierClient.row(list, row);
        if (r == null) {
            return new RepoState(null, NOT_HELD, "live row " + row + " is not held at this node", f);
        }
        String mode = str(r.get("mode"));
        f.put("mode", mode);
        f.put("row_state", str(r.get("state")));
        putIfPresent(f, "files", r.get("files"));
        putIfPresent(f, "bytes", r.get("bytes"));
        if ("copy".equals(mode)) {
            putIfPresent(f, "lease_until", r.get("lease_until"));
            if (Boolean.TRUE.equals(r.get("damaged")) || "true".equals(str(r.get("damaged")))) f.put("damaged", "true");
            return of(LIVE, f, "backed by a live copy: read-only here, recreatable by a new realise");
        }
        // anything not known to be a copy is treated as a workspace: never called recreatable unseen
        LiveTierClient.Answer ch = client.changes(row);
        if (!ch.ok()) {
            f.put("changes", "unknown");
            return of(WORKSPACE_DIRTY, f, "backed by a workspace whose changes cannot be read now"
                    + (ch.answered() ? " (status " + ch.status() + ")" : " (" + ch.failure + ")")
                    + ": counted dirty, never clean unseen");
        }
        boolean scanning = Boolean.TRUE.equals(ch.reply.get("scanning")) || "true".equals(str(ch.reply.get("scanning")));
        putIfPresent(f, "changed", ch.reply.get("changed"));
        putIfPresent(f, "refused", ch.reply.get("refused"));
        putIfPresent(f, "as_of", ch.reply.get("as_of"));
        if (scanning) f.put("scanning", "true");
        if ("clean".equals(str(ch.reply.get("state"))) && !scanning) {
            return of(WORKSPACE_CLEAN, f, "backed by a workspace with no unpublished change");
        }
        return of(WORKSPACE_DIRTY, f, scanning ? "backed by a workspace whose first walk is still running: counted dirty"
                : "backed by a workspace with unpublished changes: the only copy of them");
    }

    /** Write this state onto a reply. */
    void into(MsgEvent reply) {
        reply.setParam("status", status);
        reply.setParam("status_desc", desc);
        if (state != null) reply.setParam("state", state);
        for (Map.Entry<String, String> e : facts.entrySet()) {
            if (e.getValue() != null) reply.setParam(e.getKey(), e.getValue());
        }
    }

    private static void putIfPresent(Map<String, String> f, String key, Object v) {
        if (v != null) f.put(key, str(v));
    }

    private static String str(Object o) {
        return o == null ? null : String.valueOf(o);
    }

    @Override public String toString() {
        return (state == null ? "status " + status : state) + " " + facts;
    }
}
