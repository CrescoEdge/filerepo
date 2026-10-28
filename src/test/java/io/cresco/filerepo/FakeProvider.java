package io.cresco.filerepo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * A live-tier provider as filerepo sees one: the four ops over plain maps, status "10" for success and a code with
 * status_desc for a refusal. Rows and change summaries are set by the test; realise and relay answer through
 * replaceable functions; {@link #hang} makes every call block, ignoring interrupts, until released.
 */
final class FakeProvider implements Function<Map<String, Object>, Map<String, Object>> {

    /** Every request, as it arrived. */
    final List<Map<String, Object>> requests = Collections.synchronizedList(new ArrayList<>());
    final Map<String, Map<String, Object>> rows = Collections.synchronizedMap(new LinkedHashMap<>());
    final Map<String, Map<String, Object>> changes = new ConcurrentHashMap<>();
    volatile String root = "/nonexistent/live-root";
    volatile CountDownLatch hang;
    /** Calls in flight now (a hung call counts until it is released). */
    final AtomicInteger inFlight = new AtomicInteger();
    volatile Function<Map<String, String>, Map<String, Object>> realise = p -> answer("10", "job_id", "j_realise_1");
    volatile Function<Map<String, String>, Map<String, Object>> relay = r -> answer("10", "echoed", "1");
    volatile RuntimeException fail;
    /** Answer a list naming a row with that row alone, as a provider may; otherwise every row, whatever is named. */
    volatile boolean listsOneRow;

    @SuppressWarnings("unchecked")
    @Override public Map<String, Object> apply(Map<String, Object> req) {
        requests.add(new LinkedHashMap<>(req));
        inFlight.incrementAndGet();
        try {
            CountDownLatch h = hang;
            while (h != null && h.getCount() > 0) {
                try {
                    h.await();
                } catch (InterruptedException ignored) {
                    // a wedged provider does not answer interrupts either
                }
            }
            if (fail != null) throw fail;
            Object op = req.get("op");
            if ("list".equals(op)) {
                Map<String, Object> out = answer("10");
                synchronized (rows) {
                    List<Map<String, Object>> view = new ArrayList<>();
                    Object named = req.get("row");
                    for (Map<String, Object> r : rows.values()) {
                        if (!listsOneRow || named == null || named.equals(r.get("row"))) view.add(new LinkedHashMap<>(r));
                    }
                    out.put("rows", view);
                }
                out.put("root", root);
                return out;
            }
            if ("changes".equals(op)) {
                Map<String, Object> c = changes.get(String.valueOf(req.get("row")));
                if (c == null) return answer("9", "status_desc", "unknown row");
                Map<String, Object> out = new LinkedHashMap<>(c);
                out.put("status", "10");
                return out;
            }
            if ("realise".equals(op)) return realise.apply((Map<String, String>) req.get("params"));
            if ("relay".equals(op)) return relay.apply((Map<String, String>) req.get("request"));
            return answer("8", "status_desc", "unknown op " + op);
        } finally {
            inFlight.decrementAndGet();
        }
    }

    /** A copy row, as a provider lists one. */
    Map<String, Object> copy(String row) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("row", row);
        r.put("state", "LIVE");
        r.put("mode", "copy");
        r.put("tenant", "tenant-a");
        r.put("collection", "collection-a");
        r.put("vid", "v1");
        r.put("prefix", "data/");
        r.put("path", "tenant-a/collection-a/v1");
        r.put("files", 3);
        r.put("bytes", 300L);
        r.put("damaged", false);
        r.put("lease_until", 1234567L);
        r.put("pin_until", 0L);
        rows.put(row, r);
        return r;
    }

    /** A workspace row, as a provider lists one, with a clean change summary. */
    Map<String, Object> workspace(String row) {
        Map<String, Object> r = copy(row);
        r.put("mode", "workspace");
        r.put("path", "tenant-a/collection-a/ws/" + row);
        r.put("lease_until", 0L);
        summary(row, "clean", 0, false);
        return r;
    }

    void summary(String row, String state, int changed, boolean scanning) {
        Map<String, Object> c = new LinkedHashMap<>();
        c.put("state", state);
        c.put("changed", changed);
        c.put("refused", 0);
        c.put("as_of", 42L);
        if (scanning) c.put("scanning", true);
        changes.put(row, c);
    }

    static Map<String, Object> answer(String status, Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("status", status);
        for (int i = 0; i + 1 < kv.length; i += 2) m.put(String.valueOf(kv[i]), kv[i + 1]);
        return m;
    }

    /** The props a live-tier provider registers with. */
    static Map<String, Object> props() {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("gfs.livetier", "local");
        return p;
    }
}
