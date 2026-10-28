package io.cresco.filerepo;

import io.cresco.library.utilities.CLogger;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * filerepo's client of a live-tier provider: an in-JVM service that another bundle on this agent may register in the
 * OSGi service registry as a {@code java.util.function.Function<Map<String,Object>,Map<String,Object>>} carrying the
 * property {@code gfs.livetier=local}. filerepo embeds none of the provider's code: it asks through that one function
 * with plain maps, and without a provider it works exactly as before.
 *
 * <ul>
 *   <li><b>Who may ask.</b> The provider admits a caller by the bundle the framework names when the service is got
 *   (this bundle's context), never by anything a request carries. A provider that does not admit this bundle hands it
 *   no service, which looks here exactly like no provider.</li>
 *   <li><b>Coming and going.</b> A {@link ServiceTracker} follows the registration: a provider that appears is asked
 *   from the next call on, one that goes is no longer asked.</li>
 *   <li><b>Bounded.</b> Every call runs on a small pool of its own and is waited for at most its timeout. A hung
 *   provider costs each caller its timeout and holds at most {@code maxCalls} threads; while all of them are held,
 *   further calls are refused at once. filerepo's message threads are never wedged.</li>
 * </ul>
 *
 * Ops. A request carries {@code op}; every answer carries {@code status} ({@value #OK} for success) and, on a
 * refusal, {@code status_desc}.
 * <table>
 *   <caption>live-tier ops</caption>
 *   <tr><td>{@code list}</td><td>the rows held at this node, {@code rows} [{row, state, mode, files, bytes, ...}],
 *   and the node's live {@code root}</td></tr>
 *   <tr><td>{@code changes}, {@code row}</td><td>a workspace row's cached change summary: {@code state} clean or
 *   dirty, {@code changed}, {@code refused}, {@code as_of}, and {@code scanning} while its first walk runs</td></tr>
 *   <tr><td>{@code realise}, {@code params}</td><td>a realise made by the node itself: copies only, at this node
 *   only</td></tr>
 *   <tr><td>{@code relay}, {@code request}</td><td>a request already signed by its end user, forwarded unchanged;
 *   the reply comes back as it came</td></tr>
 * </table>
 * An answer may name paths and collections. It stays in this JVM: callers reply over the fabric with states, counts
 * and ids only.
 */
public class LiveTierClient {

    /** The provider's registration: a Function service carrying {@code gfs.livetier=local}. */
    public static final String FILTER = "(&(objectClass=java.util.function.Function)(gfs.livetier=local))";
    /** The status of a provider answer that succeeded. */
    public static final String OK = "10";
    /** Why a call has no answer. */
    public static final String ABSENT = "absent", TIMEOUT = "timeout", BUSY = "busy", ERROR = "error", REFUSED = "refused";

    private final BundleContext context;
    private final CLogger logger;
    private final long localTimeoutMs, coreTimeoutMs;
    private final int maxCalls;
    private final ThreadPoolExecutor calls;
    // null when there is no framework (no provider, ever)
    private final ServiceTracker<Object, Object> tracker;

    /**
     * @param context        this bundle's context (the provider admits by its bundle), or null for none
     * @param localTimeoutMs bound on {@code list} and {@code changes}, answered by the provider itself
     * @param coreTimeoutMs  bound on {@code realise} and {@code relay}, which the provider forwards
     * @param maxCalls       provider calls in flight at once; more are refused at once
     */
    public LiveTierClient(BundleContext context, CLogger logger, long localTimeoutMs, long coreTimeoutMs, int maxCalls) {
        this.context = context;
        this.logger = logger;
        this.localTimeoutMs = Math.max(1L, localTimeoutMs);
        this.coreTimeoutMs = Math.max(1L, coreTimeoutMs);
        this.maxCalls = Math.max(1, maxCalls);
        // a SynchronousQueue hands each call to a thread of its own or refuses it: calls never queue behind a hung one
        ThreadPoolExecutor tp = new ThreadPoolExecutor(this.maxCalls, this.maxCalls, 30L, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                r -> { Thread t = new Thread(r, "filerepo-livetier"); t.setDaemon(true); return t; });
        tp.allowCoreThreadTimeOut(true);
        this.calls = tp;

        ServiceTracker<Object, Object> t = null;
        if (context != null) {
            try {
                t = new ServiceTracker<>(context, context.createFilter(FILTER), new Watch());
                t.open();
            } catch (Throwable ex) {
                // the tracker is optional: without it filerepo runs as it always has
                logger.error("live-tier provider tracking unavailable: " + ex);
                t = null;
            }
        }
        this.tracker = t;
    }

    /** Stop tracking and stop the call pool; a call in flight is interrupted. */
    public void close() {
        try {
            if (tracker != null) tracker.close();
        } catch (Exception ex) {
            logger.error("live-tier tracker close error", ex);
        }
        calls.shutdownNow();
    }

    /** True while a provider that admits this bundle is registered. */
    public boolean present() {
        return provider() != null;
    }

    /** The rows held at this node, and the node's live root. */
    public Answer list() {
        return call(request("list"), localTimeoutMs);
    }

    /** A workspace row's cached change summary: no walk, whatever the workspace's size. */
    public Answer changes(String row) {
        Map<String, Object> q = request("changes");
        q.put("row", row);
        return call(q, localTimeoutMs);
    }

    /**
     * A realise made by the node itself: copies only, at this node only. Params naming another mode or a node are
     * refused here, before anything is sent; the provider holds the same line.
     */
    public Answer realise(Map<String, String> params) {
        if (params == null || params.containsKey("node") || params.containsKey("nodes")) {
            return Answer.none(REFUSED, "a realise through filerepo is at this node only");
        }
        String mode = params.get("mode");
        if (mode != null && !"copy".equals(mode)) {
            return Answer.none(REFUSED, "a realise through filerepo makes copies only");
        }
        Map<String, String> p = new LinkedHashMap<>(params);
        p.put("mode", "copy");
        Map<String, Object> q = request("realise");
        q.put("params", p);
        return call(q, coreTimeoutMs);
    }

    /** Forward a request already signed by its end user, unchanged; the reply is returned as it came. */
    public Answer relay(Map<String, String> signedRequest) {
        if (signedRequest == null || signedRequest.isEmpty()) {
            return Answer.none(REFUSED, "a relay carries a signed request");
        }
        Map<String, Object> q = request("relay");
        q.put("request", new LinkedHashMap<>(signedRequest));
        return call(q, coreTimeoutMs);
    }

    /** The row named {@code row} in a successful {@code list} answer, or null. Costs O(rows held at this node). */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> row(Answer list, String row) {
        if (list == null || !list.ok() || row == null) return null;
        Object rows = list.reply.get("rows");
        if (!(rows instanceof Collection)) return null;
        for (Object o : (Collection<?>) rows) {
            if (o instanceof Map && row.equals(String.valueOf(((Map<?, ?>) o).get("row")))) {
                return (Map<String, Object>) o;
            }
        }
        return null;
    }

    private static Map<String, Object> request(String op) {
        Map<String, Object> q = new LinkedHashMap<>();
        q.put("op", op);
        return q;
    }

    @SuppressWarnings("unchecked")
    private Function<Map<String, Object>, Map<String, Object>> provider() {
        if (tracker == null) return null;
        Object s = tracker.getService();
        return s instanceof Function ? (Function<Map<String, Object>, Map<String, Object>>) s : null;
    }

    private Answer call(Map<String, Object> request, long timeoutMs) {
        Function<Map<String, Object>, Map<String, Object>> f = provider();
        if (f == null) {
            return Answer.none(ABSENT, "no live-tier provider admits this bundle");
        }
        Future<Map<String, Object>> fut;
        try {
            fut = calls.submit(() -> f.apply(request));
        } catch (RejectedExecutionException full) {
            return Answer.none(BUSY, maxCalls + " live-tier call(s) already in flight");
        }
        try {
            Map<String, Object> reply = fut.get(timeoutMs, TimeUnit.MILLISECONDS);
            return reply == null ? Answer.none(ERROR, "the provider answered nothing") : Answer.of(reply);
        } catch (TimeoutException slow) {
            fut.cancel(true);
            logger.warn("live-tier " + request.get("op") + ": no answer in " + timeoutMs + " ms");
            return Answer.none(TIMEOUT, "no answer in " + timeoutMs + " ms");
        } catch (ExecutionException failed) {
            Throwable c = failed.getCause() == null ? failed : failed.getCause();
            logger.warn("live-tier " + request.get("op") + " failed: " + c);
            return Answer.none(ERROR, request.get("op") + " failed: " + c.getClass().getSimpleName());
        } catch (InterruptedException stop) {
            fut.cancel(true);
            Thread.currentThread().interrupt();
            return Answer.none(ERROR, "interrupted");
        }
    }

    /** Logs the provider's coming and going; a provider that does not admit this bundle is not tracked. */
    private final class Watch implements ServiceTrackerCustomizer<Object, Object> {
        @Override public Object addingService(ServiceReference<Object> ref) {
            Object s;
            try {
                s = context.getService(ref);
            } catch (Exception ex) {
                logger.warn("live-tier provider could not be got: " + ex);
                return null;
            }
            if (!(s instanceof Function)) {
                if (s != null) context.ungetService(ref);
                logger.warn("a live-tier provider is registered but does not admit this bundle");
                return null;
            }
            logger.info("live-tier provider available");
            return s;
        }

        @Override public void modifiedService(ServiceReference<Object> ref, Object service) { }

        @Override public void removedService(ServiceReference<Object> ref, Object service) {
            logger.info("live-tier provider gone");
            try { context.ungetService(ref); } catch (Exception ignore) { }
        }
    }

    /** One call's outcome: the provider's answer as it came, or why there is none. */
    public static final class Answer {
        /** The provider's answer; null when there is none. */
        public final Map<String, Object> reply;
        /** Why there is no answer ({@link #ABSENT}, {@link #TIMEOUT}, {@link #BUSY}, {@link #ERROR}, {@link #REFUSED}); null when answered. */
        public final String failure;
        private final String why;

        private Answer(Map<String, Object> reply, String failure, String why) {
            this.reply = reply;
            this.failure = failure;
            this.why = why;
        }

        static Answer of(Map<String, Object> reply) { return new Answer(reply, null, null); }

        static Answer none(String failure, String why) { return new Answer(null, failure, why); }

        public boolean answered() { return reply != null; }

        /** Answered with status {@value #OK}. */
        public boolean ok() { return reply != null && OK.equals(String.valueOf(reply.get("status"))); }

        /** The provider's status, or null when there is no answer. */
        public String status() { return reply == null ? null : String.valueOf(reply.get("status")); }

        /** The provider's status_desc, or why there is no answer. */
        public String desc() {
            if (reply == null) return why;
            Object d = reply.get("status_desc");
            return d == null ? "" : String.valueOf(d);
        }
    }
}
