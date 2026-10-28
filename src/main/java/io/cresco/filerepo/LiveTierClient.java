package io.cresco.filerepo;

import io.cresco.library.utilities.CLogger;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
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
 *   no service: it is never asked and never counted present, but it is still tracked, because its root cannot be
 *   learned (see live roots).</li>
 *   <li><b>Coming and going.</b> A {@link ServiceTracker} follows every registration: a provider that appears is
 *   asked from the next call on, one that goes is no longer asked. With several, the highest-ranked one that admits
 *   this bundle is asked.</li>
 *   <li><b>Bounded.</b> Every call runs on a small pool and is waited for at most its timeout. The ops the provider
 *   answers itself ({@code list}, {@code changes}) and those it forwards ({@code realise}, {@code relay}) have a pool
 *   each, so forwarded calls held up elsewhere never starve the local answers. A hung provider costs each caller its
 *   timeout and holds at most {@code maxCalls} threads of each pool; while a pool's threads are all held, further calls
 *   of that kind are refused at once. filerepo's message threads are never wedged.</li>
 * </ul>
 *
 * Ops. A request carries {@code op}; every answer carries {@code status} ({@value #OK} for success) and, on a
 * refusal, {@code status_desc}.
 * <table>
 *   <caption>live-tier ops</caption>
 *   <tr><td>{@code list}, optional {@code row}</td><td>the rows held at this node, {@code rows} [{row, state, mode,
 *   files, bytes, ...}] (a provider may answer a {@code row} with that row alone), and the node's live
 *   {@code root}</td></tr>
 *   <tr><td>{@code changes}, {@code row}</td><td>a workspace row's cached change summary: {@code state} clean or
 *   dirty, {@code changed}, {@code refused}, {@code as_of}, and {@code scanning} while its first walk runs</td></tr>
 *   <tr><td>{@code realise}, {@code params}</td><td>a realise made by the node itself: copies only, at this node
 *   only</td></tr>
 *   <tr><td>{@code relay}, {@code request}</td><td>a request already signed by its end user, forwarded unchanged;
 *   the reply comes back as it came</td></tr>
 * </table>
 * An answer may name paths and collections. It stays in this JVM: callers reply over the fabric with states, counts
 * and ids only.
 *
 * <p><b>Live roots.</b> A provider's {@code list} names the directory its live copies are held under. The client
 * learns the root of every registered provider (once per registration), the caller remembers each one, and
 * {@link #liveRootConflict} lets filerepo refuse to serve, land, delete or sync any file inside one, whatever a
 * repository's configuration: those files are read only through the filesystem at this node, never through filerepo's
 * verbs. While any registered provider's root is not known (it does not admit this bundle, or its {@code list} has
 * not answered), every path fails closed.
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
    private final long localTimeoutMs, coreTimeoutMs, listCacheNanos;
    private final int maxCalls;
    // list and changes are answered by the provider itself; realise and relay wait on whatever it forwards them to
    private final ThreadPoolExecutor localCalls, forwardedCalls;
    // every registration matching FILTER, admitting this bundle or not; null when there is no framework
    private final ServiceTracker<Object, Provider> tracker;
    // a framework was given but its registrations cannot be followed: no path can be checked
    private final boolean trackingFailed;

    // Live roots: learned from list answers and from what the caller remembers, kept while the provider is away.
    private final Set<Path> roots = ConcurrentHashMap.newKeySet();
    private volatile Consumer<String> onNewRoot;
    // the tracking count at which every registered provider's root was known: until it moves, nothing is to learn
    private volatile int allKnownAt = Integer.MIN_VALUE;
    static final long LEARN_RETRY_MS = 30000L;

    // the last successful listing of a row, reused for listCacheNanos: a stream of repostate calls costs the provider
    // at most one list per period
    private volatile Listing lastListing;

    /** A client that never reuses a listing; see {@link #LiveTierClient(BundleContext, CLogger, long, long, int, long)}. */
    public LiveTierClient(BundleContext context, CLogger logger, long localTimeoutMs, long coreTimeoutMs, int maxCalls) {
        this(context, logger, localTimeoutMs, coreTimeoutMs, maxCalls, 0L);
    }

    /**
     * @param context        this bundle's context (the provider admits by its bundle), or null for none
     * @param localTimeoutMs bound on {@code list} and {@code changes}, answered by the provider itself
     * @param coreTimeoutMs  bound on {@code realise} and {@code relay}, which the provider forwards
     * @param maxCalls       provider calls of each kind (local, forwarded) in flight at once; more are refused at once
     * @param listCacheMs    how long a row's successful listing is reused ({@link #listRow}); 0 never
     */
    public LiveTierClient(BundleContext context, CLogger logger, long localTimeoutMs, long coreTimeoutMs, int maxCalls,
                          long listCacheMs) {
        this.context = context;
        this.logger = logger;
        this.localTimeoutMs = Math.max(1L, localTimeoutMs);
        this.coreTimeoutMs = Math.max(1L, coreTimeoutMs);
        this.listCacheNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(0L, listCacheMs));
        this.maxCalls = Math.max(1, maxCalls);
        this.localCalls = pool(this.maxCalls, "filerepo-livetier-local");
        this.forwardedCalls = pool(this.maxCalls, "filerepo-livetier-forwarded");

        ServiceTracker<Object, Provider> t = null;
        boolean failed = false;
        if (context != null) {
            try {
                t = new ServiceTracker<>(context, context.createFilter(FILTER), new Watch());
                t.open();
            } catch (Throwable ex) {
                // filerepo runs without it, but cannot tell whether a provider's files are on this agent: fail closed
                logger.error("live-tier provider tracking unavailable: every file path is refused: " + ex);
                t = null;
                failed = true;
            }
        }
        this.tracker = t;
        this.trackingFailed = failed;
    }

    private static ThreadPoolExecutor pool(int threads, String name) {
        // a SynchronousQueue hands each call to a thread of its own or refuses it: calls never queue behind a hung one
        ThreadPoolExecutor tp = new ThreadPoolExecutor(threads, threads, 30L, TimeUnit.SECONDS, new SynchronousQueue<>(),
                r -> { Thread t = new Thread(r, name); t.setDaemon(true); return t; });
        tp.allowCoreThreadTimeOut(true);
        return tp;
    }

    /** Stop tracking and stop the call pools; a call in flight is interrupted. */
    public void close() {
        try {
            if (tracker != null) tracker.close();
        } catch (Exception ex) {
            logger.error("live-tier tracker close error", ex);
        }
        localCalls.shutdownNow();
        forwardedCalls.shutdownNow();
    }

    /** True while a provider that admits this bundle is registered. */
    public boolean present() {
        return admitting() != null;
    }

    /** The rows held at this node, and the node's live root (which is learned from every successful answer). */
    public Answer list() {
        Provider p = admitting();
        Answer a = call(p, request("list"), localTimeoutMs, localCalls);
        note(p, a);
        return a;
    }

    /**
     * The listing of one row: {@code list} naming the row, which a provider may answer with that row alone (one lookup)
     * or with every row held at this node; the row is found either way. A successful listing is reused for the
     * configured period, so a stream of calls costs the provider at most one {@code list} per period.
     */
    public Listing listRow(String row) {
        Provider p = admitting();
        Listing last = lastListing;
        long now = System.nanoTime();
        if (last != null && p != null && last.from == p && Objects.equals(last.rowId, row) && now - last.atNanos < listCacheNanos) {
            return last;
        }
        Map<String, Object> q = request("list");
        if (row != null) q.put("row", row);
        Answer a = call(p, q, localTimeoutMs, localCalls);
        note(p, a);
        Listing l = new Listing(a, row(a, row), p, row, now);
        if (a.ok() && listCacheNanos > 0) lastListing = l;
        return l;
    }

    /** A workspace row's cached change summary: no walk, whatever the workspace's size. */
    public Answer changes(String row) {
        Map<String, Object> q = request("changes");
        q.put("row", row);
        return call(admitting(), q, localTimeoutMs, localCalls);
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
        return call(admitting(), q, coreTimeoutMs, forwardedCalls);
    }

    /** Forward a request already signed by its end user, unchanged; the reply is returned as it came. */
    public Answer relay(Map<String, String> signedRequest) {
        if (signedRequest == null || signedRequest.isEmpty()) {
            return Answer.none(REFUSED, "a relay carries a signed request");
        }
        Map<String, Object> q = request("relay");
        q.put("request", new LinkedHashMap<>(signedRequest));
        return call(admitting(), q, coreTimeoutMs, forwardedCalls);
    }

    /** The row named {@code row} in a successful {@code list} answer, or null. Costs O(rows in the answer). */
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

    // ------------------------------------------------------------------ live roots

    /**
     * Seed the live roots known from before or from configuration (they hold while no provider is registered), and be
     * told of each root learned from now on, to remember it.
     */
    public void rememberRoots(Collection<String> known, Consumer<String> onNew) {
        if (known != null) {
            for (String r : known) {
                if (r == null || r.trim().isEmpty()) continue;
                try {
                    roots.add(canonical(Paths.get(r.trim())));
                } catch (RuntimeException bad) {
                    logger.error("a live-tier root that is not a path is ignored: " + bad.getMessage());
                }
            }
        }
        this.onNewRoot = onNew;
    }

    /**
     * Why {@code p} may not be touched through filerepo, as a phrase ("lies in a live-tier root", "contains a live-tier
     * root", or "cannot be checked: ..." while a registered provider's root is not known: fail closed), or null when it
     * may. The path is resolved as the filesystem resolves it (see {@link #canonical}), so a link into a root is
     * caught. O(live roots + registered providers), not O(files).
     */
    public String liveRootConflict(Path p) {
        if (p == null) return null;
        String unknown = learn();
        if (unknown != null) return "cannot be checked: " + unknown;
        if (roots.isEmpty()) return null;
        Path c = canonical(p);
        for (Path r : roots) {
            if (c.startsWith(r)) return "lies in a live-tier root";
            if (r.startsWith(c)) return "contains a live-tier root";
        }
        return null;
    }

    /**
     * Learn the root of each registered provider not learned yet (a failed attempt is retried after a pause, or when
     * the provider registers again). Returns why a path cannot be checked now, or null when every registered
     * provider's root is known; once they all are, nothing more is done until a registration changes.
     */
    private String learn() {
        ServiceTracker<Object, Provider> t = tracker;
        if (t == null) return trackingFailed ? "live-tier provider tracking is unavailable" : null;
        int tc = t.getTrackingCount();
        if (tc == allKnownAt) return null;
        String unknown = null;
        for (Provider p : t.getTracked().values()) {
            if (p.root != null) continue;
            if (p.fn == null) {
                unknown = "a live-tier provider is registered that does not admit this bundle, so its root cannot be learned";
                continue;
            }
            // one learner per provider: the others wait for it (at most its timeout) rather than each asking
            synchronized (p) {
                if (p.root == null && (!p.failed || System.nanoTime() - p.failedAtNanos >= TimeUnit.MILLISECONDS.toNanos(LEARN_RETRY_MS))) {
                    note(p, call(p, request("list"), localTimeoutMs, localCalls));
                    if (p.root == null) {
                        p.failedAtNanos = System.nanoTime();
                        p.failed = true;
                    }
                }
            }
            if (p.root == null && unknown == null) {
                unknown = "a live-tier provider is registered and its root is not known yet";
            }
        }
        if (unknown == null) allKnownAt = tc;
        return unknown;
    }

    /** Learn {@code p}'s root from its successful list answer {@code a}, and remember a root not known before. */
    private void note(Provider p, Answer a) {
        if (p == null || a == null || !a.ok()) return;
        Object root = a.reply.get("root");
        if (root == null || String.valueOf(root).trim().isEmpty()) return;
        Path r;
        try {
            r = canonical(Paths.get(String.valueOf(root).trim()));
        } catch (RuntimeException bad) {
            logger.error("a live-tier provider named a root that is not a path: " + bad.getMessage());
            return;
        }
        if (roots.add(r)) {
            logger.info("live-tier root learned: filerepo serves, lands, deletes and syncs nothing under it");
            Consumer<String> remember = onNewRoot;
            if (remember != null) {
                try {
                    remember.accept(r.toString());
                } catch (Exception ex) {
                    logger.error("could not remember a live-tier root: " + ex.getMessage());
                }
            }
        }
        // known (and guarded) before it counts as learned
        p.root = r;
    }

    /**
     * {@code p} as the filesystem resolves it: the real path of its nearest existing ancestor, then the rest name by
     * name, a {@code ..} taking the physical parent and every name that exists resolved again. Never normalized
     * lexically first: {@code dir/link/../x} is where {@code link}'s target's parent says, not {@code dir/x}.
     */
    static Path canonical(Path p) {
        Path abs = p.toAbsolutePath();
        java.util.Deque<Path> below = new java.util.ArrayDeque<>();
        Path real = null;
        for (Path cur = abs; cur != null; cur = cur.getParent()) {
            try {
                real = cur.toRealPath();
                break;
            } catch (IOException | SecurityException notThere) {
                if (cur.getFileName() == null) break;
                below.push(cur.getFileName());
            }
        }
        if (real == null) return abs.normalize();
        for (Path name : below) {
            String n = name.toString();
            if (n.equals(".")) continue;
            if (n.equals("..")) {
                if (real.getParent() != null) real = real.getParent();
                continue;
            }
            real = real.resolve(n);
            try {
                real = real.toRealPath();
            } catch (IOException | SecurityException notThere) {
                // does not exist (yet): kept as named
            }
        }
        return real;
    }

    private static Map<String, Object> request(String op) {
        Map<String, Object> q = new LinkedHashMap<>();
        q.put("op", op);
        return q;
    }

    /** The provider asked: the highest-ranked registration that admits this bundle, or null. */
    private Provider admitting() {
        if (tracker == null) return null;
        Provider best = null;
        for (Provider p : tracker.getTracked().values()) {
            if (p.fn != null && (best == null || p.ref.compareTo(best.ref) > 0)) best = p;
        }
        return best;
    }

    private Answer call(Provider p, Map<String, Object> request, long timeoutMs, ThreadPoolExecutor pool) {
        if (p == null || p.fn == null) {
            return Answer.none(ABSENT, "no live-tier provider admits this bundle");
        }
        Function<Map<String, Object>, Map<String, Object>> f = p.fn;
        Future<Map<String, Object>> fut;
        try {
            fut = pool.submit(() -> f.apply(request));
        } catch (RejectedExecutionException full) {
            return Answer.none(BUSY, maxCalls + " live-tier " + (pool == localCalls ? "local" : "forwarded")
                    + " call(s) already in flight");
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

    /** One registration of a provider: its function (null when it does not admit this bundle) and its learned root. */
    private static final class Provider {
        final ServiceReference<Object> ref;
        final Function<Map<String, Object>, Map<String, Object>> fn;
        volatile Path root;
        volatile boolean failed;
        volatile long failedAtNanos;

        Provider(ServiceReference<Object> ref, Function<Map<String, Object>, Map<String, Object>> fn) {
            this.ref = ref;
            this.fn = fn;
        }
    }

    /**
     * Follows every provider registration. One that does not admit this bundle is tracked all the same: while it is
     * registered its root cannot be learned, and every path fails closed.
     */
    private final class Watch implements ServiceTrackerCustomizer<Object, Provider> {
        @SuppressWarnings("unchecked")
        @Override public Provider addingService(ServiceReference<Object> ref) {
            Object s = null;
            try {
                s = context.getService(ref);
            } catch (Exception ex) {
                logger.warn("live-tier provider could not be got: " + ex);
            }
            if (s instanceof Function) {
                logger.info("live-tier provider available");
                return new Provider(ref, (Function<Map<String, Object>, Map<String, Object>>) s);
            }
            if (s != null) {
                try { context.ungetService(ref); } catch (Exception ignore) { }
            }
            logger.warn("a live-tier provider is registered but does not admit this bundle: its root cannot be learned,"
                    + " so every file path is refused while it is registered");
            return new Provider(ref, null);
        }

        @Override public void modifiedService(ServiceReference<Object> ref, Provider p) { }

        @Override public void removedService(ServiceReference<Object> ref, Provider p) {
            logger.info("live-tier provider gone");
            if (p.fn != null) {
                try { context.ungetService(ref); } catch (Exception ignore) { }
            }
        }
    }

    /** One row's listing: the provider's {@code list} answer, and the row it names (null when not held, or no answer). */
    public static final class Listing {
        /** The provider's list answer, or why there is none. */
        public final Answer answer;
        /** The row, as the provider listed it; null when it is not held at this node or there is no answer. */
        public final Map<String, Object> row;
        private final Provider from;
        private final String rowId;
        private final long atNanos;

        private Listing(Answer answer, Map<String, Object> row, Provider from, String rowId, long atNanos) {
            this.answer = answer;
            this.row = row;
            this.from = from;
            this.rowId = rowId;
            this.atNanos = atNanos;
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
