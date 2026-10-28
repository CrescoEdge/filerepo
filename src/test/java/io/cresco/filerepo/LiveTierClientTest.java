package io.cresco.filerepo;

import io.cresco.library.utilities.CLogger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.osgi.framework.ServiceReference;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The live-tier client against a real ServiceTracker over a fake registry: a provider found only by its filter, asked
 * as this bundle (and free to refuse it), tolerated absent, followed as it comes and goes, and bounded when it hangs.
 * The requests it sends are exactly the documented maps.
 */
class LiveTierClientTest {

    static final String SELF = "io.cresco.filerepo";

    final FakeRegistry registry = new FakeRegistry();
    final FakeProvider provider = new FakeProvider();
    final List<String> logged = Collections.synchronizedList(new ArrayList<>());
    LiveTierClient client;

    @AfterEach
    void down() {
        if (client != null) client.close();
        CountDownLatch h = provider.hang;
        if (h != null) h.countDown();
    }

    CLogger logger() {
        return (CLogger) Proxy.newProxyInstance(CLogger.class.getClassLoader(), new Class<?>[]{CLogger.class}, (p, m, a) -> {
            if (a != null && a.length > 0) logged.add(m.getName() + ": " + a[0]);
            return null;
        });
    }

    LiveTierClient client(String bundle, long localMs, long coreMs, int maxCalls) {
        client = new LiveTierClient(registry.contextOf(bundle), logger(), localMs, coreMs, maxCalls);
        return client;
    }

    /** A provider admitting only {@code admitted}, as a ServiceFactory keyed on the asking bundle does. */
    ServiceReference<?> registerProvider(FakeProvider p, String admitted) {
        return registry.register(FakeProvider.props(), b -> admitted.equals(b.getSymbolicName()) ? p : null);
    }

    @Test
    void withNoProviderEveryOpIsAbsentAndNothingBreaks() {
        LiveTierClient c = client(SELF, 1000, 1000, 2);
        assertFalse(c.present());
        for (LiveTierClient.Answer a : List.of(c.list(), c.changes("lt_1"), c.realise(Map.of("collection", "c", "vid", "v")),
                c.relay(Map.of("k1", "v1")))) {
            assertFalse(a.answered());
            assertFalse(a.ok());
            assertEquals(LiveTierClient.ABSENT, a.failure);
            assertNull(a.status());
        }
        LiveTierClient none = new LiveTierClient(null, logger(), 1000, 1000, 2);
        try {
            assertFalse(none.present(), "no framework: no provider, ever");
            assertEquals(LiveTierClient.ABSENT, none.list().failure);
        } finally {
            none.close();
        }
    }

    @Test
    void theProviderIsFoundByItsFilterAlone() {
        LiveTierClient c = client(SELF, 1000, 1000, 2);
        registry.register(Map.of(), b -> provider);                                      // a Function without the property
        registry.register(Map.of("gfs.livetier", "remote"), b -> provider);              // the property, another value
        registry.register(new String[]{Runnable.class.getName()}, FakeProvider.props(), b -> (Runnable) () -> { });
        assertFalse(c.present(), "only a Function carrying gfs.livetier=local is the provider");
        assertEquals(LiveTierClient.ABSENT, c.list().failure);
        registerProvider(provider, SELF);
        assertTrue(c.present());
        assertTrue(c.list().ok());
    }

    @Test
    void theProviderAdmitsByTheAskingBundleAndMayRefuseIt() {
        registerProvider(provider, SELF);
        LiveTierClient other = new LiveTierClient(registry.contextOf("io.cresco.other"), logger(), 1000, 1000, 2);
        try {
            assertFalse(other.present(), "a bundle the provider does not admit gets no service");
            assertEquals(LiveTierClient.ABSENT, other.list().failure);
        } finally {
            other.close();
        }
        LiveTierClient c = client(SELF, 1000, 1000, 2);
        assertTrue(c.present());
        assertTrue(c.list().ok());
        assertTrue(registry.askedBy.contains("io.cresco.other") && registry.askedBy.contains(SELF),
                "the factory saw who asked: " + registry.askedBy);
        assertTrue(logged.stream().anyMatch(l -> l.contains("does not admit this bundle")), String.valueOf(logged));
    }

    @Test
    void aProviderThatDoesNotAdmitThisBundleIsTrackedAndEveryPathFailsClosed() {
        ServiceReference<?> refusing = registerProvider(provider, "io.cresco.other");
        LiveTierClient c = client(SELF, 1000, 1000, 2);
        assertFalse(c.present(), "it is never asked");
        assertEquals(LiveTierClient.ABSENT, c.list().failure);
        String why = c.liveRootConflict(java.nio.file.Path.of("/any/file"));
        assertNotNull(why, "its root cannot be learned: nothing can be checked");
        assertTrue(why.startsWith("cannot be checked") && why.contains("does not admit"), why);

        FakeProvider admitting = new FakeProvider();
        admitting.root = "/nonexistent/live-a";
        registerProvider(admitting, SELF);                     // another provider, lower-ranked, that admits it
        assertTrue(c.present());
        assertTrue(c.list().ok());
        assertEquals(1, admitting.requests.size(), "the one that admits this bundle is asked");
        assertNotNull(c.liveRootConflict(java.nio.file.Path.of("/any/file")), "still closed while the other is registered");

        registry.unregister(refusing);
        assertNull(c.liveRootConflict(java.nio.file.Path.of("/any/file")), "every registered root known: open");
        assertEquals("lies in a live-tier root", c.liveRootConflict(java.nio.file.Path.of("/nonexistent/live-a/t/f")));
        assertTrue(provider.requests.isEmpty(), "the provider that does not admit this bundle was never called");
    }

    @Test
    void theProviderComesAndGoes() {
        LiveTierClient c = client(SELF, 1000, 1000, 2);
        assertEquals(LiveTierClient.ABSENT, c.list().failure, "filerepo starts before the provider");

        ServiceReference<?> first = registerProvider(provider, SELF);
        LiveTierClient.Answer a = c.list();
        assertTrue(a.ok(), a.desc());
        assertEquals(provider.root, a.reply.get("root"));

        registry.unregister(first);
        assertFalse(c.present());
        assertEquals(LiveTierClient.ABSENT, c.list().failure, "a provider that went is not asked");

        FakeProvider second = new FakeProvider();
        second.root = "/another/live-root";
        registerProvider(second, SELF);
        LiveTierClient.Answer b = c.list();
        assertTrue(b.ok());
        assertEquals("/another/live-root", b.reply.get("root"), "the provider that came back is the one asked");
        assertTrue(logged.stream().anyMatch(l -> l.contains("live-tier provider gone")), String.valueOf(logged));
    }

    @Test
    void aHungProviderCostsACallerItsTimeoutAndHoldsAtMostMaxCallsThreadsOfEachKind() throws Exception {
        registerProvider(provider, SELF);
        LiveTierClient c = client(SELF, 600, 900, 2);
        provider.hang = new CountDownLatch(1);

        for (int i = 0; i < 2; i++) {
            long t0 = System.nanoTime();
            LiveTierClient.Answer a = i == 0 ? c.list() : c.changes("lt_1");
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertEquals(LiveTierClient.TIMEOUT, a.failure);
            assertTrue(ms >= 590 && ms < 6000, "waited its timeout, no more: " + ms + " ms");
        }
        long t0 = System.nanoTime();
        LiveTierClient.Answer busy = c.changes("lt_1");
        long ms = (System.nanoTime() - t0) / 1_000_000;
        assertEquals(LiveTierClient.BUSY, busy.failure, "both local call threads are held: refused at once");
        assertTrue(ms < 500, "refused without waiting for the 600 ms timeout: " + ms + " ms");

        // the forwarded calls have threads of their own, and their own timeout
        t0 = System.nanoTime();
        assertEquals(LiveTierClient.TIMEOUT, c.relay(Map.of("k1", "v1")).failure);
        ms = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(ms >= 890 && ms < 6000, "the forwarded timeout for relay: " + ms + " ms");
        assertEquals(LiveTierClient.TIMEOUT, c.realise(Map.of("collection", "c", "vid", "v")).failure);
        assertEquals(LiveTierClient.BUSY, c.relay(Map.of("k1", "v1")).failure, "both forwarded call threads are held");
        assertEquals(4, provider.inFlight.get(), "every held call still hangs in the provider, ignoring interrupts");
        assertEquals(4, provider.requests.size(), "the busy calls never reached the provider");

        provider.hang.countDown();                            // the provider recovers
        long deadline = System.currentTimeMillis() + 5000;
        LiveTierClient.Answer back, relayed;
        do {
            back = c.list();
            relayed = c.relay(Map.of("k1", "v1"));
        } while (!(back.ok() && relayed.ok()) && System.currentTimeMillis() < deadline);
        assertTrue(back.ok() && relayed.ok(), "calls succeed again once the provider answers: " + back.failure + " " + relayed.failure);
    }

    @Test
    void forwardedCallsHeldUpElsewhereNeverStarveTheLocalAnswers() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        provider.relay = r -> {
            while (release.getCount() > 0) {                   // whatever the provider forwards to does not answer
                try { release.await(); } catch (InterruptedException ignored) { }
            }
            return FakeProvider.answer("11");
        };
        provider.workspace("lt_000000000000000a");
        registerProvider(provider, SELF);
        LiveTierClient c = client(SELF, 600, 300, 2);
        try {
            for (int i = 0; i < 2; i++) assertEquals(LiveTierClient.TIMEOUT, c.relay(Map.of("k1", "v1")).failure);
            assertEquals(LiveTierClient.BUSY, c.relay(Map.of("k1", "v1")).failure, "the forwarded threads are all held");
            for (int i = 0; i < 5; i++) {
                assertTrue(c.list().ok(), "list has threads of its own");
                assertTrue(c.changes("lt_000000000000000a").ok(), "changes has threads of its own");
            }
            assertEquals("lies in a live-tier root", c.liveRootConflict(java.nio.file.Path.of(provider.root, "f")),
                    "the root is still learned");
        } finally {
            release.countDown();
        }
    }

    @Test
    void aProviderThatThrowsIsAnErrorNotACrash() {
        registerProvider(provider, SELF);
        LiveTierClient c = client(SELF, 1000, 1000, 2);
        provider.fail = new IllegalStateException("boom");
        LiveTierClient.Answer a = c.list();
        assertEquals(LiveTierClient.ERROR, a.failure);
        assertTrue(a.desc().contains("IllegalStateException"), a.desc());
        provider.fail = null;
        assertTrue(c.list().ok());
    }

    @Test
    void theRequestsAreExactlyTheDocumentedMaps() {
        registerProvider(provider, SELF);
        LiveTierClient c = client(SELF, 1000, 1000, 2);

        assertTrue(c.list().ok());
        assertEquals(Map.of("op", "list"), provider.requests.get(0));

        provider.workspace("lt_000000000000000a");
        LiveTierClient.Answer ch = c.changes("lt_000000000000000a");
        assertTrue(ch.ok());
        assertEquals("clean", ch.reply.get("state"));
        assertEquals(Map.of("op", "changes", "row", "lt_000000000000000a"), provider.requests.get(1));

        Map<String, String> params = new LinkedHashMap<>();
        params.put("collection", "collection-a");
        params.put("vid", "v1");
        params.put("prefix", "data/");
        params.put("lease_ms", "60000");
        params.put("request_id", "r-1");
        LiveTierClient.Answer r = c.realise(params);
        assertTrue(r.ok());
        assertEquals("j_realise_1", r.reply.get("job_id"));
        Map<String, String> sentParams = new LinkedHashMap<>(params);
        sentParams.put("mode", "copy");
        assertEquals(Map.of("op", "realise", "params", sentParams), provider.requests.get(2));

        Map<String, String> signed = new LinkedHashMap<>();
        signed.put("k1", "v1");
        signed.put("k2", "v2");
        signed.put("k3", "lt_000000000000000a");
        signed.put("k4", "1700000000000");
        signed.put("k5", "c2lnbmF0dXJl");
        Map<String, Object> coreReply = FakeProvider.answer("10", "status_desc", "ok", "lease_until", "99");
        provider.relay = req -> coreReply;
        LiveTierClient.Answer rel = c.relay(signed);
        assertSame(coreReply, rel.reply, "the reply comes back as it came");
        assertEquals(Map.of("op", "relay", "request", signed), provider.requests.get(3), "the request goes unchanged");

        LiveTierClient.Listing one = c.listRow("lt_000000000000000a");
        assertTrue(one.answer.ok());
        assertEquals("workspace", one.row.get("mode"));
        assertEquals(Map.of("op", "list", "row", "lt_000000000000000a"), provider.requests.get(4), "a row's listing names it");
    }

    @Test
    void aRowsListingIsReusedForItsPeriodOnlyAndOnlyWhenItSucceeded() throws Exception {
        registerProvider(provider, SELF);
        provider.copy("lt_0000000000000001");
        client = new LiveTierClient(registry.contextOf(SELF), logger(), 1000, 1000, 2, 1000);
        LiveTierClient c = client;
        assertEquals("copy", c.listRow("lt_0000000000000001").row.get("mode"));
        assertEquals("copy", c.listRow("lt_0000000000000001").row.get("mode"));
        assertEquals(1, provider.requests.size(), "reused within its period");
        assertNull(c.listRow("lt_0000000000000002").row, "another row is listed afresh");
        assertEquals(2, provider.requests.size());
        Thread.sleep(1100);
        assertTrue(c.listRow("lt_0000000000000002").answer.ok());
        assertEquals(3, provider.requests.size(), "and listed again once its period is over");
        Thread.sleep(1100);
        provider.fail = new IllegalStateException("boom");
        assertEquals(LiveTierClient.ERROR, c.listRow("lt_0000000000000002").answer.failure);
        provider.fail = null;
        assertTrue(c.listRow("lt_0000000000000002").answer.ok(), "a failure is never reused");
        assertEquals(5, provider.requests.size());
        LiveTierClient none = new LiveTierClient(registry.contextOf(SELF), logger(), 1000, 1000, 2);
        try {
            none.listRow("lt_0000000000000001");
            none.listRow("lt_0000000000000001");
            assertEquals(7, provider.requests.size(), "a period of 0 never reuses");
        } finally {
            none.close();
        }
    }

    @Test
    void aRealiseIsCopiesAtThisNodeOnlyAndAnythingElseIsRefusedBeforeItIsSent() {
        registerProvider(provider, SELF);
        LiveTierClient c = client(SELF, 1000, 1000, 2);
        assertEquals(LiveTierClient.REFUSED, c.realise(Map.of("collection", "c", "vid", "v", "mode", "workspace")).failure);
        assertEquals(LiveTierClient.REFUSED, c.realise(Map.of("collection", "c", "vid", "v", "node", "another-node")).failure);
        assertEquals(LiveTierClient.REFUSED, c.realise(Map.of("collection", "c", "vid", "v", "nodes", "a,b")).failure);
        assertEquals(LiveTierClient.REFUSED, c.relay(Map.of()).failure);
        assertTrue(provider.requests.isEmpty(), "nothing reached the provider: " + provider.requests);
        assertTrue(c.realise(Map.of("collection", "c", "vid", "v", "mode", "copy")).ok());
    }

    @Test
    void canonicalResolvesAPathAsTheFilesystemDoes(@org.junit.jupiter.api.io.TempDir java.nio.file.Path tmp) throws Exception {
        java.nio.file.Path base = tmp.toRealPath();
        java.nio.file.Path target = java.nio.file.Files.createDirectories(base.resolve("t/inner"));
        java.nio.file.Path dir = java.nio.file.Files.createDirectories(base.resolve("d"));
        java.nio.file.Files.createSymbolicLink(dir.resolve("link"), target);
        // an existing path: its real path
        assertEquals(target, LiveTierClient.canonical(dir.resolve("link")));
        // a name below a link that does not exist yet: under the link's target
        assertEquals(target.resolve("new/file"), LiveTierClient.canonical(dir.resolve("link/new/file")));
        // .. after a link is the target's parent, not the link's
        assertEquals(base.resolve("t/x"), LiveTierClient.canonical(java.nio.file.Path.of(dir + "/link/../x")));
        // a missing directory, then .., then the link
        assertEquals(target.resolve("y"), LiveTierClient.canonical(java.nio.file.Path.of(dir + "/missing/../link/y")));
        // nothing exists below the root of the filesystem but the root itself: still an absolute path
        assertTrue(LiveTierClient.canonical(java.nio.file.Path.of("relative/name")).isAbsolute());
    }

    @Test
    void rowFindsOneRowOfAListAnswer() {
        registerProvider(provider, SELF);
        LiveTierClient c = client(SELF, 1000, 1000, 2);
        provider.copy("lt_0000000000000001");
        provider.workspace("lt_0000000000000002");
        LiveTierClient.Answer list = c.list();
        assertEquals("workspace", LiveTierClient.row(list, "lt_0000000000000002").get("mode"));
        assertNull(LiveTierClient.row(list, "lt_ffffffffffffffff"));
        assertNull(LiveTierClient.row(c.changes("lt_nope"), "lt_0000000000000001"), "not a list answer");
    }
}
