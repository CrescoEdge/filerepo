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
                c.relay(Map.of("action", "core.list", "sig", "s")))) {
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
    void aHungProviderCostsACallerItsTimeoutAndHoldsAtMostMaxCallsThreads() throws Exception {
        registerProvider(provider, SELF);
        LiveTierClient c = client(SELF, 200, 300, 2);
        provider.hang = new CountDownLatch(1);

        long t0 = System.nanoTime();
        LiveTierClient.Answer a = c.list();
        long ms = (System.nanoTime() - t0) / 1_000_000;
        assertEquals(LiveTierClient.TIMEOUT, a.failure);
        assertTrue(ms >= 190 && ms < 3000, "waited its timeout, no more: " + ms + " ms");

        t0 = System.nanoTime();
        assertEquals(LiveTierClient.TIMEOUT, c.relay(Map.of("action", "core.list", "sig", "s")).failure);
        ms = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(ms >= 290 && ms < 3000, "the core-bound timeout for relay: " + ms + " ms");
        assertEquals(2, provider.inFlight.get(), "both calls still hang in the provider, ignoring interrupts");

        t0 = System.nanoTime();
        LiveTierClient.Answer busy = c.changes("lt_1");
        ms = (System.nanoTime() - t0) / 1_000_000;
        assertEquals(LiveTierClient.BUSY, busy.failure, "every call thread is held: refused at once");
        assertTrue(ms < 150, "refused without waiting: " + ms + " ms");
        assertEquals(2, provider.requests.size(), "the busy call never reached the provider");

        provider.hang.countDown();                            // the provider recovers
        long deadline = System.currentTimeMillis() + 5000;
        LiveTierClient.Answer back;
        do {
            back = c.list();
        } while (!back.ok() && System.currentTimeMillis() < deadline);
        assertTrue(back.ok(), "calls succeed again once the provider answers: " + back.failure);
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
        signed.put("action", "core.livehold");
        signed.put("principal", "user-a");
        signed.put("sig_ts", "1700000000000");
        signed.put("sig_nonce", "n-1");
        signed.put("sig_params", "op,row");
        signed.put("op", "extend");
        signed.put("row", "lt_000000000000000a");
        signed.put("sig", "c2lnbmF0dXJl");
        Map<String, Object> coreReply = FakeProvider.answer("10", "status_desc", "ok", "lease_until", "99");
        provider.relay = req -> coreReply;
        LiveTierClient.Answer rel = c.relay(signed);
        assertSame(coreReply, rel.reply, "the reply comes back as it came");
        assertEquals(Map.of("op", "relay", "request", signed), provider.requests.get(3), "the request goes unchanged");
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
