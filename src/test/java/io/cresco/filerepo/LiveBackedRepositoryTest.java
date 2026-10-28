package io.cresco.filerepo;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.utilities.CLogger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A repository backed by a live-tier row (config live_row): its state comes from the provider's cached answers, it
 * refuses every verb that would serve, write, delete, sync or list it, or say where it is (with or without a provider),
 * it runs no scanner,
 * nothing a caller sends can change its backing, and it keeps its copy live through the node's own realise. Nothing a
 * reply carries names what the row holds.
 */
class LiveBackedRepositoryTest {

    static final String COPY = "lt_00000000000000c1", WORKSPACE = "lt_00000000000000a1";

    @TempDir Path tmp;
    final FakeRegistry registry = new FakeRegistry();
    final FakeProvider provider = new FakeProvider();
    final Gson gson = new Gson();
    LiveTierClient client;
    TestRig rig;

    @AfterEach
    void down() {
        CountDownLatch h = provider.hang;
        if (h != null) h.countDown();
        if (rig != null) rig.close();
        if (client != null) client.close();
    }

    static CLogger quiet() {
        return (CLogger) Proxy.newProxyInstance(CLogger.class.getClassLoader(), new Class<?>[]{CLogger.class}, (p, m, a) -> null);
    }

    void registerProvider() {
        registry.register(FakeProvider.props(), b -> "io.cresco.filerepo".equals(b.getSymbolicName()) ? provider : null);
    }

    /** A live-backed repository also configured as a scanning producer: that configuration must count for nothing. */
    TestRig liveRig(String row) throws Exception {
        client = new LiveTierClient(registry.contextOf("io.cresco.filerepo"), quiet(), 500, 800, 2);
        Path scan = Files.createDirectories(tmp.resolve("scan"));
        Files.write(scan.resolve("in-scan-dir.txt"), "bytes".getBytes());
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("live_row", row);
        cfg.put("scan_dir", scan.toString());
        cfg.put("repo_dir", Files.createDirectories(tmp.resolve("repo")).toString());
        cfg.put("filerepo_name", "fr-live");
        cfg.put("enable_scan", "true");
        cfg.put("scan_delay", "10");
        cfg.put("scan_period", "10");
        rig = new TestRig(tmp.resolve("agent"), cfg, client);
        return rig;
    }

    static void assertNamesNothing(MsgEvent r) {
        for (String k : new String[]{"tenant", "collection", "vid", "prefix", "path", "root"}) {
            assertNull(r.getParam(k), "a reply never carries " + k);
        }
        for (String v : r.getParams().values()) {
            assertFalse(v.contains("collection-a") || v.contains("tenant-a") || v.contains("data/") || v.contains("live-root"),
                    "a reply never names what the row holds: " + r.getParams());
        }
    }

    // ------------------------------------------------------------------ repostate

    @Test
    void aLiveCopyIsLIVE() throws Exception {
        provider.copy(COPY);
        registerProvider();
        MsgEvent r = liveRig(COPY).call("repostate");
        assertEquals("10", r.getParam("status"), r.getParams().toString());
        assertEquals("LIVE", r.getParam("state"));
        assertEquals("live", r.getParam("backing"));
        assertEquals(COPY, r.getParam("live_row"));
        assertEquals("copy", r.getParam("mode"));
        assertEquals("LIVE", r.getParam("row_state"));
        assertEquals("3", r.getParam("files"));
        assertEquals("300", r.getParam("bytes"));
        assertEquals("1234567", r.getParam("lease_until"));
        assertNamesNothing(r);
        assertEquals(List.of(Map.of("op", "list", "row", COPY)), provider.requests, "a copy needs its row's listing only");
    }

    @Test
    void aProviderThatListsTheNamedRowAloneIsReadTheSameWay() throws Exception {
        provider.listsOneRow = true;
        provider.copy("lt_00000000000000c0");
        provider.copy(COPY);
        registerProvider();
        MsgEvent r = liveRig(COPY).call("repostate");
        assertEquals("LIVE", r.getParam("state"), r.getParams().toString());
        assertEquals("3", r.getParam("files"));
        rig.close();
        client.close();
        assertEquals("9", liveRig("lt_000000000000dead").call("repostate").getParam("status"), "an empty listing: not held");
    }

    @Test
    void aStreamOfRepostateCallsCostsTheProviderOneListPerPeriod() throws Exception {
        provider.copy(COPY);
        org.osgi.framework.ServiceReference<?> first =
                registry.register(FakeProvider.props(), b -> "io.cresco.filerepo".equals(b.getSymbolicName()) ? provider : null);
        liveRig(COPY);
        client.close();
        rig.close();
        client = new LiveTierClient(registry.contextOf("io.cresco.filerepo"), quiet(), 500, 800, 2, 60000L);
        rig = new TestRig(tmp.resolve("agent"), rig.config, client);
        for (int i = 0; i < 50; i++) {
            assertEquals("LIVE", rig.call("repostate").getParam("state"));
        }
        assertEquals(1, provider.requests.size(), "one list in the period, whatever the number of calls: " + provider.requests.size());
        FakeProvider restarted = new FakeProvider();
        restarted.copy(COPY);
        restarted.rows.get(COPY).put("files", 4);
        registry.register(FakeProvider.props(), b -> restarted);
        // the first provider stays the higher-ranked one; once it goes, the one left is asked afresh
        registry.unregister(first);
        assertEquals("4", rig.call("repostate").getParam("files"), "a listing is never reused across providers");
    }

    @Test
    void aWorkspaceIsCleanOrDirtyFromItsCachedSummaryAlone() throws Exception {
        provider.workspace(WORKSPACE);
        registerProvider();
        liveRig(WORKSPACE);
        assertFalse(Files.exists(Paths.get(provider.root)), "the workspace is nowhere on this disk: no walk could tell");

        MsgEvent clean = rig.call("repostate");
        assertEquals("WORKSPACE_CLEAN", clean.getParam("state"), clean.getParams().toString());
        assertEquals("workspace", clean.getParam("mode"));
        assertEquals("0", clean.getParam("changed"));
        assertEquals("42", clean.getParam("as_of"));
        assertNamesNothing(clean);

        provider.summary(WORKSPACE, "dirty", 2, false);
        MsgEvent dirty = rig.call("repostate");
        assertEquals("WORKSPACE_DIRTY", dirty.getParam("state"));
        assertEquals("2", dirty.getParam("changed"));

        provider.summary(WORKSPACE, "clean", 0, true);
        MsgEvent scanning = rig.call("repostate");
        assertEquals("WORKSPACE_DIRTY", scanning.getParam("state"), "a first walk still running is never clean");
        assertEquals("true", scanning.getParam("scanning"));

        provider.changes.remove(WORKSPACE);
        MsgEvent unknown = rig.call("repostate");
        assertEquals("10", unknown.getParam("status"));
        assertEquals("WORKSPACE_DIRTY", unknown.getParam("state"), "changes that cannot be read are counted dirty");
        assertEquals("unknown", unknown.getParam("changes"));

        long changesAsked = provider.requests.stream().filter(q -> "changes".equals(q.get("op"))).count();
        assertEquals(4, changesAsked, "one cached summary per repostate");
    }

    @Test
    void aRowNotHeldAtThisNodeAnswers9() throws Exception {
        provider.copy(COPY);
        registerProvider();
        MsgEvent r = liveRig("lt_000000000000dead").call("repostate");
        assertEquals("9", r.getParam("status"));
        assertNull(r.getParam("state"));
        assertEquals("lt_000000000000dead", r.getParam("live_row"));
    }

    @Test
    void withoutAProviderTheStateIsUnknownAndEveryRefusalStillHolds() throws Exception {
        provider.copy(COPY);
        MsgEvent r = liveRig(COPY).call("repostate");
        assertEquals("11", r.getParam("status"), r.getParams().toString());
        assertNull(r.getParam("state"));
        assertEquals("absent", r.getParam("provider"));
        assertEveryUnsafeVerbIsRefused();
    }

    @Test
    void theRepositoryFollowsItsProviderGoingAndComingBack() throws Exception {
        provider.copy(COPY);
        liveRig(COPY);
        assertEquals("11", rig.call("repostate").getParam("status"), "filerepo started before the provider");
        org.osgi.framework.ServiceReference<?> first =
                registry.register(FakeProvider.props(), b -> "io.cresco.filerepo".equals(b.getSymbolicName()) ? provider : null);
        assertEquals("LIVE", rig.call("repostate").getParam("state"));
        registry.unregister(first);
        assertEquals("11", rig.call("repostate").getParam("status"));
        assertEquals("live-backed", rig.call("getfile", "file_path", "/x").getParam("refusal"), "and still refuses");
        FakeProvider restarted = new FakeProvider();
        restarted.copy(COPY);
        registry.register(FakeProvider.props(), b -> "io.cresco.filerepo".equals(b.getSymbolicName()) ? restarted : null);
        assertEquals("LIVE", rig.call("repostate").getParam("state"), "the provider that came back is asked");
        assertFalse(restarted.requests.isEmpty());
    }

    @Test
    void aHungProviderAnswers11WithinItsTimeout() throws Exception {
        provider.copy(COPY);
        registerProvider();
        liveRig(COPY);
        provider.hang = new CountDownLatch(1);
        long t0 = System.nanoTime();
        MsgEvent r = rig.call("repostate");
        long ms = (System.nanoTime() - t0) / 1_000_000;
        assertEquals("11", r.getParam("status"));
        assertEquals("timeout", r.getParam("provider"));
        assertTrue(ms < 3000, "bounded: " + ms + " ms");
        assertEveryUnsafeVerbIsRefused();
    }

    @Test
    void relaysHeldUpElsewhereNeverStarveRepostate() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        provider.relay = r -> {
            while (release.getCount() > 0) {                   // whatever the provider forwards to does not answer
                try { release.await(); } catch (InterruptedException ignored) { }
            }
            return FakeProvider.answer("11");
        };
        provider.copy(COPY);
        registerProvider();
        liveRig(COPY);
        try {
            assertEquals("LIVE", rig.call("repostate").getParam("state"), "healthy at first");
            for (int i = 0; i < 3; i++) {                     // more relays than there are threads for them
                MsgEvent q = TestRig.request("liverelay");
                q.setCompressedParam("request", gson.toJson(Map.of("k1", "v1")));
                assertEquals("11", rig.call(q).getParam("status"), "the relay was not answered");
            }
            MsgEvent s = rig.call("repostate");
            assertEquals("LIVE", s.getParam("state"), "the provider's own list still answers: " + s.getParams());
        } finally {
            release.countDown();
        }
    }

    // ------------------------------------------------------------------ refusals

    @Test
    void everyVerbThatWouldServeWriteDeleteSyncOrListIsRefused() throws Exception {
        provider.copy(COPY);
        registerProvider();
        liveRig(COPY);
        assertEveryUnsafeVerbIsRefused();
        assertTrue(provider.requests.isEmpty(), "a refusal needs no provider: " + provider.requests);
    }

    /** Every verb outside the live-backed set, with real inputs, is refused and leaves no trace. */
    void assertEveryUnsafeVerbIsRefused() throws Exception {
        Path scanFile = tmp.resolve("scan").resolve("in-scan-dir.txt");
        Path incoming = Files.write(tmp.resolve("incoming.bin"), "landing".getBytes());
        List<MsgEvent> requests = new ArrayList<>();
        requests.add(TestRig.request("getfile", "file_path", scanFile.toString()));
        requests.add(TestRig.request("streamfile", "file_path", scanFile.toString(), "start_byte", "0", "byte_length", "1",
                "transfer_id", "t-1", "ident_key", "stream_name", "ident_id", "s-1"));
        requests.add(TestRig.request("getjar", "action_pluginname", "p", "action_pluginmd5", "m"));
        MsgEvent putjar = TestRig.request("putjar", "pluginname", "p", "md5", "m", "jarfile", "p.jar", "version", "1");
        putjar.setDataParam("jardata", "jar-bytes".getBytes());
        requests.add(putjar);
        MsgEvent putfiles = TestRig.request("putfiles", "repo_name", "fr-live", "overwrite", "true");
        putfiles.addFile(incoming.toString());
        requests.add(putfiles);
        requests.add(TestRig.request("removefile", "repo_name", "fr-live", "file_name", "in-scan-dir.txt"));
        MsgEvent remote = TestRig.request("putfilesremote", "dst_region", "r2", "dst_agent", "a2", "dst_plugin", "p2", "repo_name", "fr");
        remote.setCompressedParam("file_list", gson.toJson(List.of(scanFile.toString())));
        requests.add(remote);
        requests.add(TestRig.request("clearrepo", "repo_name", "fr-live"));
        requests.add(TestRig.request("clearrepo", "repo_name", "fr-live", "force", "true"));
        MsgEvent listin = TestRig.request("repolistin", "transfer_id", "1");
        listin.setCompressedParam("repolistin", "{}");
        requests.add(listin);
        requests.add(TestRig.request("repoconfirm", "transfer_id", "1"));
        requests.add(TestRig.request("repolist"));
        requests.add(TestRig.request("getrepofilelist", "repo_name", "fr-live"));
        requests.add(TestRig.request("streamfilecancel", "transfer_id", "t-1"));
        requests.add(TestRig.request("getscandir"));
        requests.add(TestRig.request("averbaddedlater"));

        for (MsgEvent q : requests) {
            String action = q.getParam("action");
            MsgEvent r = rig.call(q);
            assertNotNull(r, action + " answers its refusal");
            assertEquals("9", r.getParam("status"), action + ": " + r.getParams());
            assertEquals("9", r.getParam("status_code"), action);
            assertEquals("live-backed", r.getParam("refusal"), action);
            assertNull(r.getParam("jardata"), action + " drops its payload");
            assertNull(r.getParam("repolistin"), action + " drops its payload");
            assertNull(r.getParam("repofilelist"), action);
            assertNull(r.getParam("file_data"), action);
            assertNull(r.getParam("repolist"), action);
            assertNull(r.getParam("scan_dir"), action + " names no path");
            assertFalse(r.getParam("status_desc").contains(tmp.toString()), action + " names no path: " + r.getParam("status_desc"));
        }
        assertTrue(rig.sent.isEmpty(), "nothing was pushed or confirmed: " + rig.sent);
        assertTrue(Files.exists(scanFile), "nothing was deleted");
        assertTrue(Files.exists(incoming), "nothing was landed (moved)");
        try (Stream<Path> s = Files.list(tmp.resolve("repo"))) {
            assertEquals(0, s.count(), "nothing was written into the repo dir");
        }
        assertEquals(0, rig.repo.getRepoCount(), "nothing was cataloged");
    }

    @Test
    void theVerbsItStillAnswers() throws Exception {
        provider.copy(COPY);
        registerProvider();
        liveRig(COPY);
        assertEquals("10", rig.call("repostate").getParam("status"));
        assertNotNull(rig.call("getcapabilities").getParam("capabilities"));
        assertEquals("10", rig.call("getmetrics").getParam("status"));
        for (String verb : ExecutorImpl.LIVE_BACKED_VERBS) {
            MsgEvent r = rig.call(verb);
            assertNotEquals("live-backed", r == null ? null : r.getParam("refusal"), verb);
        }
    }

    @Test
    void itRunsNoScannerAndNoSync() throws Exception {
        provider.copy(COPY);
        registerProvider();
        liveRig(COPY);
        rig.repo.start();
        rig.repo.startScan(10, 10);
        Thread.sleep(300);                                   // a scan due at 10 ms would have run by now
        assertFalse(rig.repo.isScanScheduled());
        assertTrue(rig.listeners.isEmpty(), "no discover, subscribe or sync listener: " + rig.listeners);
        assertTrue(rig.dataplaneSent.isEmpty(), "no broadcast");
        assertEquals(0, rig.repo.getRepoCount(), "the scan dir was never cataloged");
    }

    @Test
    void nothingACallerSendsChangesTheBacking() throws Exception {
        provider.copy(COPY);
        registerProvider();
        liveRig(COPY);
        for (String verb : ExecutorImpl.LIVE_BACKED_VERBS) {
            rig.call(verb, "live_row", "", "backing", "local");
            rig.call(verb, "live_row", "lt_00000000000000ff");
        }
        MsgEvent cfg = TestRig.request("config", "live_row", "");
        rig.exec.executeCONFIG(cfg);
        rig.plugin.setConfig(new HashMap<>(Map.of("pluginID", TestRig.PLUGIN_ID)));   // even a replaced config
        assertTrue(rig.repo.isLiveBacked());
        assertEquals(COPY, rig.call("repostate").getParam("live_row"));
        assertEquals("live-backed", rig.call("getfile", "file_path", "/x").getParam("refusal"));
        assertTrue(Modifier.isFinal(RepoEngine.class.getDeclaredField("liveRow").getModifiers()), "read once, at construction");
    }

    // ------------------------------------------------------------------ liverelay and liverealise

    @Test
    void liverelayCarriesTheSignedRequestUnchangedAndTheReplyAsItCame() throws Exception {
        provider.copy(COPY);
        registerProvider();
        liveRig(COPY);
        Map<String, String> signed = new LinkedHashMap<>();
        signed.put("k1", "v1");
        signed.put("k2", "v2");
        signed.put("k3", COPY);
        signed.put("k4", "1700000000000");
        signed.put("k5", "c2lnbmF0dXJl");
        Map<String, Object> coreReply = FakeProvider.answer("10", "status_desc", "held", "lease_until", "99");
        provider.relay = req -> coreReply;

        MsgEvent q = TestRig.request("liverelay");
        q.setCompressedParam("request", gson.toJson(signed));
        MsgEvent r = rig.call(q);
        assertEquals("10", r.getParam("status"), r.getParams().toString());
        assertEquals("10", r.getParam("reply_status"));
        assertNull(r.getParam("request"));
        Map<String, String> reply = gson.fromJson(r.getCompressedParam("reply"), new TypeToken<Map<String, String>>(){}.getType());
        assertEquals(Map.of("status", "10", "status_desc", "held", "lease_until", "99"), reply);
        assertEquals(Map.of("op", "relay", "request", signed), provider.requests.get(provider.requests.size() - 1),
                "relayed unchanged: filerepo adds and signs nothing");

        provider.relay = req -> FakeProvider.answer("3", "status_desc", "refused");
        MsgEvent refused = TestRig.request("liverelay");
        refused.setCompressedParam("request", gson.toJson(Map.of("k1", "v1")));
        MsgEvent rr = rig.call(refused);
        assertEquals("10", rr.getParam("status"), "the provider answered");
        assertEquals("3", rr.getParam("reply_status"), "and its answer was a refusal");

        assertEquals("8", rig.call("liverelay").getParam("status"), "no request");
        MsgEvent junk = TestRig.request("liverelay");
        junk.setCompressedParam("request", "not json");
        assertEquals("8", rig.call(junk).getParam("status"));
    }

    @Test
    void liverelayWithoutAProviderAnswers11() throws Exception {
        provider.copy(COPY);
        liveRig(COPY);
        MsgEvent q = TestRig.request("liverelay");
        q.setCompressedParam("request", gson.toJson(Map.of("k1", "v1", "k2", "v2")));
        MsgEvent r = rig.call(q);
        assertEquals("11", r.getParam("status"));
        assertNull(r.getParam("reply"));
    }

    @Test
    void liverealiseKeepsTheBoundCopyLiveAsTheNodeNamingNothingOnTheFabric() throws Exception {
        provider.copy(COPY);
        registerProvider();
        liveRig(COPY);
        MsgEvent r = rig.call("liverealise", "lease_ms", "60000", "request_id", "keep-1");
        assertEquals("10", r.getParam("status"), r.getParams().toString());
        assertEquals("j_realise_1", r.getParam("job_id"));
        assertEquals("10", r.getParam("reply_status"));
        assertEquals("keep-1", r.getParam("request_id"));
        assertNamesNothing(r);
        Map<String, String> params = new LinkedHashMap<>();
        params.put("collection", "collection-a");
        params.put("vid", "v1");
        params.put("prefix", "data/");
        params.put("lease_ms", "60000");
        params.put("request_id", "keep-1");
        params.put("mode", "copy");
        assertEquals(Map.of("op", "realise", "params", params), provider.requests.get(provider.requests.size() - 1),
                "the row's own selection, a copy, no node named (the provider realises at itself)");

        MsgEvent generated = rig.call("liverealise");
        assertEquals("10", generated.getParam("status"));
        assertTrue(generated.getParam("request_id").matches("filerepo-[0-9a-f]{32}"), generated.getParam("request_id"));

        provider.realise = p -> FakeProvider.answer("8", "status_desc", "path data/secret-name refused");
        MsgEvent refused = rig.call("liverealise");
        assertEquals("9", refused.getParam("status"));
        assertEquals("8", refused.getParam("reply_status"));
        assertEquals("realise", refused.getParam("refusal"));
        assertFalse(refused.getParams().values().stream().anyMatch(v -> v.contains("secret-name")),
                "the sealed reply's description never crosses the fabric: " + refused.getParams());

        assertEquals("8", rig.call("liverealise", "lease_ms", "soon").getParam("status"));
        assertEquals("8", rig.call("liverealise", "request_id", "../x").getParam("status"));
    }

    @Test
    void liverealiseRefusesWhatTheNodeMayNotRealise() throws Exception {
        provider.workspace(WORKSPACE);
        registerProvider();
        liveRig(WORKSPACE);
        MsgEvent ws = rig.call("liverealise");
        assertEquals("9", ws.getParam("status"));
        assertEquals("workspace", ws.getParam("refusal"), "the node realises copies only");
        rig.close();
        client.close();

        liveRig("lt_000000000000dead");
        assertEquals("not-held", rig.call("liverealise").getParam("refusal"));
        long realises = provider.requests.stream().filter(q -> "realise".equals(q.get("op"))).count();
        assertEquals(0, realises, "nothing was realised");
    }
}
