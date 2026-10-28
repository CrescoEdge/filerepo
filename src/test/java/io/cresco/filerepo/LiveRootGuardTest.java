package io.cresco.filerepo;

import com.google.gson.Gson;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.utilities.CLogger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.osgi.framework.ServiceReference;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Files in a live-tier root are the provider's: read only through the filesystem at the node, never served, landed,
 * deleted or synced through filerepo, whatever a repository's configuration. The root is learned from the provider and
 * remembered in the catalog, links into it are resolved, and a provider whose root cannot be learned fails closed.
 */
class LiveRootGuardTest {

    @TempDir Path tmp;
    final FakeRegistry registry = new FakeRegistry();
    final FakeProvider provider = new FakeProvider();
    final Gson gson = new Gson();
    LiveTierClient client;
    TestRig rig;
    Path root, secret;

    @BeforeEach
    void up() throws Exception {
        tmp = tmp.toRealPath();
        root = Files.createDirectories(tmp.resolve("live"));
        secret = Files.write(Files.createDirectories(root.resolve("tenant-a/collection-a/v1")).resolve("secret.txt"), "plaintext".getBytes());
        provider.root = root.toString();
        provider.copy("lt_00000000000000c1");
    }

    @AfterEach
    void down() {
        if (rig != null) rig.close();
        if (client != null) client.close();
    }

    ServiceReference<?> registerProvider(FakeProvider p) {
        return registry.register(FakeProvider.props(), b -> "io.cresco.filerepo".equals(b.getSymbolicName()) ? p : null);
    }

    TestRig rig(Map<String, Object> cfg) throws Exception {
        client = new LiveTierClient(registry.contextOf("io.cresco.filerepo"),
                (CLogger) Proxy.newProxyInstance(CLogger.class.getClassLoader(), new Class<?>[]{CLogger.class}, (p, m, a) -> null),
                500, 500, 2);
        return rig = new TestRig(tmp.resolve("agent"), cfg, client);
    }

    TestRig onDemand() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("repo_dir", Files.createDirectories(tmp.resolve("repo")).toString());
        return rig(cfg);
    }

    MsgEvent pushRemote(Path... files) {
        MsgEvent m = TestRig.request("putfilesremote", "dst_region", "r2", "dst_agent", "a2", "dst_plugin", "p2", "repo_name", "fr");
        List<String> paths = new java.util.ArrayList<>();
        for (Path f : files) paths.add(f.toString());
        m.setCompressedParam("file_list", gson.toJson(paths));
        return m;
    }

    @Test
    void putfilesremoteNeverPushesAFileFromALiveRoot() throws Exception {
        registerProvider(provider);
        onDemand();
        Path ordinary = Files.write(tmp.resolve("ordinary.txt"), "shareable".getBytes());

        MsgEvent refused = rig.call(pushRemote(ordinary, secret));
        assertEquals("9", refused.getParam("status"));
        assertEquals("live-root", refused.getParam("refusal"));
        assertFalse(refused.getParam("status_desc").contains(secret.toString()), "the reply does not echo the path");
        assertTrue(rig.sent.isEmpty(), "nothing was pushed");

        Path link = Files.createSymbolicLink(tmp.resolve("innocent.txt"), secret);
        assertEquals("live-root", rig.call(pushRemote(link)).getParam("refusal"), "a link into the root is resolved");

        // dir/link/../secret.txt is, to the filesystem, the secret beside the link's target, not dir/secret.txt
        Path inner = Files.createDirectories(secret.getParent().resolve("inner"));
        Path dir = Files.createDirectories(tmp.resolve("dir"));
        Files.createSymbolicLink(dir.resolve("link"), inner);
        Path dotted = Path.of(dir + "/link/../secret.txt");
        assertTrue(Files.exists(dotted), "the filesystem reaches the secret through it");
        assertEquals("live-root", rig.call(pushRemote(dotted)).getParam("refusal"), ".. is resolved after the link, not before");

        MsgEvent ok = rig.call(pushRemote(ordinary));
        assertEquals("10", ok.getParam("status"), "an ordinary file is pushed as before");
        assertEquals(1, rig.sent.size());
        assertEquals("putfiles", rig.sent.get(0).getParam("action"));
    }

    @Test
    void aLandingFromALiveRootIsRefused() throws Exception {
        registerProvider(provider);
        onDemand();
        MsgEvent put = TestRig.request("putfiles", "repo_name", "fr");
        put.addFile(secret.toString());
        MsgEvent r = rig.call(put);
        assertEquals("9", r.getParam("status"));
        assertEquals("live-root", r.getParam("refusal"));
        assertFalse(Files.exists(tmp.resolve("repo").resolve("secret.txt")));
        assertEquals(0, rig.repo.getRepoCount());
        assertTrue(rig.sent.isEmpty(), "no confirm");
    }

    @Test
    void aCatalogedFileInALiveRootIsNeverServed() throws Exception {
        registerProvider(provider);
        onDemand();
        Path link = Files.createSymbolicLink(tmp.resolve("repo").resolve("link.txt"), secret);
        // as if cataloged before the root was known
        rig.db.addFile(secret.toString(), "md5", 1L, 9L);
        rig.db.addFile(link.toString(), "md5", 1L, 9L);
        for (Path p : new Path[]{secret, link}) {
            MsgEvent got = rig.call("getfile", "file_path", p.toString());
            assertEquals("live-root", got.getParam("refusal"), p.toString());
            assertNull(got.getDataParam("file_data"));
            MsgEvent streamed = rig.call("streamfile", "file_path", p.toString(), "start_byte", "0", "byte_length", "1",
                    "transfer_id", "t", "ident_key", "k", "ident_id", "i");
            assertEquals("live-root", streamed.getParam("refusal"), p.toString());
        }
        // and a clear forgets them without deleting them: only the provider's own drop removes a live file
        assertEquals("10", rig.call("clearrepo", "repo_name", "fr", "force", "true").getParam("status"));
        assertEquals(0, rig.repo.getRepoCount());
        assertTrue(Files.exists(secret));
    }

    @Test
    void aRepositoryOverALiveRootIsRefusedLikeALiveBackedOne() throws Exception {
        registerProvider(provider);
        Map<String, Object> inside = new HashMap<>();
        inside.put("repo_dir", root.resolve("tenant-a").toString());
        rig(inside);
        for (String verb : new String[]{"getrepofilelist", "repolist", "clearrepo", "removefile", "putjar", "repolistin", "getjar", "getscandir"}) {
            MsgEvent r = rig.call(verb, "repo_name", "fr", "force", "true", "file_name", "collection-a/v1/secret.txt");
            assertEquals("live-root", r.getParam("refusal"), verb + ": " + r.getParams());
        }
        assertEquals("10", rig.call("repostate").getParam("status"), "repostate still answers");
        assertTrue(Files.exists(secret));
        rig.close();
        client.close();

        Map<String, Object> containing = new HashMap<>();
        containing.put("repo_dir", tmp.toString());                         // the live root lies inside it
        rig(containing);
        MsgEvent clear = rig.call("clearrepo", "repo_name", "fr", "force", "true");
        assertEquals("live-root", clear.getParam("refusal"));
        assertTrue(Files.exists(secret), "a forced clear of a directory holding a live root deletes nothing");
    }

    @Test
    void theScannerSkipsLinksIntoALiveRoot() throws Exception {
        registerProvider(provider);
        Path scan = Files.createDirectories(tmp.resolve("scan"));
        Path plain = Files.write(scan.resolve("plain.txt"), "scanned".getBytes());
        Path link = Files.createSymbolicLink(scan.resolve("link.txt"), secret);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("scan_dir", scan.toString());
        cfg.put("filerepo_name", "fr");
        cfg.put("enable_scan", "true");
        cfg.put("scan_delay", "20");
        cfg.put("scan_period", "3600000");
        rig(cfg);
        rig.repo.start();
        long deadline = System.currentTimeMillis() + 10_000;
        while (rig.repo.getRepoCount() < 1 && System.currentTimeMillis() < deadline) Thread.sleep(25);
        Thread.sleep(100);
        assertNotNull(rig.repo.getFileInfo(plain.toString()), "an ordinary file is cataloged as before");
        assertNull(rig.repo.getFileInfo(link.toString()), "a link into the live root is not");
        assertEquals(1, rig.repo.getRepoCount());
    }

    @Test
    void aRowCatalogedBeforeTheRootWasKnownIsNeverReoffered() throws Exception {
        registerProvider(provider);
        Path scan = Files.createDirectories(tmp.resolve("scan"));
        Path plain = Files.write(scan.resolve("plain.txt"), "scanned".getBytes());
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("scan_dir", scan.toString());
        cfg.put("filerepo_name", "fr");
        rig(cfg);
        // unconfirmed rows (insync=0): one ordinary, one in the live root, cataloged before the root was known
        rig.db.addFile(plain.toString(), rig.plugin.getMD5(plain.toString()), Files.getLastModifiedTime(plain).toMillis(), 7L);
        rig.db.addFile(secret.toString(), "md5", 1L, 9L);
        Map<String, FileObject> offered = rig.repo.getFileRepoDiff();
        assertTrue(offered.containsKey(plain.toString()), "an unconfirmed ordinary file is re-offered as before: " + offered.keySet());
        assertFalse(offered.containsKey(secret.toString()), "one in a live-tier root never is");
    }

    @Test
    void syncNeverWritesThroughALinkIntoALiveRoot() throws Exception {
        registerProvider(provider);
        Path mirror = Files.createDirectories(tmp.resolve("mirror"));
        Files.createSymbolicLink(mirror.resolve("into"), root);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("filerepo_name", "fr");
        cfg.put("repo_dir", mirror.toString());
        rig(cfg);
        Map<String, FileObject> diff = new LinkedHashMap<>();
        diff.put("/remote/new.txt", new FileObject("into/tenant-a/new.txt", "abc", "/remote/new.txt", 1L, 3L));
        diff.put("/remote/plain.txt", new FileObject("plain.txt", "def", "/remote/plain.txt", 1L, 3L));
        MsgEvent in = TestRig.request("repolistin", "transfer_id", "5");
        in.setCompressedParam("repolistin", gson.toJson(diff));
        assertEquals("10", rig.call(in).getParam("status_code"));

        long deadline = System.currentTimeMillis() + 10_000;
        while (rig.sent.isEmpty() && System.currentTimeMillis() < deadline) Thread.sleep(25);
        assertEquals(1, rig.sent.size(), "the updater confirmed");
        MsgEvent confirm = rig.sent.get(0);
        assertEquals("repoconfirm", confirm.getParam("action"));
        String failed = confirm.getCompressedParam("failedfiles");
        assertTrue(failed.contains("/remote/new.txt"), failed);
        assertEquals(List.of("/remote/plain.txt"), rig.downloads, "nothing was fetched through the link");
        assertFalse(Files.exists(root.resolve("tenant-a/new.txt")));
    }

    @Test
    void theRootIsRememberedAndHoldsWithoutAProvider() throws Exception {
        registerProvider(provider);
        onDemand();
        assertEquals("live-root", rig.call(pushRemote(secret)).getParam("refusal"));
        rig.close();
        client.close();

        FakeRegistry empty = new FakeRegistry();                            // the provider is gone
        client = new LiveTierClient(empty.contextOf("io.cresco.filerepo"),
                (CLogger) Proxy.newProxyInstance(CLogger.class.getClassLoader(), new Class<?>[]{CLogger.class}, (p, m, a) -> null),
                500, 500, 2);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("repo_dir", tmp.resolve("repo").toString());
        rig = new TestRig(tmp.resolve("agent"), cfg, client);
        assertFalse(client.present());
        assertEquals("live-root", rig.call(pushRemote(secret)).getParam("refusal"), "remembered in the catalog");
        Path ordinary = Files.write(tmp.resolve("ordinary.txt"), "shareable".getBytes());
        assertEquals("10", rig.call(pushRemote(ordinary)).getParam("status"));
    }

    @Test
    void aProviderWhoseRootCannotBeLearnedFailsClosedUntilOneCanBe() throws Exception {
        FakeProvider broken = new FakeProvider();
        broken.hang = new java.util.concurrent.CountDownLatch(1);           // registered, and never answers
        ServiceReference<?> ref = registerProvider(broken);
        onDemand();
        Path ordinary = Files.write(tmp.resolve("ordinary.txt"), "shareable".getBytes());
        MsgEvent r = rig.call(pushRemote(ordinary));
        assertEquals("live-root", r.getParam("refusal"), "fail closed: " + r.getParams());
        assertTrue(r.getParam("status_desc").contains("cannot be checked"), r.getParam("status_desc"));
        assertEquals("live-root", rig.call("getrepofilelist", "repo_name", "fr").getParam("refusal"));
        assertEquals("live-root", rig.call(pushRemote(ordinary)).getParam("refusal"));
        assertEquals(1, broken.requests.size(), "a failed learn is retried after a pause, not on every call");

        registry.unregister(ref);
        broken.hang.countDown();
        registerProvider(provider);                                         // a provider that answers
        assertEquals("10", rig.call("getrepofilelist", "repo_name", "fr").getParam("status"));
        assertEquals("10", rig.call(pushRemote(ordinary)).getParam("status"));
        assertEquals("live-root", rig.call(pushRemote(secret)).getParam("refusal"));
    }

    // ------------------------------------------------------------------ fail closed while any registered root is unknown

    LiveTierClient clientOver(FakeRegistry reg) {
        return new LiveTierClient(reg.contextOf("io.cresco.filerepo"),
                (CLogger) Proxy.newProxyInstance(CLogger.class.getClassLoader(), new Class<?>[]{CLogger.class}, (p, m, a) -> null),
                300, 300, 2);
    }

    @Test
    void aRememberedRootNeverStandsInForANewProvidersUnlearnedOne() throws Exception {
        // an earlier run: a provider whose root is A, learned and remembered in the catalog
        Path rootA = Files.createDirectories(tmp.resolve("liveA"));
        provider.root = rootA.toString();
        registerProvider(provider);
        onDemand();
        assertEquals("live-root", rig.call(pushRemote(rootA.resolve("x"))).getParam("refusal"));
        Map<String, Object> cfg = rig.config;
        rig.close();
        client.close();

        // now: the provider registered holds another root, and its list does not answer
        FakeRegistry now = new FakeRegistry();
        FakeProvider hung = new FakeProvider();
        hung.root = root.toString();
        hung.hang = new java.util.concurrent.CountDownLatch(1);
        ServiceReference<?> ref = now.register(FakeProvider.props(), b -> hung);
        client = clientOver(now);
        rig = new TestRig(tmp.resolve("agent"), cfg, client);
        Path ordinary = Files.write(tmp.resolve("ordinary.txt"), "shareable".getBytes());
        try {
            MsgEvent r = rig.call(pushRemote(secret));
            assertEquals("live-root", r.getParam("refusal"), "fail closed, root A notwithstanding: " + r.getParams());
            assertTrue(r.getParam("status_desc").contains("cannot be checked"), r.getParam("status_desc"));
            assertEquals("live-root", rig.call(pushRemote(ordinary)).getParam("refusal"), "every path, while its root is unknown");
            assertTrue(rig.sent.isEmpty(), "nothing was pushed");
        } finally {
            hung.hang.countDown();
        }

        now.unregister(ref);
        FakeProvider answering = new FakeProvider();
        answering.root = root.toString();
        now.register(FakeProvider.props(), b -> answering);
        assertEquals("live-root", rig.call(pushRemote(secret)).getParam("refusal"), "its root, learned");
        assertEquals("live-root", rig.call(pushRemote(rootA.resolve("x"))).getParam("refusal"), "and root A, remembered");
        assertEquals("10", rig.call(pushRemote(ordinary)).getParam("status"));
    }

    @Test
    void aRegisteredProviderThatDoesNotAdmitFilerepoKeepsEveryPathClosed() throws Exception {
        ServiceReference<?> ref = registry.register(FakeProvider.props(), b -> null);   // admits no bundle filerepo runs as
        onDemand();
        assertFalse(client.present());
        Path ordinary = Files.write(tmp.resolve("ordinary.txt"), "shareable".getBytes());
        for (Path p : new Path[]{secret, ordinary}) {
            MsgEvent r = rig.call(pushRemote(p));
            assertEquals("live-root", r.getParam("refusal"), r.getParams().toString());
            assertTrue(r.getParam("status_desc").contains("does not admit"), r.getParam("status_desc"));
        }
        assertEquals("live-root", rig.call("getrepofilelist", "repo_name", "fr").getParam("refusal"));
        assertTrue(rig.sent.isEmpty(), "nothing was pushed");

        registry.unregister(ref);                                          // gone: no provider and no root known
        assertEquals("10", rig.call(pushRemote(ordinary)).getParam("status"), "as before, without a provider");
    }

    @Test
    void everyRegisteredProvidersRootIsGuarded() throws Exception {
        Path rootB = Files.createDirectories(tmp.resolve("liveB"));
        Path secretB = Files.write(Files.createDirectories(rootB.resolve("t/c/v1")).resolve("b.txt"), "plaintext".getBytes());
        FakeProvider second = new FakeProvider();
        second.root = rootB.toString();
        registerProvider(provider);
        registerProvider(second);
        onDemand();
        assertEquals("live-root", rig.call(pushRemote(secret)).getParam("refusal"), "the first provider's root");
        assertEquals("live-root", rig.call(pushRemote(secretB)).getParam("refusal"), "and the second's");
        assertTrue(rig.sent.isEmpty(), "nothing was pushed");
        Path ordinary = Files.write(tmp.resolve("ordinary.txt"), "shareable".getBytes());
        assertEquals("10", rig.call(pushRemote(ordinary)).getParam("status"));
    }

    @Test
    void configuredRootsHoldBeforeAnyProviderHasRegistered() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("repo_dir", Files.createDirectories(tmp.resolve("repo")).toString());
        cfg.put("live_roots", tmp.resolve("elsewhere") + " , " + root);
        rig(cfg);
        assertFalse(client.present());
        assertEquals("live-root", rig.call(pushRemote(secret)).getParam("refusal"), "named in configuration");
        Path ordinary = Files.write(tmp.resolve("ordinary.txt"), "shareable".getBytes());
        assertEquals("10", rig.call(pushRemote(ordinary)).getParam("status"));
    }
}
