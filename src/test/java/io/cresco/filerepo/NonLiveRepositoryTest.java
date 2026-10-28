package io.cresco.filerepo;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.utilities.CLogger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A repository that is not live-backed answers the verbs this change touched exactly as before, whether or not a
 * live-tier provider is registered on the agent: putfiles lands, catalogs and confirms; getfile, streamfile,
 * removefile, getrepofilelist, repolist, putjar and getscandir answer as they did.
 */
class NonLiveRepositoryTest {

    @TempDir Path tmp;
    final FakeRegistry registry = new FakeRegistry();
    final FakeProvider provider = new FakeProvider();
    final Gson gson = new Gson();
    LiveTierClient client;
    TestRig rig;

    @AfterEach
    void down() {
        if (rig != null) rig.close();
        if (client != null) client.close();
    }

    TestRig onDemand(boolean withProvider) throws Exception {
        // a canonical directory: putfiles catalogs a landed file by its absolute path and removefile drops the row by
        // its canonical path, so through a symlinked temp dir (/var on macOS) the row would outlive the file
        tmp = tmp.toRealPath();
        if (withProvider) {
            registry.register(FakeProvider.props(), b -> "io.cresco.filerepo".equals(b.getSymbolicName()) ? provider : null);
        }
        client = new LiveTierClient(registry.contextOf("io.cresco.filerepo"),
                (CLogger) Proxy.newProxyInstance(CLogger.class.getClassLoader(), new Class<?>[]{CLogger.class}, (p, m, a) -> null),
                500, 500, 2);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("repo_dir", Files.createDirectories(tmp.resolve("repo")).toString());
        cfg.put("max_inline_bytes", "64");
        return rig = new TestRig(tmp.resolve("agent"), cfg, client);
    }

    @Test
    void theTouchedVerbsAnswerAsBeforeWithoutAProvider() throws Exception {
        assertVerbsAsBefore(onDemand(false));
    }

    @Test
    void theTouchedVerbsAnswerAsBeforeWithAProviderPresent() throws Exception {
        assertVerbsAsBefore(onDemand(true));
        assertEquals(List.of(Map.of("op", "list")), provider.requests,
                "a repository that is not live-backed asks the provider once, for its live root, and nothing else");
    }

    void assertVerbsAsBefore(TestRig rig) throws Exception {
        Path repo = tmp.resolve("repo");
        Path src = Files.write(tmp.resolve("small.txt"), "small file".getBytes());

        // putfiles: lands a copy, catalogs it (no scanner here), and confirms to the sender
        MsgEvent put = TestRig.request("putfiles", "repo_name", "fr", "transfer_id", "7");
        put.addFile(src.toString());
        MsgEvent pr = rig.call(put);
        assertEquals("10", pr.getParam("status"));
        assertEquals("putfiles stored", pr.getParam("status_desc"));
        Path landed = repo.resolve("small.txt");
        assertTrue(Files.exists(landed));
        assertEquals(1, rig.repo.getRepoCount());
        assertEquals(1, rig.sent.size());
        assertEquals("repoconfirm", rig.sent.get(0).getParam("action"));
        assertEquals("7", rig.sent.get(0).getParam("transfer_id"));

        // putfiles again without overwrite: nothing replaced, still answered stored (as before)
        assertEquals("10", rig.call(put).getParam("status"));

        // getfile: a cataloged file inline; an uncataloged path refused; an oversized one sent to streamfile
        MsgEvent got = rig.call("getfile", "file_path", landed.toAbsolutePath().toString());
        assertEquals("10", got.getParam("status"));
        assertArrayEquals("small file".getBytes(), got.getDataParam("file_data"));
        assertEquals("9", rig.call("getfile", "file_path", src.toAbsolutePath().toString()).getParam("status"));
        assertEquals("8", rig.call("getfile").getParam("status"));
        Path big = Files.write(tmp.resolve("big.bin"), new byte[200]);
        MsgEvent putBig = TestRig.request("putfiles", "repo_name", "fr");
        putBig.addFile(big.toString());
        assertEquals("10", rig.call(putBig).getParam("status"));
        assertEquals("5", rig.call("getfile", "file_path", repo.resolve("big.bin").toAbsolutePath().toString()).getParam("status"));

        // streamfile: range checks as before (the transfer itself runs on the dataplane, not here)
        String lp = landed.toAbsolutePath().toString();
        assertEquals("8", rig.call("streamfile", "file_path", lp, "start_byte", "0", "byte_length", "999",
                "transfer_id", "t", "ident_key", "k", "ident_id", "i").getParam("status"));
        assertEquals("10", rig.call("streamfile", "file_path", lp, "start_byte", "0", "byte_length", "4",
                "transfer_id", "t", "ident_key", "k", "ident_id", "i").getParam("status"));

        // getrepofilelist: the catalog, paged
        MsgEvent list = rig.call("getrepofilelist", "repo_name", "fr", "limit", "1", "offset", "0");
        assertEquals("10", list.getParam("status"));
        assertEquals("2", list.getParam("repo_total"));
        List<Map<String, String>> rows = gson.fromJson(list.getCompressedParam("repofilelist"), new TypeToken<List<Map<String, String>>>(){}.getType());
        assertEquals(1, rows.size());

        // repolist: inventory and server, as before
        MsgEvent inv = rig.call("repolist");
        assertNotNull(inv.getCompressedParam("repolist"));
        assertNull(inv.getParam("refusal"));

        // putjar: md5-verified as before; a mismatch is deleted and refused
        Path probe = Files.write(tmp.resolve("probe.jar"), "jar bytes".getBytes());
        MsgEvent jar = TestRig.request("putjar", "pluginname", "p", "md5", rig.plugin.getMD5(probe.toString()),
                "jarfile", "p.jar", "version", "1");
        jar.setDataParam("jardata", "jar bytes".getBytes());
        MsgEvent jr = rig.call(jar);
        assertEquals("10", jr.getParam("status"));
        assertEquals("p", jr.getParam("uploaded"));
        assertNull(jr.getParam("jardata"));
        MsgEvent bad = TestRig.request("putjar", "pluginname", "q", "md5", "0000", "jarfile", "q.jar", "version", "1");
        bad.setDataParam("jardata", "other bytes".getBytes());
        assertEquals("9", rig.call(bad).getParam("status"));
        assertFalse(Files.exists(repo.resolve("q.jar")));
        assertEquals("7", rig.call("putjar", "pluginname", "p").getParam("status"), "missing params, as before");

        // removefile: disk and catalog, containment as before
        assertEquals("9", rig.call("removefile", "repo_name", "fr", "file_name", "../small.txt").getParam("status"));
        assertTrue(Files.exists(src));
        assertEquals("10", rig.call("removefile", "repo_name", "fr", "file_name", "small.txt").getParam("status"));
        assertFalse(Files.exists(landed));
        assertEquals(1, rig.repo.getRepoCount());

        // getscandir: unset here, as before
        assertEquals("9", rig.call("getscandir").getParam("status"));

        // clearrepo: no repo_name, as before
        assertEquals("9", rig.call("clearrepo").getParam("status"));

        // an unknown verb still answers nothing
        assertNull(rig.call("averbaddedlater"));

        // the live verbs: liverealise is for live-backed repositories only
        assertEquals("not-live-backed", rig.call("liverealise").getParam("refusal"));
        assertTrue(Arrays.asList("ONLY_COPY").contains(rig.call("repostate").getParam("state")));
    }

    @Test
    void liverelayIsANodeServiceAnyRepositoryMayCarry() throws Exception {
        onDemand(true);
        MsgEvent q = TestRig.request("liverelay");
        q.setCompressedParam("request", gson.toJson(Map.of("action", "core.livelist", "principal", "user-a", "sig", "s")));
        MsgEvent r = rig.call(q);
        assertEquals("10", r.getParam("status"), r.getParams().toString());
        assertEquals("10", r.getParam("reply_status"));
    }
}
