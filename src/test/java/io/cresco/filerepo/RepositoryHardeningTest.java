package io.cresco.filerepo;

import com.google.gson.Gson;
import io.cresco.library.messaging.MsgEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * putjar (FR-D3), putfiles (FR-D10), clearrepo's guards (FR-D2), one absolute repository directory (FR-D9), the jar
 * inventory cache (getjar), and batched catalog writes (FR-D8).
 */
class RepositoryHardeningTest {

    @TempDir Path tmp;
    final Gson gson = new Gson();
    TestRig rig;
    Path repo;

    @BeforeEach
    void up() throws Exception {
        tmp = tmp.toRealPath();
        repo = tmp.resolve("repo");
    }

    @AfterEach
    void down() {
        if (rig != null) rig.close();
    }

    TestRig onDemand(String... kv) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("repo_dir", Files.createDirectories(repo).toString());
        for (int i = 0; i + 1 < kv.length; i += 2) cfg.put(kv[i], kv[i + 1]);
        return rig = new TestRig(tmp.resolve("agent"), cfg, null);
    }

    MsgEvent putjar(String name, String jarfile, byte[] jar, String... kv) {
        MsgEvent m = TestRig.request("putjar", "pluginname", name, "jarfile", jarfile, "version", "1.3.0");
        m.setDataParam("jardata", jar);
        for (int i = 0; i + 1 < kv.length; i += 2) m.setParam(kv[i], kv[i + 1]);
        return rig.call(m);
    }

    static String sha384(byte[] b) throws Exception {
        return Hashes.hex(MessageDigest.getInstance("SHA-384").digest(b));
    }

    static void assertNoStaging(Path dir) throws Exception {
        SyncOverDataplaneTest.assertNoStaging(dir);
    }

    // ------------------------------------------------------------------ putjar (FR-D3)

    @Test
    void aSignedJarIsAcceptedWithTheHashTheRepoComputed() throws Exception {
        onDemand();
        byte[] jar = JarSigning.signedJar("io.cresco.example", "v1");
        MsgEvent r = putjar("io.cresco.example", "example.jar", jar);
        assertEquals("10", r.getParam("status"), r.getParams().toString());
        assertEquals(sha384(jar), r.getParam("sha384"));
        assertEquals(Hashes.content(jar), r.getParam("md5-confirm"));
        assertArrayEquals(jar, Files.readAllBytes(repo.resolve("example.jar")));
        assertNull(r.getDataParam("jardata"));
        assertNoStaging(repo);
        // the advisory md5, when given, must match; when it does, it proves nothing more
        assertEquals("10", putjar("io.cresco.example", "example2.jar", jar, "md5", Hashes.content(jar)).getParam("status"));
    }

    @Test
    void unsignedForeignTamperedAndMismatchedJarsAreRefusedAndNothingIsLeft() throws Exception {
        onDemand();
        MsgEvent unsigned = putjar("io.cresco.example", "u.jar", JarSigning.unsignedJar("io.cresco.example", "v1"));
        assertEquals("9", unsigned.getParam("status"));
        assertEquals("signature", unsigned.getParam("refusal"));
        MsgEvent foreign = putjar("io.cresco.example", "f.jar", JarSigning.foreignSignedJar("io.cresco.example", "v1"));
        assertEquals("signature", foreign.getParam("refusal"), foreign.getParams().toString());
        MsgEvent tampered = putjar("io.cresco.example", "t.jar", tamper(JarSigning.signedJar("io.cresco.example", "v1")));
        assertEquals("signature", tampered.getParam("refusal"), tampered.getParams().toString());
        byte[] good = JarSigning.signedJar("io.cresco.example", "v1");
        assertEquals("9", putjar("io.cresco.example", "m.jar", good, "md5", "0000").getParam("status"), "a supplied hash that differs");
        assertEquals("9", putjar("io.cresco.other", "n.jar", good).getParam("status"), "a name that is not the jar's own");
        for (String name : new String[]{"u.jar", "f.jar", "t.jar", "m.jar", "n.jar"}) assertFalse(Files.exists(repo.resolve(name)), name);
        assertNoStaging(repo);
        for (String bad : new String[]{"../escape.jar", "a/b.jar", ".filerepo-x.part", "..", "with space.jar"}) {
            assertEquals("8", putjar("io.cresco.example", bad, good).getParam("status"), bad);
        }
        assertFalse(Files.exists(tmp.resolve("escape.jar")));
        assertEquals("7", rig.call("putjar", "pluginname", "p").getParam("status"));
    }

    @Test
    void withNoTrustStoreConfiguredEveryJarIsRefused() throws Exception {
        onDemand();
        rig.signingTrustConfigured = false;
        MsgEvent r = putjar("io.cresco.example", "x.jar", JarSigning.signedJar("io.cresco.example", "v1"));
        assertEquals("9", r.getParam("status"));
        assertEquals("signature", r.getParam("refusal"), "fail closed without plugin_signing_truststore");
        assertFalse(Files.exists(repo.resolve("x.jar")));
    }

    @Test
    void onlyAnExplicitDevelopmentSettingLetsAnUnsignedJarThrough() throws Exception {
        onDemand();
        rig.signingRequired = false;
        byte[] unsigned = JarSigning.unsignedJar("io.cresco.example", "dev build");
        MsgEvent r = putjar("io.cresco.example", "dev.jar", unsigned);
        assertEquals("10", r.getParam("status"), r.getParams().toString());
        assertEquals(sha384(unsigned), r.getParam("sha384"), "the repo still computes its hash");
    }

    /** A signed jar with one entry's content changed after signing. */
    static byte[] tamper(byte[] jar) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ZipInputStream zin = new ZipInputStream(new java.io.ByteArrayInputStream(jar));
             ZipOutputStream zout = new ZipOutputStream(out)) {
            ZipEntry e;
            while ((e = zin.getNextEntry()) != null) {
                byte[] data = zin.readAllBytes();
                if (e.getName().endsWith("Payload.txt")) data = "tampered".getBytes();
                zout.putNextEntry(new ZipEntry(e.getName()));
                zout.write(data);
                zout.closeEntry();
            }
        }
        return out.toByteArray();
    }

    @Test
    void aProducersJarLandsWhereItsScannerLooksAndIsServedFromThere() throws Exception {
        Path scan = Files.createDirectories(tmp.resolve("scan"));
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("filerepo_name", "fr");
        cfg.put("scan_dir", scan.toString());
        cfg.put("repo_dir", Files.createDirectories(tmp.resolve("unused")).toString());
        rig = new TestRig(tmp.resolve("agent"), cfg, null);
        byte[] jar = JarSigning.signedJar("io.cresco.example", "v1");
        assertEquals("10", putjar("io.cresco.example", "example.jar", jar).getParam("status"));
        assertTrue(Files.exists(scan.resolve("example.jar")), "FR-D9: putjar and the scanner agree on the directory");
        rig.repo.scan();
        assertNotNull(rig.repo.getFileInfo(scan.resolve("example.jar").toString()), "cataloged for subscribers");
        MsgEvent got = rig.call("getjar", "action_pluginname", "io.cresco.example", "action_pluginmd5", Hashes.content(jar));
        assertEquals("10", got.getParam("status"), got.getParams().toString());
        assertArrayEquals(jar, got.getDataParam("jardata"));
    }

    // ------------------------------------------------------------------ the repository directory (FR-D9)

    @Test
    void withNoDirectoryConfiguredTheRepositoryIsAbsoluteUnderThePluginsDataDirectory() throws Exception {
        rig = new TestRig(tmp.resolve("agent"), new HashMap<>(), null);
        File dir = rig.repo.getRepoDir();
        assertTrue(dir.isAbsolute());
        assertEquals(tmp.resolve("agent/plugin-data/" + TestRig.PLUGIN_ID + "/repo").toFile(), dir);
        assertTrue(dir.isDirectory());
        assertEquals("10", putjar("io.cresco.example", "e.jar", JarSigning.signedJar("io.cresco.example", "v")).getParam("status"));
        assertTrue(new File(dir, "e.jar").isFile());
        assertFalse(new File("filerepo/e.jar").exists(), "never the working directory");
    }

    // ------------------------------------------------------------------ getjar's inventory

    @Test
    void theInventoryIsHashedOnceAndAJarChangedInPlaceIsNotServedUnderItsOldHash() throws Exception {
        onDemand("max_inline_bytes", "1000000");
        byte[] jar = JarSigning.signedJar("io.cresco.example", "v1");
        Files.write(repo.resolve("example.jar"), jar);
        Files.write(repo.resolve("notes.txt"), "not a jar".getBytes());
        rig.call("repolist");
        long hashed = rig.exec.jarInventory().hashed;
        for (int i = 0; i < 5; i++) {
            assertEquals("10", rig.call("getjar", "action_pluginname", "io.cresco.example",
                    "action_pluginmd5", Hashes.content(jar)).getParam("status"));
            rig.call("repolist");
        }
        assertEquals(hashed, rig.exec.jarInventory().hashed, "no re-hash while the files are unchanged");
        String list = rig.call("repolist").getCompressedParam("repolist");
        assertTrue(list.contains("example.jar") && !list.contains("notes.txt"), list);

        // changed in place keeping size, mtime and inode: the cache cannot see it, the recheck of the bytes sent does
        Path p = repo.resolve("example.jar");
        FileTime mtime = Files.getLastModifiedTime(p);
        byte[] changed = jar.clone();
        changed[changed.length / 2] ^= 0x01;
        try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(p, java.nio.file.StandardOpenOption.WRITE)) {
            ch.write(java.nio.ByteBuffer.wrap(changed, changed.length / 2, 1), changed.length / 2);
        }
        Files.setLastModifiedTime(p, mtime);
        MsgEvent stale = rig.call("getjar", "action_pluginname", "io.cresco.example", "action_pluginmd5", Hashes.content(jar));
        assertEquals("9", stale.getParam("status"), "never served under its old hash");
        assertNull(stale.getDataParam("jardata"));
        String relisted = rig.call("repolist").getCompressedParam("repolist");
        assertTrue(relisted.contains(Hashes.content(changed)), "listed again under the hash of what it now holds");
    }

    // ------------------------------------------------------------------ putfiles (FR-D10)

    MsgEvent putfiles(Map<String, String> hashes, boolean overwrite, Path... files) {
        MsgEvent m = TestRig.request("putfiles", "repo_name", "fr", "overwrite", String.valueOf(overwrite));
        for (Path f : files) m.addFile(f.toString());
        if (hashes != null) m.setCompressedParam("file_hashes", gson.toJson(hashes));
        return rig.call(m);
    }

    @Test
    void putfilesVerifiesEachFileAndSaysWhatItDid() throws Exception {
        onDemand();
        Path a = Files.write(tmp.resolve("a.txt"), "alpha".getBytes());
        Path b = Files.write(tmp.resolve("b.txt"), "beta".getBytes());

        MsgEvent wrong = putfiles(Map.of("a.txt", Hashes.content("not alpha".getBytes())), false, a);
        assertEquals("9", wrong.getParam("status"));
        assertTrue(wrong.getCompressedParam("failed").contains("a.txt"));
        assertFalse(Files.exists(repo.resolve("a.txt")), "a mismatch never lands");
        assertNoStaging(repo);

        MsgEvent noHash = putfiles(Map.of(), false, b);
        assertEquals("9", noHash.getParam("status"), "an expected hash is required");
        assertFalse(Files.exists(repo.resolve("b.txt")));

        MsgEvent ok = putfiles(Map.of("a.txt", Hashes.content("alpha".getBytes()), "b.txt", Hashes.content("beta".getBytes())), false, a, b);
        assertEquals("10", ok.getParam("status"), ok.getParams().toString());
        assertEquals("[\"a.txt\",\"b.txt\"]", ok.getCompressedParam("stored"));
        assertEquals(2, rig.repo.getRepoCount());

        // the same name with other content, overwrite=false: skipped, and not called success
        Path a2 = Files.write(Files.createDirectories(tmp.resolve("v2")).resolve("a.txt"), "alpha two".getBytes());
        MsgEvent skipped = putfiles(Map.of("a.txt", Hashes.content("alpha two".getBytes())), false, a2);
        assertEquals("9", skipped.getParam("status"));
        assertEquals("[\"a.txt\"]", skipped.getCompressedParam("skipped"));
        assertEquals("alpha", Files.readString(repo.resolve("a.txt")));

        // overwrite=true replaces it, and the catalog records the new content
        MsgEvent replaced = putfiles(Map.of("a.txt", Hashes.content("alpha two".getBytes())), true, a2);
        assertEquals("10", replaced.getParam("status"));
        assertEquals("alpha two", Files.readString(repo.resolve("a.txt")));
        assertEquals(Hashes.content("alpha two".getBytes()), rig.repo.getFileInfo(repo.resolve("a.txt").toString()).get("md5"));
    }

    @Test
    void putfilesremoteSendsTheHashesAndRefusesTwoFilesOfOneName() throws Exception {
        onDemand();
        Path a = Files.write(tmp.resolve("a.txt"), "alpha".getBytes());
        Path other = Files.write(Files.createDirectories(tmp.resolve("x")).resolve("a.txt"), "other".getBytes());
        MsgEvent m = TestRig.request("putfilesremote", "dst_region", "r2", "dst_agent", "a2", "dst_plugin", "p2", "repo_name", "fr");
        m.setCompressedParam("file_list", gson.toJson(List.of(a.toString())));
        assertEquals("10", rig.call(m).getParam("status"));
        MsgEvent sent = rig.sent.get(0);
        assertEquals("putfiles", sent.getParam("action"));
        assertEquals(gson.toJson(Map.of("a.txt", Hashes.content("alpha".getBytes()))), sent.getCompressedParam("file_hashes"));

        MsgEvent dup = TestRig.request("putfilesremote", "dst_region", "r2", "dst_agent", "a2", "dst_plugin", "p2", "repo_name", "fr");
        dup.setCompressedParam("file_list", gson.toJson(List.of(a.toString(), other.toString())));
        assertEquals("8", rig.call(dup).getParam("status"));
        assertEquals(1, rig.sent.size());
    }

    // ------------------------------------------------------------------ clearrepo (FR-D2)

    MsgEvent clear(String... kv) {
        MsgEvent m = TestRig.request("clearrepo");
        for (int i = 0; i + 1 < kv.length; i += 2) m.setParam(kv[i], kv[i + 1]);
        return rig.call(m);
    }

    @Test
    void aClearMustBeConfirmedWithTheExactDirectory() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("filerepo_name", "fr");
        cfg.put("repo_dir", Files.createDirectories(repo).toString());
        rig = new TestRig(tmp.resolve("agent"), cfg, null);       // a mirror: no force needed
        Path f = Files.write(repo.resolve("kept.txt"), "x".getBytes());
        assertEquals("unconfirmed", clear("repo_name", "fr").getParam("refusal"));
        assertEquals("unconfirmed", clear("repo_name", "fr", "confirm", tmp.toString()).getParam("refusal"));
        assertEquals("unconfirmed", clear("repo_name", "fr", "confirm", repo + "/").getParam("refusal"), "exactly");
        assertEquals("repo-name", clear("repo_name", "another", "confirm", repo.toString()).getParam("refusal"),
                "repo_name, when given, must name this repository");
        assertTrue(Files.exists(f));
        assertEquals("10", clear("repo_name", "fr", "confirm", repo.toString()).getParam("status"));
        assertFalse(Files.exists(f));
        Files.write(f, "x".getBytes());
        assertEquals("10", clear("confirm", repo.toString()).getParam("status"), "repo_name is optional");
    }

    @Test
    void forceNeedsTheAdminRole() throws Exception {
        onDemand();
        Path f = Files.write(repo.resolve("only.txt"), "only here".getBytes());
        assertEquals("force-needs-admin", clear("confirm", repo.toString(), "force", "true").getParam("refusal"), "no caller");
        assertEquals("force-needs-admin", rig.call(TestRig.as(TestRig.request("clearrepo", "confirm", repo.toString(),
                "force", "true"), "user")).getParam("refusal"));
        assertEquals("force-needs-admin", rig.call(TestRig.as(TestRig.request("clearrepo", "confirm", repo.toString(),
                "force", "true"), "infra", "plugin")).getParam("refusal"), "admin only, not infra");
        assertTrue(Files.exists(f));
        assertEquals("10", rig.call(TestRig.as(TestRig.request("clearrepo", "confirm", repo.toString(),
                "force", "true"), "admin")).getParam("status"));
        assertFalse(Files.exists(f));
    }

    @Test
    void aProtectedDirectoryIsNeverCleared() throws Exception {
        // a repository directory that holds the agent's own data directory
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("repo_dir", tmp.toString());
        rig = new TestRig(tmp.resolve("agent"), cfg, null);
        Path f = Files.write(tmp.resolve("precious.txt"), "x".getBytes());
        MsgEvent r = rig.call(TestRig.as(TestRig.request("clearrepo", "confirm", tmp.toString(), "force", "true"), "admin"));
        assertEquals("protected-dir", r.getParam("refusal"), r.getParams().toString());
        assertTrue(Files.exists(f));
        assertTrue(Files.exists(tmp.resolve("agent")));

        // the rule itself, without ever pointing a clear at them
        RepoEngine e = rig.repo;
        assertNotNull(e.protectedDir(new File("/")));
        assertNotNull(e.protectedDir(new File("/etc")));
        assertNotNull(e.protectedDir(new File("/usr/local")));
        assertNotNull(e.protectedDir(new File(System.getProperty("user.home"))));
        assertNotNull(e.protectedDir(new File(System.getProperty("user.home")).getParentFile()));
        assertNotNull(e.protectedDir(new File(System.getProperty("user.dir"))));
        assertNotNull(e.protectedDir(tmp.resolve("agent").toFile()), "the agent's data directory");
        assertNotNull(e.protectedDir(tmp.resolve("agent/plugin-data").toFile()), "holds this plugin's data directory");
        assertNull(e.protectedDir(tmp.resolve("agent/plugin-data/" + TestRig.PLUGIN_ID + "/repo").toFile()), "the default repo");
        assertNull(e.protectedDir(tmp.resolve("some/repo").toFile()));
    }

    @Test
    void aClearForgetsStaleRowsElsewhereAndRemovesLinksWithoutFollowingThem() throws Exception {
        onDemand();
        Path elsewhere = Files.write(tmp.resolve("elsewhere.txt"), "not this repository's".getBytes());
        rig.db.addFile(elsewhere.toString(), "h", 1L, 1L);
        Path target = Files.write(tmp.resolve("target.txt"), "linked to".getBytes());
        Files.createSymbolicLink(repo.resolve("link.txt"), target);
        Files.write(repo.resolve("mine.txt"), "mine".getBytes());
        MsgEvent r = rig.call(TestRig.as(TestRig.request("clearrepo", "confirm", repo.toString(), "force", "true"), "admin"));
        assertEquals("10", r.getParam("status"), r.getParams().toString());
        assertTrue(Files.exists(elsewhere), "a row outside the directory is forgotten, its file left alone");
        assertTrue(Files.exists(target), "a link is removed, its target left alone");
        assertFalse(Files.exists(repo.resolve("link.txt"), java.nio.file.LinkOption.NOFOLLOW_LINKS));
        assertFalse(Files.exists(repo.resolve("mine.txt")));
        assertEquals(0, rig.repo.getRepoCount());
    }

    @Test
    void aClearOfADirectoryThatDoesNotExistYetIsAnEmptyClear() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("filerepo_name", "fr");
        cfg.put("repo_dir", tmp.resolve("not-yet").toString());
        rig = new TestRig(tmp.resolve("agent"), cfg, null);
        MsgEvent r = clear("confirm", tmp.resolve("not-yet").toString());
        assertEquals("10", r.getParam("status"), r.getParams().toString());
        assertFalse(r.getParam("status_desc").contains(tmp.toString()), "no path in the reply");
    }

    // ------------------------------------------------------------------ batched catalog writes (FR-D8)

    @Test
    void aLargeScanIsWrittenInBatchesNotOneCommitPerFile() throws Exception {
        Path scan = Files.createDirectories(tmp.resolve("scan"));
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("filerepo_name", "fr");
        cfg.put("scan_dir", scan.toString());
        cfg.put("scan_batch_size", "250");
        rig = new TestRig(tmp.resolve("agent"), cfg, null);
        for (int i = 0; i < 1000; i++) Files.write(scan.resolve("f" + i), ("file " + i).getBytes());
        long before = rig.db.batchCommits.get();
        rig.repo.scan();
        assertEquals(1000, rig.repo.getRepoCount());
        assertEquals(4, rig.db.batchCommits.get() - before, "1000 rows in 4 commits of 250");
        // an update batch too
        for (int i = 0; i < 300; i++) {
            Files.write(scan.resolve("f" + i), ("changed " + i).getBytes());
        }
        before = rig.db.batchCommits.get();
        rig.repo.scan();
        assertEquals(2, rig.db.batchCommits.get() - before);
        assertEquals(Hashes.content("changed 7".getBytes()), rig.repo.getFileInfo(scan.resolve("f7").toString()).get("md5"));
        // and a rescan with nothing changed writes nothing
        before = rig.db.batchCommits.get();
        rig.repo.scan();
        assertEquals(0, rig.db.batchCommits.get() - before);
    }

    @Test
    void stagingFilesAreNeverScannedOrListed() throws Exception {
        Path scan = Files.createDirectories(tmp.resolve("scan"));
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("filerepo_name", "fr");
        cfg.put("scan_dir", scan.toString());
        rig = new TestRig(tmp.resolve("agent"), cfg, null);
        Files.write(scan.resolve(".filerepo-0123.part"), "half written".getBytes());
        Files.write(scan.resolve("real.txt"), "done".getBytes());
        rig.repo.scan();
        assertEquals(1, rig.repo.getRepoCount());
        assertNull(rig.repo.getFileInfo(scan.resolve(".filerepo-0123.part").toString()));
    }
}
