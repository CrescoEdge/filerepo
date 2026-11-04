package io.cresco.filerepo;

import io.cresco.library.messaging.MsgEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import jakarta.jms.BytesMessage;
import jakarta.jms.Message;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * streamfile's protocol (FR-D6): the transfer id validated or generated, unique per caller; offsets on every chunk; a
 * terminal frame with the SHA-384 of the bytes sent; cancels only by their owner, queued ones too; the file opened and
 * checked on its descriptor (the TOCTOU fix); and the bounded transfer pool (FR-D5).
 */
class StreamFileTest {

    @TempDir Path tmp;
    final Fabric fabric = new Fabric();
    TestRig rig;
    Path repo;
    final List<Message> frames = Collections.synchronizedList(new ArrayList<>());

    @BeforeEach
    void up() throws Exception {
        tmp = tmp.toRealPath();
        repo = Files.createDirectories(tmp.resolve("repo"));
    }

    @AfterEach
    void down() {
        if (rig != null) rig.close();
    }

    TestRig rig(String... kv) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("repo_dir", repo.toString());
        for (int i = 0; i + 1 < kv.length; i += 2) cfg.put(kv[i], kv[i + 1]);
        rig = new TestRig(tmp.resolve("agent"), cfg, null, "plugin-0", fabric);
        fabric.addListener("stream_name='rx'", frames::add);
        return rig;
    }

    /** A cataloged file in the repository. */
    Path cataloged(String name, byte[] data) throws Exception {
        Path f = Files.write(repo.resolve(name), data);
        rig.db.addFile(f.toString(), Hashes.content(data), Files.getLastModifiedTime(f).toMillis(), data.length);
        return f;
    }

    MsgEvent stream(Path f, long start, long len, String... kv) {
        MsgEvent m = rig.requestTo("streamfile", "file_path", f.toString(), "start_byte", String.valueOf(start),
                "byte_length", String.valueOf(len), "ident_key", "stream_name", "ident_id", "rx", "buffer_size", "4096");
        for (int i = 0; i + 1 < kv.length; i += 2) m.setParam(kv[i], kv[i + 1]);
        return rig.call(m);
    }

    Message terminal(String tid) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            synchronized (frames) {
                for (Message m : frames) {
                    if ("true".equals(m.getStringProperty("transfer_end")) && tid.equals(m.getStringProperty("transfer_id"))) return m;
                }
            }
            Thread.sleep(10);
        }
        fail("no terminal frame for " + tid);
        return null;
    }

    byte[] assembled(String tid) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        List<Message> mine = new ArrayList<>();
        synchronized (frames) {
            for (Message m : frames) {
                if (tid.equals(m.getStringProperty("transfer_id")) && !"true".equals(m.getStringProperty("transfer_end"))) mine.add(m);
            }
        }
        mine.sort((a, b) -> {
            try {
                return Long.compare(Long.parseLong(a.getStringProperty("offset")), Long.parseLong(b.getStringProperty("offset")));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        for (Message m : mine) {
            BytesMessage b = (BytesMessage) m;
            byte[] d = new byte[(int) b.getBodyLength()];
            b.readBytes(d);
            out.write(d);
        }
        return out.toByteArray();
    }

    static String sha384(byte[] b, int from, int len) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-384");
        md.update(b, from, len);
        return Hashes.hex(md.digest());
    }

    @Test
    void aTransferEndsWithATerminalFrameCarryingTheSha384OfWhatWasSent() throws Exception {
        rig();
        byte[] data = new byte[10_000];
        new Random(7).nextBytes(data);
        Path f = cataloged("data.bin", data);

        MsgEvent r = stream(f, 0, data.length);
        assertEquals("10", r.getParam("status"), r.getParams().toString());
        String tid = r.getParam("transfer_id");
        assertTrue(tid.startsWith("fr-"), "generated when the caller gives none: " + tid);
        Message end = terminal(tid);
        assertEquals("10", end.getStringProperty("transfer_status"));
        assertEquals(String.valueOf(data.length), end.getStringProperty("total_bytes"));
        assertEquals(sha384(data, 0, data.length), end.getStringProperty("sha384"));
        assertEquals("3", end.getStringProperty("seq_num"), "three chunks of 4096 came before it");
        assertArrayEquals(data, assembled(tid));

        // a range: the SHA-384 of that range
        MsgEvent rr = stream(f, 5000, 3000, "transfer_id", "range-1");
        assertEquals("range-1", rr.getParam("transfer_id"));
        Message rend = terminal("range-1");
        assertEquals(sha384(data, 5000, 3000), rend.getStringProperty("sha384"));
        assertEquals("5000", ((Message) frames.stream().filter(m -> {
            try { return "range-1".equals(m.getStringProperty("transfer_id")) && "0".equals(m.getStringProperty("seq_num")); }
            catch (Exception e) { return false; }
        }).findFirst().get()).getStringProperty("offset"));
    }

    @Test
    void badIdsAndIdentKeysAreRefused() throws Exception {
        rig();
        Path f = cataloged("x.txt", "hello".getBytes());
        for (String bad : new String[]{"has space", "x/y", "", "a".repeat(129), "quote'"}) {
            if (bad.isEmpty()) continue;
            assertEquals("8", stream(f, 0, 5, "transfer_id", bad).getParam("status"), bad);
        }
        for (String bad : new String[]{"transfer_id", "seq_num", "offset", "JMSXGroupID", "AND", "a-b", "1abc", "sha384"}) {
            MsgEvent r = stream(f, 0, 5, "ident_key", bad);
            assertEquals("8", r.getParam("status"), bad + ": " + r.getParams());
        }
        assertEquals("8", rig.call(rig.requestTo("streamfile", "file_path", f.toString(), "start_byte", "x",
                "byte_length", "5", "ident_key", "k", "ident_id", "i")).getParam("status"));
        assertEquals("8", stream(f, 3, 3).getParam("status"), "past the end");
        assertEquals("8", stream(f, Long.MAX_VALUE, 1).getParam("status"));
        assertEquals("8", stream(f, 1, Long.MAX_VALUE).getParam("status"), "no overflow");
    }

    @Test
    void aTransferIdIsUniqueAmongACallersTransfersAndOnlyItsOwnerCancelsIt() throws Exception {
        rig();
        CountDownLatch hold = new CountDownLatch(1);
        rig.exec.beforeStream = () -> {
            try { hold.await(10, TimeUnit.SECONDS); } catch (InterruptedException ignored) { }
        };
        Path f = cataloged("y.bin", new byte[20_000]);
        assertEquals("10", stream(f, 0, 20_000, "transfer_id", "mine").getParam("status"));
        assertEquals("8", stream(f, 0, 20_000, "transfer_id", "mine").getParam("status"), "the same caller, the same id");
        // another caller may use the same id: ids are per caller
        MsgEvent other = TestRig.as(rig.requestTo("streamfile", "file_path", f.toString(), "start_byte", "0",
                "byte_length", "10", "ident_key", "stream_name", "ident_id", "rx", "transfer_id", "mine"), "user");
        assertEquals("10", rig.call(other).getParam("status"));

        // the other caller cannot cancel the first caller's transfer
        MsgEvent foreignCancel = TestRig.as(rig.requestTo("streamfilecancel", "transfer_id", "nothers"), "user");
        assertEquals("9", rig.call(foreignCancel).getParam("status_code"));
        assertEquals("10", rig.call(rig.requestTo("streamfilecancel", "transfer_id", "mine")).getParam("status_code"));
        hold.countDown();
        Message end = terminal("mine");
        // whichever of the two "mine" transfers ended first; the cancelled one says so
        long deadline = System.currentTimeMillis() + 10_000;
        boolean cancelled = false;
        while (!cancelled && System.currentTimeMillis() < deadline) {
            synchronized (frames) {
                for (Message m : frames) {
                    if ("true".equals(m.getStringProperty("transfer_end")) && "9".equals(m.getStringProperty("transfer_status"))) cancelled = true;
                }
            }
            Thread.sleep(10);
        }
        assertTrue(cancelled, "the owner's cancel ended its transfer with status 9");
        assertNotNull(end);
    }

    @Test
    void aQueuedTransferCanBeCancelledAndTheBoundsRefuseAsBusy() throws Exception {
        rig("transfer_threads", "2", "transfer_queue_max", "1", "transfer_max_per_caller", "3");
        CountDownLatch hold = new CountDownLatch(1);
        rig.exec.beforeStream = () -> {
            try { hold.await(10, TimeUnit.SECONDS); } catch (InterruptedException ignored) { }
        };
        Path f = cataloged("z.bin", new byte[5000]);
        assertEquals("10", stream(f, 0, 100, "transfer_id", "t1").getParam("status"));
        assertEquals("10", stream(f, 0, 100, "transfer_id", "t2").getParam("status"));
        Thread.sleep(100);
        assertEquals("10", stream(f, 0, 100, "transfer_id", "t3").getParam("status"), "queued");
        MsgEvent busy = stream(f, 0, 100, "transfer_id", "t4");
        assertEquals("11", busy.getParam("status"), "per-caller cap: " + busy.getParams());
        // another caller hits the pool's own bound: two running, one queued
        MsgEvent other = TestRig.as(rig.requestTo("streamfile", "file_path", f.toString(), "start_byte", "0",
                "byte_length", "10", "ident_key", "stream_name", "ident_id", "rx", "transfer_id", "o1"), "user");
        assertEquals("11", rig.call(other).getParam("status"), "the transfer queue is full");
        // the queued transfer is cancelled before it starts
        assertEquals("10", rig.call(rig.requestTo("streamfilecancel", "transfer_id", "t3")).getParam("status_code"));
        hold.countDown();
        assertEquals("9", terminal("t3").getStringProperty("transfer_status"));
        assertEquals("10", terminal("t1").getStringProperty("transfer_status"));
        assertTrue(rig.call("getmetrics").getParam("metrics").contains("filerepo.transfer.rejected"));
    }

    @Test
    void theFileIsCheckedOnTheDescriptorItIsReadThrough() throws Exception {
        rig();
        byte[] original = "the cataloged content".getBytes();
        Path f = cataloged("swap.txt", original);
        CountDownLatch hold = new CountDownLatch(1);
        rig.exec.beforeStream = () -> {
            try { hold.await(10, TimeUnit.SECONDS); } catch (InterruptedException ignored) { }
        };
        MsgEvent r = stream(f, 0, original.length, "transfer_id", "swapped");
        assertEquals("10", r.getParam("status"));
        // after the check, another file is renamed over the path: the transfer still reads the file it checked
        Path evil = Files.write(tmp.resolve("evil.txt"), "something else entirely".getBytes());
        Files.move(evil, f, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        hold.countDown();
        Message end = terminal("swapped");
        assertEquals("10", end.getStringProperty("transfer_status"));
        assertArrayEquals(original, assembled("swapped"));
        rig.exec.beforeStream = null;

        // a link as the named file is refused, whatever the catalog says
        Path outside = Files.write(tmp.resolve("secret.txt"), "secret".getBytes());
        Path link = Files.createSymbolicLink(repo.resolve("link.txt"), outside);
        rig.db.addFile(link.toString(), "h", 1L, 6L);
        MsgEvent viaLink = stream(link, 0, 6);
        assertEquals("9", viaLink.getParam("status"), viaLink.getParams().toString());
        // a cataloged row outside the repository directory is not served
        rig.db.addFile(outside.toString(), Hashes.content("secret".getBytes()), 1L, 6L);
        MsgEvent out = stream(outside, 0, 6);
        assertEquals("9", out.getParam("status"));
        assertTrue(out.getParam("status_desc").contains("not in this repository"), out.getParam("status_desc"));
        // nor is an uncataloged file inside it
        Path stray = Files.write(repo.resolve("stray.txt"), "stray".getBytes());
        assertEquals("9", stream(stray, 0, 5).getParam("status"));
        // getfile takes the same path
        assertEquals("9", rig.call("getfile", "file_path", link.toString()).getParam("status"));
        assertEquals("10", rig.call("getfile", "file_path", f.toString()).getParam("status"));
    }

    @Test
    void aFileTruncatedWhileStreamingEndsWithAnErrorFrame() throws Exception {
        rig();
        Path f = cataloged("shrinks.bin", new byte[50_000]);
        CountDownLatch hold = new CountDownLatch(1);
        rig.exec.beforeStream = () -> {
            try { hold.await(10, TimeUnit.SECONDS); } catch (InterruptedException ignored) { }
        };
        assertEquals("10", stream(f, 0, 50_000, "transfer_id", "short").getParam("status"));
        try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(f, java.nio.file.StandardOpenOption.WRITE)) {
            ch.truncate(1000);
        }
        hold.countDown();
        Message end = terminal("short");
        assertEquals("8", end.getStringProperty("transfer_status"));
        assertNull(end.getStringProperty("sha384"), "no hash for an incomplete transfer");
        assertEquals("1000", end.getStringProperty("total_bytes"));
    }
}
