package io.cresco.filerepo;

import io.cresco.library.data.DataPlaneService;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import jakarta.jms.BytesMessage;
import jakarta.jms.Message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The receiving half of a sync over the dataplane (FR-D7): files are pulled from the producer with its own
 * {@code streamfile}, so bulk bytes ride the dataplane and only one small request per file crosses the control plane
 * (the old path pulled every byte through control-plane {@code getfiledata} RPCs).
 *
 * <p>One dataplane listener serves a whole session (one diff from one producer), selected by a random session id;
 * each file is one or more transfers, told apart by {@code transfer_id}. Chunks are written where their
 * {@code offset} says, so loss and reordering are both visible: after the terminal frame, any byte range not received is
 * asked for again, a bounded number of times. Nothing is committed until the whole file is present, its SHA-384 equals
 * the one the sender's terminal frame carried (the bytes it read), and its content hash equals the one the producer
 * offered (the file it cataloged). The file is assembled in a staging file beside its destination and moved into place
 * atomically, so a partial or unverified file is never visible under its name.
 */
final class SyncReceiver implements AutoCloseable {

    /** The dataplane property a sync session's frames are selected by. */
    static final String IDENT_KEY = "filerepo_rx";

    private static final SecureRandom RANDOM = new SecureRandom();

    private final PluginBuilder plugin;
    private final CLogger logger;
    private final String region, agent, pluginId;
    private final int bufferSize;
    private final long idleMs;
    private final int rounds;
    private final long rpcTimeoutMs;
    private final String session = token();
    private final Map<String, Transfer> byTransfer = new ConcurrentHashMap<>();
    private String listenerId;

    /** The outcome of one fetch. */
    static final class Result {
        final boolean ok;
        final String why;
        final boolean verifyFailed;

        private Result(boolean ok, String why, boolean verifyFailed) {
            this.ok = ok;
            this.why = why;
            this.verifyFailed = verifyFailed;
        }

        static Result ok() { return new Result(true, null, false); }
        static Result failed(String why) { return new Result(false, why, false); }
        static Result unverified(String why) { return new Result(false, why, true); }
        @Override public String toString() { return ok ? "ok" : why; }
    }

    SyncReceiver(PluginBuilder plugin, CLogger logger, String region, String agent, String pluginId,
                 int bufferSize, long idleMs, int rounds, long rpcTimeoutMs) {
        this.plugin = plugin;
        this.logger = logger;
        this.region = region;
        this.agent = agent;
        this.pluginId = pluginId;
        this.bufferSize = bufferSize;
        this.idleMs = idleMs;
        this.rounds = Math.max(1, rounds);
        this.rpcTimeoutMs = rpcTimeoutMs;
    }

    private DataPlaneService dataplane() {
        return plugin.getAgentService().getDataPlaneService();
    }

    /** Start listening for this session's frames. */
    SyncReceiver open() {
        listenerId = dataplane().addMessageListener(TopicType.GLOBAL, this::onMessage, IDENT_KEY + "='" + session + "'");
        if (listenerId == null) {
            throw new IllegalStateException("the dataplane did not register a listener");
        }
        return this;
    }

    @Override
    public void close() {
        String id = listenerId;
        listenerId = null;
        if (id != null) {
            try {
                dataplane().removeMessageListener(id);
            } catch (Exception ex) {
                logger.error("sync listener removal failed: " + ex.getMessage());
            }
        }
    }

    private void onMessage(Message m) {
        try {
            String tid = m.getStringProperty("transfer_id");
            Transfer t = tid == null ? null : byTransfer.get(tid);
            if (t != null) t.onFrame(m, tid);
        } catch (Exception ex) {
            logger.error("sync frame error: " + ex.getMessage());
        }
    }

    /** Puts a verified staging file in place; false when it may not be any more (nothing is changed then). */
    interface Committer {
        boolean commit(Path part, Path destination) throws IOException;
    }

    /** A plain atomic move into place. */
    static final Committer MOVE = (part, destination) -> {
        Files.move(part, destination, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        return true;
    };

    Result fetch(String remotePath, long size, String expectedHash, Path destination) {
        return fetch(remotePath, size, expectedHash, destination, MOVE);
    }

    /**
     * Pull {@code remotePath} ({@code size} bytes, content hash {@code expectedHash}) from the producer into
     * {@code destination}, committed (once verified) by {@code committer}. On success the verified file is in place;
     * on failure, or when the committer declines, nothing under {@code destination} has changed.
     */
    Result fetch(String remotePath, long size, String expectedHash, Path destination, Committer committer) {
        Path part = RepoEngine.stagingFor(destination);
        boolean committed = false;
        try {
            if (size == 0) {
                Files.write(part, new byte[0], StandardOpenOption.CREATE_NEW);
                String[] h = Hashes.both(part);
                if (!expectedHash.equals(h[0])) return Result.unverified("an empty file does not have the offered content hash");
                if (!committer.commit(part, destination)) return Result.failed("not committed: superseded here meanwhile");
                committed = true;
                return Result.ok();
            }
            String sha384 = null;
            try (FileChannel ch = FileChannel.open(part, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE,
                    StandardOpenOption.READ)) {
                Transfer t = new Transfer(ch, size);
                String why = "no attempt";
                // the whole file, until a terminal frame says what the sender read
                for (int round = 0; round < rounds && sha384 == null; round++) {
                    String tid = token();
                    byTransfer.put(tid, t);
                    try {
                        t.touch();
                        String refused = request(remotePath, 0, size, tid);
                        if (refused != null) {
                            why = refused;
                            if (refused.startsWith("busy")) {
                                Thread.sleep(Math.min(2000L, 250L * (round + 1)));
                                continue;
                            }
                            return Result.failed(refused);
                        }
                        Terminal end = t.awaitEnd(tid, idleMs);
                        if (end == null) {
                            cancel(tid);
                            why = "the stream stalled (no frame for " + idleMs + " ms)";
                            continue;
                        }
                        if (!"10".equals(end.status)) {
                            return Result.failed("the producer ended the stream: " + end.status + " " + end.desc);
                        }
                        if (end.totalBytes != size || end.sha384 == null) {
                            return Result.failed("the producer sent " + end.totalBytes + " of " + size
                                    + " bytes (the file changed since it was offered)");
                        }
                        sha384 = end.sha384;
                    } finally {
                        byTransfer.remove(tid);
                    }
                }
                if (sha384 == null) return Result.failed(why);
                // any range lost in transit is asked for again
                for (int round = 0; round < rounds; round++) {
                    List<long[]> gaps = t.gaps();
                    if (gaps.isEmpty()) break;
                    for (long[] gap : gaps) {
                        String tid = token();
                        byTransfer.put(tid, t);
                        try {
                            t.touch();
                            if (request(remotePath, gap[0], gap[1] - gap[0], tid) != null) break;
                            if (t.awaitEnd(tid, idleMs) == null) cancel(tid);
                        } finally {
                            byTransfer.remove(tid);
                        }
                    }
                }
                List<long[]> left = t.gaps();
                if (!left.isEmpty()) return Result.failed(left.size() + " byte range(s) still missing after " + rounds + " rounds");
                if (t.broken != null) return Result.failed(t.broken);
                ch.force(true);
            }
            // verify before commit: the bytes the sender read, and the content the producer offered
            String[] h = Hashes.both(part);
            if (!h[1].equals(sha384)) {
                return Result.unverified("SHA-384 of the assembled file differs from the sender's terminal frame");
            }
            if (!h[0].equals(expectedHash)) {
                return Result.unverified("the file differs from the content the producer offered (changed since the offer)");
            }
            if (!committer.commit(part, destination)) return Result.failed("not committed: superseded here meanwhile");
            committed = true;
            return Result.ok();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return Result.failed("interrupted");
        } catch (Exception ex) {
            return Result.failed(ex.getClass().getSimpleName() + ": " + ex.getMessage());
        } finally {
            if (!committed) {
                try {
                    Files.deleteIfExists(part);
                } catch (Exception ignore) {
                }
            }
        }
    }

    /** Ask the producer to stream a range; null when it accepted, otherwise why not ("busy..." when it may retry). */
    private String request(String remotePath, long start, long length, String tid) {
        MsgEvent req = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, region, agent, pluginId);
        req.setParam("action", "streamfile");
        req.setParam("file_path", remotePath);
        req.setParam("start_byte", String.valueOf(start));
        req.setParam("byte_length", String.valueOf(length));
        req.setParam("transfer_id", tid);
        req.setParam("ident_key", IDENT_KEY);
        req.setParam("ident_id", session);
        req.setParam("buffer_size", String.valueOf(bufferSize));
        MsgEvent reply = plugin.sendRPC(req, rpcTimeoutMs);
        if (reply == null) return "the producer did not answer streamfile";
        String status = reply.getParam("status");
        if ("10".equals(status)) return null;
        String desc = reply.getParam("status_desc");
        if ("11".equals(status)) return "busy: " + desc;
        return "streamfile refused (" + status + "): " + desc;
    }

    private void cancel(String tid) {
        try {
            MsgEvent c = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, region, agent, pluginId);
            c.setParam("action", "streamfilecancel");
            c.setParam("transfer_id", tid);
            plugin.msgOut(c);
        } catch (Exception ex) {
            logger.debug("streamfilecancel not sent: " + ex.getMessage());
        }
    }

    static String token() {
        byte[] b = new byte[16];
        RANDOM.nextBytes(b);
        return Hashes.hex(b);
    }

    /** A terminal frame: how the sender ended one transfer. */
    static final class Terminal {
        final String status, desc, sha384;
        final long totalBytes;

        Terminal(String status, String desc, String sha384, long totalBytes) {
            this.status = status;
            this.desc = desc;
            this.sha384 = sha384;
            this.totalBytes = totalBytes;
        }
    }

    /** One file being assembled: what arrived where, and how each of its transfers ended. */
    static final class Transfer {
        private final FileChannel ch;
        private final long size;
        /** received ranges, merged: start -> end (exclusive) */
        private final TreeMap<Long, Long> have = new TreeMap<>();
        private final Map<String, Terminal> ends = new ConcurrentHashMap<>();
        private long lastFrame = System.currentTimeMillis();
        volatile String broken;

        Transfer(FileChannel ch, long size) {
            this.ch = ch;
            this.size = size;
        }

        synchronized void touch() {
            lastFrame = System.currentTimeMillis();
        }

        synchronized void onFrame(Message m, String tid) throws Exception {
            lastFrame = System.currentTimeMillis();
            if ("true".equals(m.getStringProperty("transfer_end"))) {
                long total = -1;
                try { total = Long.parseLong(m.getStringProperty("total_bytes")); } catch (Exception ignore) { }
                ends.put(tid, new Terminal(m.getStringProperty("transfer_status"), m.getStringProperty("transfer_desc"),
                        m.getStringProperty("sha384"), total));
                notifyAll();
                return;
            }
            if (!(m instanceof BytesMessage)) return;
            long offset;
            try {
                offset = Long.parseLong(m.getStringProperty("offset"));
            } catch (Exception e) {
                broken = "a frame without an offset";
                return;
            }
            BytesMessage bm = (BytesMessage) m;
            long len = bm.getBodyLength();
            if (offset < 0 || len < 0 || len > Integer.MAX_VALUE || offset > size - len) {
                broken = "a frame outside the file (offset " + offset + ", length " + len + ")";
                return;
            }
            byte[] data = new byte[(int) len];
            bm.readBytes(data);
            ByteBuffer buf = ByteBuffer.wrap(data);
            long pos = offset;
            while (buf.hasRemaining()) pos += ch.write(buf, pos);
            add(offset, offset + len);
        }

        private void add(long start, long end) {
            if (end <= start) return;
            Map.Entry<Long, Long> before = have.floorEntry(start);
            if (before != null && before.getValue() >= start) {
                start = before.getKey();
                end = Math.max(end, before.getValue());
            }
            Map.Entry<Long, Long> next = have.ceilingEntry(start);
            while (next != null && next.getKey() <= end) {
                end = Math.max(end, next.getValue());
                have.remove(next.getKey());
                next = have.ceilingEntry(start);
            }
            have.put(start, end);
        }

        /** The ranges not yet received, as {start, end}. */
        synchronized List<long[]> gaps() {
            List<long[]> out = new ArrayList<>();
            long pos = 0;
            for (Map.Entry<Long, Long> e : have.entrySet()) {
                if (e.getKey() > pos) out.add(new long[]{pos, e.getKey()});
                pos = Math.max(pos, e.getValue());
            }
            if (pos < size) out.add(new long[]{pos, size});
            return out;
        }

        /** The terminal frame of {@code tid}, or null when no frame arrived for {@code idleMs}. */
        synchronized Terminal awaitEnd(String tid, long idleMs) throws InterruptedException {
            while (!ends.containsKey(tid)) {
                long idle = System.currentTimeMillis() - lastFrame;
                if (idle >= idleMs) return null;
                wait(Math.max(1L, idleMs - idle));
            }
            return ends.get(tid);
        }
    }
}
