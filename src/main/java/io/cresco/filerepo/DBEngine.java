package io.cresco.filerepo;

import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.io.File;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBEngine {

    private DataSource ds;
    private CLogger logger;

    private List<String> tablesNames;

    private PluginBuilder pluginBuilder;

    private PoolableConnectionFactory poolableConnectionFactory;
    private ObjectPool<PoolableConnection> connectionPool;
    private PoolingDataSource<PoolableConnection> dataSource;
    private File dbsource;
    /** Transactions committed by the batched writers (tests: FR-D8 writes a scan in few commits, not one per file). */
    final java.util.concurrent.atomic.AtomicLong batchCommits = new java.util.concurrent.atomic.AtomicLong();

    public DBEngine(PluginBuilder plugin) {

        try {
            logger = plugin.getLogger(DBEngine.class.getName(), CLogger.Level.Info);

            this.pluginBuilder = plugin;

            tablesNames = new ArrayList<>();
            tablesNames.add("filelist");

            String dbName = "filerepo-db";
            String dbPath = plugin.getPluginDataDirectory() + "/derbydb-home/" + dbName;

            dbsource = Paths.get(dbPath).toFile();

            String dbDriver = plugin.getConfig().getStringParam("db_driver", "org.apache.derby.jdbc.EmbeddedDriver");
            String dbConnectionString = plugin.getConfig().getStringParam("db_jdbc", "jdbc:derby:" + dbsource.getAbsolutePath()  + ";create=true");

            Class.forName(dbDriver);

            ds = setupDataSource(dbConnectionString);

            if (dbsource.exists()) {
                logger.debug("DB SOURCE EXIST: " + dbsource.getAbsolutePath() );
            } else {
                logger.debug("CREATING DB DBSOURCE: " + dbsource.getAbsolutePath());
            }
            // Verify the SCHEMA, not just the directory. A crash/disk-full during first boot can
            // leave a booted DB dir with no FILELIST table; the old dir-exists check then skipped
            // initDB forever, wedging the catalog silently. ensureSchema() is idempotent.
            ensureSchema();

        } catch (Exception ex) {
            logger.error("DBEngine init failed", ex);
        }
    }

    public boolean shutdown() {
        boolean isShutdown = false;

        // Close the DBCP pool FIRST. The Derby ";shutdown=true" connection below ALWAYS throws on
        // success, so the previous ordering (close after getConnection) made these closes dead code
        // and the pool + its connections leaked on every OSGi reload.
        try {
            if (dataSource != null) dataSource.close();
        } catch (Exception ex) { logger.error("DBEngine dataSource close error", ex); }
        try {
            if (connectionPool != null) connectionPool.close();
        } catch (Exception ex) { logger.error("DBEngine connectionPool close error", ex); }

        try {
            if(dbsource != null && dbsource.exists()) {
                String shutdownString = "jdbc:derby:" + dbsource.getAbsolutePath() + ";shutdown=true";
                DriverManager.getConnection(shutdownString);
            }
        } catch (SQLException e) {
            // XJ015 (SQLCODE 50000) = full system shutdown; 08006 (45000) = single db shutdown. Both expected.
            if (e.getErrorCode() == 50000 || e.getErrorCode() == 45000) {
                isShutdown = true;
            } else {
                logger.error("DBEngine shutdown error", e);
            }
        } catch (Exception ex) {
            logger.error("DBEngine shutdown error", ex);
        }
        return isShutdown;
    }

    /** Create the FILELIST and REPOMETA tables if they are absent. Idempotent; safe to run on every boot. */
    public void ensureSchema() {
        try (Connection conn = ds.getConnection()) {
            boolean exists;
            // Derby folds unquoted identifiers to upper case
            try (ResultSet rs = conn.getMetaData().getTables(null, null, "FILELIST", null)) {
                exists = rs.next();
            }
            if (!exists) {
                logger.info("FILELIST table absent; creating catalog schema");
                initDB();
            }
            boolean metaExists;
            try (ResultSet rs = conn.getMetaData().getTables(null, null, "REPOMETA", null)) {
                metaExists = rs.next();
            }
            if (!metaExists) {
                initMeta();
            }
            boolean tombExists;
            try (ResultSet rs = conn.getMetaData().getTables(null, null, "TOMBSTONE", null)) {
                tombExists = rs.next();
            }
            if (!tombExists) {
                initTombstones();
            }
            for (String[] t : SYNC_TABLES) {
                boolean has;
                try (ResultSet rs = conn.getMetaData().getTables(null, null, t[0], null)) {
                    has = rs.next();
                }
                if (!has) {
                    try (Statement stmt = conn.createStatement()) {
                        stmt.executeUpdate(t[1]);
                    } catch (Exception ex) {
                        logger.error("could not create " + t[0], ex);
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("ensureSchema error", ex);
        }
    }

    /**
     * TOMBSTONE holds the files a producer's scanner found deleted (FR-D4): each is offered to the subscribers as a
     * delete until one confirms it (insync=1), then kept until it ages out, so the delete is not lost while a subscriber
     * is away. Its own table, so the FILELIST of an existing catalog needs no migration.
     */
    private void initTombstones() {
        String create = "CREATE TABLE tombstone" +
                "(" +
                "   filepath varchar(1000) primary key NOT NULL," +
                "   filename varchar(1000) NOT NULL," +
                "   md5 varchar(255)," +
                "   deleted_at bigint NOT NULL," +
                "   insync int NOT NULL" +
                ")";
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(create);
        } catch (Exception ex) {
            logger.error("initTombstones error", ex);
        }
    }

    /**
     * Sync state (protocol 2). A producer keeps, per subscriber, what that subscriber still needs: SUBSCRIBERS (who, its
     * sync generation, when last seen), PENDING (a file version it has not confirmed) and PENDINGDEL (a delete it has
     * not applied). A subscriber keeps MIRRORED: which producer delivered which content under which name, so it never
     * overwrites or deletes content that is not that producer's. Paths are keyed by their SHA-256, so index keys stay
     * small whatever a path's length.
     */
    private static final String[][] SYNC_TABLES = {
            {"SUBSCRIBERS", "CREATE TABLE subscribers (subkey varchar(256) primary key NOT NULL, region varchar(128),"
                    + " agent varchar(128), plugin varchar(128), gen varchar(128), last_seen bigint)"},
            {"PENDING", "CREATE TABLE pending (subkey varchar(256) NOT NULL, pathhash varchar(200) NOT NULL,"
                    + " filepath varchar(1000) NOT NULL, PRIMARY KEY (subkey, pathhash))"},
            {"PENDINGDEL", "CREATE TABLE pendingdel (subkey varchar(256) NOT NULL, pathhash varchar(200) NOT NULL,"
                    + " filepath varchar(1000) NOT NULL, PRIMARY KEY (subkey, pathhash))"},
            {"MIRRORED", "CREATE TABLE mirrored (producer varchar(256) NOT NULL, relhash char(64) NOT NULL,"
                    + " relpath varchar(1000) NOT NULL, md5 varchar(255), PRIMARY KEY (producer, relhash))"},
    };

    /**
     * The key of a path in the pending tables: its first 120 characters, a separator below every printable character,
     * and its SHA-256. Unique, small enough for an index whatever the path's length, and ordered as the paths are (for
     * paths that differ in their first 120 characters), so pages of what is pending come in path order.
     */
    static String pathKey(String path) {
        return (path.length() > 120 ? path.substring(0, 120) : path) + "\u0001" + sha256(path);
    }

    /** SHA-256 of a path (UTF-8), lowercase hex: the key of a name in the mirrored table. */
    static String sha256(String path) {
        try {
            return Hashes.hex(java.security.MessageDigest.getInstance("SHA-256")
                    .digest(path.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** A subscriber as a producer remembers it. */
    static final class Subscriber {
        final String key, region, agent, plugin, gen;
        final long lastSeen;

        Subscriber(String key, String region, String agent, String plugin, String gen, long lastSeen) {
            this.key = key;
            this.region = region;
            this.agent = agent;
            this.plugin = plugin;
            this.gen = gen;
            this.lastSeen = lastSeen;
        }
    }

    public List<Subscriber> getSubscribers() {
        List<Subscriber> out = new ArrayList<>();
        try (Connection conn = ds.getConnection();
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT subkey, region, agent, plugin, gen, last_seen FROM subscribers")) {
            while (rs.next()) {
                out.add(new Subscriber(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                        rs.getString(5), rs.getLong(6)));
            }
        } catch (Exception ex) {
            logger.error("getSubscribers error", ex);
        }
        return out;
    }

    public Subscriber getSubscriber(String key) {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT subkey, region, agent, plugin, gen, last_seen FROM subscribers WHERE subkey = ?")) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new Subscriber(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                            rs.getString(5), rs.getLong(6));
                }
            }
        } catch (Exception ex) {
            logger.error("getSubscriber error", ex);
        }
        return null;
    }

    public void touchSubscriber(String key, long now) {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement("UPDATE subscribers SET last_seen = ? WHERE subkey = ?")) {
            ps.setLong(1, now);
            ps.setString(2, key);
            ps.executeUpdate();
        } catch (Exception ex) {
            logger.error("touchSubscriber error", ex);
        }
    }

    /**
     * Record a subscriber with a new generation: forget what it had, and mark every cataloged file and every tombstone
     * pending for it (it will be offered everything, paced, and skip what it already holds). Returns true when done.
     */
    public boolean resetSubscriber(Subscriber s, int batchSize) {
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try {
                for (String sql : new String[]{"DELETE FROM pending WHERE subkey = ?", "DELETE FROM pendingdel WHERE subkey = ?",
                        "DELETE FROM subscribers WHERE subkey = ?"}) {
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setString(1, s.key);
                        ps.executeUpdate();
                    }
                }
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO subscribers (subkey, region, agent, plugin, gen, last_seen) VALUES (?,?,?,?,?,?)")) {
                    ps.setString(1, s.key);
                    ps.setString(2, s.region);
                    ps.setString(3, s.agent);
                    ps.setString(4, s.plugin);
                    ps.setString(5, s.gen);
                    ps.setLong(6, s.lastSeen);
                    ps.executeUpdate();
                }
                fillPending(conn, s.key, "SELECT filepath FROM filelist", "INSERT INTO pending (subkey, pathhash, filepath) VALUES (?,?,?)", batchSize);
                fillPending(conn, s.key, "SELECT filepath FROM tombstone", "INSERT INTO pendingdel (subkey, pathhash, filepath) VALUES (?,?,?)", batchSize);
                conn.commit();
                return true;
            } catch (SQLException ex) {
                try { conn.rollback(); } catch (SQLException ignore) { }
                logger.error("resetSubscriber failed: " + ex.getMessage());
                return false;
            } finally {
                try { conn.setAutoCommit(true); } catch (SQLException ignore) { }
            }
        } catch (Exception ex) {
            logger.error("resetSubscriber error", ex);
            return false;
        }
    }

    private static void fillPending(Connection conn, String key, String select, String insert, int batchSize) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(select);
             PreparedStatement ins = conn.prepareStatement(insert)) {
            int n = 0;
            while (rs.next()) {
                String path = rs.getString(1);
                ins.setString(1, key);
                ins.setString(2, pathKey(path));
                ins.setString(3, path);
                ins.addBatch();
                if (++n % Math.max(1, batchSize) == 0) ins.executeBatch();
            }
            ins.executeBatch();
        }
    }

    /** Forget a subscriber and everything it had pending. */
    public void deleteSubscriber(String key) {
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try {
                for (String sql : new String[]{"DELETE FROM pending WHERE subkey = ?", "DELETE FROM pendingdel WHERE subkey = ?",
                        "DELETE FROM subscribers WHERE subkey = ?"}) {
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setString(1, key);
                        ps.executeUpdate();
                    }
                }
                conn.commit();
            } catch (SQLException ex) {
                try { conn.rollback(); } catch (SQLException ignore) { }
                logger.error("deleteSubscriber failed: " + ex.getMessage());
            } finally {
                try { conn.setAutoCommit(true); } catch (SQLException ignore) { }
            }
        } catch (Exception ex) {
            logger.error("deleteSubscriber error", ex);
        }
    }

    /**
     * New or changed files: pending for every known subscriber (a changed file is offered again to everyone), and no
     * longer a pending delete. One transaction per {@code batchSize} paths.
     */
    public void markChanged(List<String> paths, int batchSize) {
        markPending(paths, batchSize, "pending", "pendingdel");
    }

    /** Deleted files: a pending delete for every known subscriber, and no longer a pending file. */
    public void markDeleted(List<String> paths, int batchSize) {
        markPending(paths, batchSize, "pendingdel", "pending");
    }

    private void markPending(List<String> paths, int batchSize, String into, String outOf) {
        if (paths.isEmpty()) return;
        List<String> subs = new ArrayList<>();
        for (Subscriber s : getSubscribers()) subs.add(s.key);
        int size = Math.max(1, batchSize);
        for (int from = 0; from < paths.size(); from += size) {
            List<String> chunk = paths.subList(from, Math.min(paths.size(), from + size));
            try (Connection conn = ds.getConnection()) {
                conn.setAutoCommit(false);
                try (PreparedStatement drop = conn.prepareStatement("DELETE FROM " + outOf + " WHERE pathhash = ?");
                     PreparedStatement clear = conn.prepareStatement("DELETE FROM " + into + " WHERE subkey = ? AND pathhash = ?");
                     PreparedStatement add = conn.prepareStatement("INSERT INTO " + into + " (subkey, pathhash, filepath) VALUES (?,?,?)")) {
                    for (String path : chunk) {
                        String h = pathKey(path);
                        drop.setString(1, h);
                        drop.addBatch();
                        for (String sub : subs) {
                            clear.setString(1, sub);
                            clear.setString(2, h);
                            clear.addBatch();
                            add.setString(1, sub);
                            add.setString(2, h);
                            add.setString(3, path);
                            add.addBatch();
                        }
                    }
                    drop.executeBatch();
                    clear.executeBatch();
                    add.executeBatch();
                    conn.commit();
                } catch (SQLException ex) {
                    try { conn.rollback(); } catch (SQLException ignore) { }
                    logger.error("could not record " + into + " for " + chunk.size() + " path(s): " + ex.getMessage());
                } finally {
                    try { conn.setAutoCommit(true); } catch (SQLException ignore) { }
                }
            } catch (Exception ex) {
                logger.error("markPending error", ex);
            }
        }
    }

    /** A pending file for a subscriber: its key and the catalog row (path, md5, lastmodified, filesize). */
    public List<String[]> pendingFiles(String subkey, String afterKey, int limit) {
        List<String[]> out = new ArrayList<>();
        String sql = "SELECT p.pathhash, f.filepath, f.md5, f.lastmodified, f.filesize FROM pending p"
                + " JOIN filelist f ON f.filepath = p.filepath WHERE p.subkey = ? AND p.pathhash > ?"
                + " ORDER BY p.pathhash FETCH FIRST ? ROWS ONLY";
        try (Connection conn = ds.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, subkey);
            ps.setString(2, afterKey);
            ps.setInt(3, Math.max(1, limit));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(new String[]{rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5)});
            }
        } catch (Exception ex) {
            logger.error("pendingFiles error", ex);
        }
        return out;
    }

    /** A pending delete for a subscriber: its key and the tombstone (path, name, md5, deleted_at). */
    public List<String[]> pendingDeletes(String subkey, String afterKey, int limit) {
        List<String[]> out = new ArrayList<>();
        String sql = "SELECT d.pathhash, t.filepath, t.filename, t.md5, t.deleted_at FROM pendingdel d"
                + " JOIN tombstone t ON t.filepath = d.filepath WHERE d.subkey = ? AND d.pathhash > ?"
                + " ORDER BY d.pathhash FETCH FIRST ? ROWS ONLY";
        try (Connection conn = ds.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, subkey);
            ps.setString(2, afterKey);
            ps.setInt(3, Math.max(1, limit));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(new String[]{rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                        String.valueOf(rs.getLong(5))});
            }
        } catch (Exception ex) {
            logger.error("pendingDeletes error", ex);
        }
        return out;
    }

    /** Drop a pending row that can never be offered (outside the scan dir, in a live root). */
    public void dropPending(String subkey, String pathHash) {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement("DELETE FROM pending WHERE subkey = ? AND pathhash = ?")) {
            ps.setString(1, subkey);
            ps.setString(2, pathHash);
            ps.executeUpdate();
        } catch (Exception ex) {
            logger.error("dropPending error", ex);
        }
    }

    /** A subscriber holds these file versions: no longer pending for it (only where the catalog still has that version). */
    public int settleFiles(String subkey, List<Row> held) {
        return settle(held.size(), "DELETE FROM pending WHERE subkey = ? AND pathhash = ?"
                + " AND EXISTS (SELECT 1 FROM filelist f WHERE f.filepath = ? AND f.md5 = ?)", (ps, i) -> {
            Row r = held.get(i);
            ps.setString(1, subkey);
            ps.setString(2, pathKey(r.filepath));
            ps.setString(3, r.filepath);
            ps.setString(4, r.md5);
        });
    }

    /** A subscriber applied these deletes: no longer pending for it (only the same tombstone). */
    public int settleDeletes(String subkey, List<Tombstone> applied) {
        return settle(applied.size(), "DELETE FROM pendingdel WHERE subkey = ? AND pathhash = ?"
                + " AND EXISTS (SELECT 1 FROM tombstone t WHERE t.filepath = ? AND t.deleted_at = ?)", (ps, i) -> {
            Tombstone t = applied.get(i);
            ps.setString(1, subkey);
            ps.setString(2, pathKey(t.filepath));
            ps.setString(3, t.filepath);
            ps.setLong(4, t.deletedAt);
        });
    }

    private interface Binder {
        void bind(PreparedStatement ps, int i) throws SQLException;
    }

    private int settle(int count, String sql, Binder binder) {
        if (count == 0) return 0;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (int i = 0; i < count; i++) {
                    binder.bind(ps, i);
                    ps.addBatch();
                }
                int n = 0;
                for (int c : ps.executeBatch()) n += Math.max(0, c);
                conn.commit();
                return n;
            } catch (SQLException ex) {
                try { conn.rollback(); } catch (SQLException ignore) { }
                logger.error("settle failed: " + ex.getMessage());
                return 0;
            } finally {
                try { conn.setAutoCommit(true); } catch (SQLException ignore) { }
            }
        } catch (Exception ex) {
            logger.error("settle error", ex);
            return 0;
        }
    }

    /** Files and deletes a subscriber still has pending (tests, metrics). */
    public long pendingCount(String subkey) {
        long n = 0;
        for (String table : new String[]{"pending", "pendingdel"}) {
            try (Connection conn = ds.getConnection();
                 PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM " + table + " WHERE subkey = ?")) {
                ps.setString(1, subkey);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) n += rs.getLong(1);
                }
            } catch (Exception ex) {
                logger.error("pendingCount error", ex);
            }
        }
        return n;
    }

    // ---- subscriber side: which producer delivered which content under which name ----

    /** The content producer {@code producer} delivered under {@code relpath}, or null. */
    public String mirroredMd5(String producer, String relpath) {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT md5 FROM mirrored WHERE producer = ? AND relhash = ?")) {
            ps.setString(1, producer);
            ps.setString(2, sha256(relpath));
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        } catch (Exception ex) {
            logger.error("mirroredMd5 error", ex);
            return null;
        }
    }

    /** Every producer that delivered content under {@code relpath}, with that content: producer -> md5. */
    public Map<String, String> mirroredBy(String relpath) {
        Map<String, String> out = new HashMap<>();
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT producer, md5 FROM mirrored WHERE relhash = ?")) {
            ps.setString(1, sha256(relpath));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.put(rs.getString(1), rs.getString(2));
            }
        } catch (Exception ex) {
            logger.error("mirroredBy error", ex);
        }
        return out;
    }

    public void recordMirrored(String producer, String relpath, String md5) {
        try (Connection conn = ds.getConnection()) {
            try (PreparedStatement up = conn.prepareStatement("UPDATE mirrored SET md5 = ?, relpath = ? WHERE producer = ? AND relhash = ?")) {
                up.setString(1, md5);
                up.setString(2, relpath);
                up.setString(3, producer);
                up.setString(4, sha256(relpath));
                if (up.executeUpdate() > 0) return;
            }
            try (PreparedStatement in = conn.prepareStatement("INSERT INTO mirrored (producer, relhash, relpath, md5) VALUES (?,?,?,?)")) {
                in.setString(1, producer);
                in.setString(2, sha256(relpath));
                in.setString(3, relpath);
                in.setString(4, md5);
                in.executeUpdate();
            }
        } catch (Exception ex) {
            logger.error("recordMirrored error", ex);
        }
    }

    public void forgetMirrored(String producer, String relpath) {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement("DELETE FROM mirrored WHERE producer = ? AND relhash = ?")) {
            ps.setString(1, producer);
            ps.setString(2, sha256(relpath));
            ps.executeUpdate();
        } catch (Exception ex) {
            logger.error("forgetMirrored error", ex);
        }
    }

    /** Forget every mirrored record (a clear removed the files). */
    public void clearMirrored() {
        try (Connection conn = ds.getConnection(); Statement st = conn.createStatement()) {
            st.executeUpdate("DELETE FROM mirrored");
        } catch (Exception ex) {
            logger.error("clearMirrored error", ex);
        }
    }

    /** A catalog row as the scanner writes it. */
    static final class Row {
        final String filepath;
        final String md5;
        final long lastmodified;
        final long filesize;

        Row(String filepath, String md5, long lastmodified, long filesize) {
            this.filepath = filepath;
            this.md5 = md5;
            this.lastmodified = lastmodified;
            this.filesize = filesize;
        }
    }

    /** A deleted file: its catalog path, the name a subscriber holds it under, and the content it had. */
    static final class Tombstone {
        final String filepath;
        final String filename;
        final String md5;
        final long deletedAt;

        Tombstone(String filepath, String filename, String md5, long deletedAt) {
            this.filepath = filepath;
            this.filename = filename;
            this.md5 = md5;
            this.deletedAt = deletedAt;
        }
    }

    /**
     * Write a scan's new and changed rows (FR-D8): batched statements, one commit per {@code batchSize} rows, instead
     * of one forced log write per file. A new or changed file is no longer deleted, so its tombstone (if any) goes in
     * the same transaction. A batch that fails is rolled back and written row by row, so one bad row costs only itself.
     * Returns the number of rows written.
     */
    public int applyScan(List<Row> adds, List<Row> updates, int batchSize) {
        int written = 0;
        int size = Math.max(1, batchSize);
        for (int from = 0; from < adds.size(); from += size) {
            written += writeChunk(adds.subList(from, Math.min(adds.size(), from + size)), true);
        }
        for (int from = 0; from < updates.size(); from += size) {
            written += writeChunk(updates.subList(from, Math.min(updates.size(), from + size)), false);
        }
        return written;
    }

    private int writeChunk(List<Row> rows, boolean insert) {
        if (rows.isEmpty()) return 0;
        String upsert = insert
                ? "insert into filelist (filepath, md5, insync, lastmodified, filesize) values (?,?,?,?,?)"
                : "UPDATE filelist SET md5=?, insync=?, filesize=?, lastmodified=? WHERE filepath=?";
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(upsert);
                 PreparedStatement untomb = conn.prepareStatement("DELETE FROM tombstone WHERE filepath=?")) {
                for (Row r : rows) {
                    bindRow(ps, r, insert);
                    ps.addBatch();
                    untomb.setString(1, r.filepath);
                    untomb.addBatch();
                }
                ps.executeBatch();
                untomb.executeBatch();
                conn.commit();
                batchCommits.incrementAndGet();
                return rows.size();
            } catch (SQLException batchFailed) {
                try { conn.rollback(); } catch (SQLException ignore) { }
                logger.warn("catalog batch of " + rows.size() + " failed (" + batchFailed.getMessage() + "); writing row by row");
            } finally {
                try { conn.setAutoCommit(true); } catch (SQLException ignore) { }
            }
        } catch (Exception ex) {
            logger.error("catalog batch error", ex);
        }
        int written = 0;
        for (Row r : rows) {
            if (insert) {
                if (addFileRow(r)) written++;
            } else if (updateFile(r.filepath, r.md5, 0, r.lastmodified, r.filesize) > 0) {
                deleteTombstone(r.filepath);
                written++;
            }
        }
        return written;
    }

    private static void bindRow(PreparedStatement ps, Row r, boolean insert) throws SQLException {
        if (insert) {
            ps.setString(1, r.filepath);
            ps.setString(2, r.md5);
            ps.setInt(3, 0);
            ps.setString(4, String.valueOf(r.lastmodified));
            ps.setString(5, String.valueOf(r.filesize));
        } else {
            ps.setString(1, r.md5);
            ps.setInt(2, 0);
            ps.setString(3, String.valueOf(r.filesize));
            ps.setString(4, String.valueOf(r.lastmodified));
            ps.setString(5, r.filepath);
        }
    }

    private boolean addFileRow(Row r) {
        String sql = "insert into filelist (filepath, md5, insync, lastmodified, filesize) values (?,?,?,?,?)";
        try (Connection conn = ds.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            bindRow(ps, r, true);
            ps.executeUpdate();
            deleteTombstone(r.filepath);
            return true;
        } catch (Exception ex) {
            logger.error("addFile error for " + r.filepath, ex);
            return false;
        }
    }

    /**
     * Record deleted files (FR-D4): each catalog row goes and a tombstone takes its place (insync=0, to be offered), in
     * one transaction per {@code batchSize}. Returns the number recorded.
     */
    public int tombstone(List<Tombstone> deleted, int batchSize) {
        int done = 0;
        int size = Math.max(1, batchSize);
        for (int from = 0; from < deleted.size(); from += size) {
            List<Tombstone> chunk = deleted.subList(from, Math.min(deleted.size(), from + size));
            try (Connection conn = ds.getConnection()) {
                conn.setAutoCommit(false);
                try (PreparedStatement drop = conn.prepareStatement("DELETE FROM filelist WHERE filepath=?");
                     PreparedStatement clear = conn.prepareStatement("DELETE FROM tombstone WHERE filepath=?");
                     PreparedStatement add = conn.prepareStatement(
                             "INSERT INTO tombstone (filepath, filename, md5, deleted_at, insync) VALUES (?,?,?,?,0)")) {
                    for (Tombstone t : chunk) {
                        drop.setString(1, t.filepath);
                        drop.addBatch();
                        clear.setString(1, t.filepath);
                        clear.addBatch();
                        add.setString(1, t.filepath);
                        add.setString(2, t.filename);
                        add.setString(3, t.md5);
                        add.setLong(4, t.deletedAt);
                        add.addBatch();
                    }
                    drop.executeBatch();
                    clear.executeBatch();
                    add.executeBatch();
                    conn.commit();
                    batchCommits.incrementAndGet();
                    done += chunk.size();
                } catch (SQLException ex) {
                    try { conn.rollback(); } catch (SQLException ignore) { }
                    logger.error("tombstone batch failed: " + ex.getMessage());
                } finally {
                    try { conn.setAutoCommit(true); } catch (SQLException ignore) { }
                }
            } catch (Exception ex) {
                logger.error("tombstone error", ex);
            }
        }
        return done;
    }

    /** Tombstones no subscriber has confirmed (insync=0), oldest first, at most {@code limit}. */
    public List<Tombstone> getTombstonesNotInSync(int limit) {
        List<Tombstone> out = new ArrayList<>();
        String sql = "SELECT filepath, filename, md5, deleted_at FROM tombstone WHERE insync = 0 ORDER BY deleted_at"
                + " FETCH FIRST ? ROWS ONLY";
        try (Connection conn = ds.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, Math.max(1, limit));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new Tombstone(rs.getString(1), rs.getString(2), rs.getString(3), rs.getLong(4)));
                }
            }
        } catch (Exception ex) {
            logger.error("getTombstonesNotInSync error", ex);
        }
        return out;
    }

    /** Count of tombstones (both offered and confirmed). */
    public long getTombstoneCount() {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM tombstone")) {
            return rs.next() ? rs.getLong(1) : 0L;
        } catch (Exception ex) {
            logger.error("getTombstoneCount error", ex);
            return 0L;
        }
    }

    /** A subscriber applied these deletes: mark them insync=1 (only a tombstone for that same content). */
    public int confirmTombstones(List<Tombstone> confirmed) {
        if (confirmed.isEmpty()) return 0;
        String sql = "UPDATE tombstone SET insync = 1 WHERE filepath = ? AND deleted_at = ?";
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (Tombstone t : confirmed) {
                    ps.setString(1, t.filepath);
                    ps.setLong(2, t.deletedAt);
                    ps.addBatch();
                }
                int n = 0;
                for (int c : ps.executeBatch()) n += Math.max(0, c);
                conn.commit();
                return n;
            } catch (SQLException ex) {
                try { conn.rollback(); } catch (SQLException ignore) { }
                logger.error("confirmTombstones failed: " + ex.getMessage());
                return 0;
            } finally {
                try { conn.setAutoCommit(true); } catch (SQLException ignore) { }
            }
        } catch (Exception ex) {
            logger.error("confirmTombstones error", ex);
            return 0;
        }
    }

    /** Forget confirmed tombstones deleted before {@code before} (epoch ms). Returns how many went. */
    public int purgeTombstones(long before) {
        // never while a subscriber still has the delete pending
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement("DELETE FROM tombstone WHERE insync = 1 AND deleted_at < ?"
                     + " AND NOT EXISTS (SELECT 1 FROM pendingdel d WHERE d.filepath = tombstone.filepath)")) {
            ps.setLong(1, before);
            return ps.executeUpdate();
        } catch (Exception ex) {
            logger.error("purgeTombstones error", ex);
            return 0;
        }
    }

    public int deleteTombstone(String filepath) {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement("DELETE FROM tombstone WHERE filepath = ?")) {
            ps.setString(1, filepath);
            return ps.executeUpdate();
        } catch (Exception ex) {
            logger.error("deleteTombstone error for " + filepath, ex);
            return -1;
        }
    }

    /**
     * A subscriber confirmed it holds these files: insync=1, but only where the catalog still holds the content that
     * was offered. A file changed since the offer keeps insync=0 and is offered again; the old code wrote the offered
     * md5 and mtime back over the newer ones.
     */
    public int confirmFiles(List<Row> confirmed) {
        if (confirmed.isEmpty()) return 0;
        String sql = "UPDATE filelist SET insync = 1 WHERE filepath = ? AND md5 = ?";
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (Row r : confirmed) {
                    ps.setString(1, r.filepath);
                    ps.setString(2, r.md5);
                    ps.addBatch();
                }
                int n = 0;
                for (int c : ps.executeBatch()) n += Math.max(0, c);
                conn.commit();
                return n;
            } catch (SQLException ex) {
                try { conn.rollback(); } catch (SQLException ignore) { }
                logger.error("confirmFiles failed: " + ex.getMessage());
                return 0;
            } finally {
                try { conn.setAutoCommit(true); } catch (SQLException ignore) { }
            }
        } catch (Exception ex) {
            logger.error("confirmFiles error", ex);
            return 0;
        }
    }

    /** A page of the rows some subscriber confirmed (insync=1), by path: what a full offer to a new subscriber sends. */
    public List<Map<String,String>> getSyncedRows(int limit, int offset) {
        List<Map<String,String>> out = new ArrayList<>();
        String sql = "SELECT filepath, md5, lastmodified, filesize FROM filelist WHERE insync = 1 ORDER BY filepath " +
                "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, Math.max(0, offset));
            ps.setInt(2, Math.max(1, limit));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String,String> row = new HashMap<>();
                    row.put("filepath", rs.getString(1));
                    row.put("md5", rs.getString(2));
                    row.put("lastmodified", rs.getString(3));
                    row.put("filesize", rs.getString(4));
                    out.add(row);
                }
            }
        } catch (Exception ex) {
            logger.error("getSyncedRows error", ex);
        }
        return out;
    }

    /** Offer these files again (insync=0): a subscriber failed them in a full offer. */
    public int markUnsynced(java.util.Collection<String> paths) {
        if (paths.isEmpty()) return 0;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement("UPDATE filelist SET insync = 0 WHERE filepath = ?")) {
                for (String p : paths) {
                    ps.setString(1, p);
                    ps.addBatch();
                }
                int n = 0;
                for (int c : ps.executeBatch()) n += Math.max(0, c);
                conn.commit();
                return n;
            } catch (SQLException ex) {
                try { conn.rollback(); } catch (SQLException ignore) { }
                logger.error("markUnsynced failed: " + ex.getMessage());
                return 0;
            } finally {
                try { conn.setAutoCommit(true); } catch (SQLException ignore) { }
            }
        } catch (Exception ex) {
            logger.error("markUnsynced error", ex);
            return 0;
        }
    }

    /** Every tombstone, for tests and diagnostics. */
    List<Tombstone> getTombstones() {
        List<Tombstone> out = new ArrayList<>();
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT filepath, filename, md5, deleted_at, insync FROM tombstone")) {
            while (rs.next()) {
                out.add(new Tombstone(rs.getString(1), rs.getString(2), rs.getString(3), rs.getLong(4)));
            }
        } catch (Exception ex) {
            logger.error("getTombstones error", ex);
        }
        return out;
    }

    /**
     * REPOMETA holds a few named facts about the repository itself (that something landed in it, the live-tier roots
     * this agent holds), kept in the catalog rather than as marker files in the tree, where a scanner would catalog
     * them and a peer would inherit them.
     */
    private void initMeta() {
        String createRepoMeta = "CREATE TABLE repometa" +
                "(" +
                "   name varchar(128) primary key NOT NULL," +
                "   val varchar(32672)" +
                ")";
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(createRepoMeta);
        } catch(Exception ex) {
            logger.error("initMeta error", ex);
        }
    }

    /** A named repository fact, or null when it is not recorded. Throws when the catalog cannot be read. */
    public String getMeta(String name) throws SQLException {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT val FROM repometa WHERE name = ?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    /** Record a named repository fact (insert or replace). Throws when the catalog cannot be written. */
    public void setMeta(String name, String val) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            try (PreparedStatement up = conn.prepareStatement("UPDATE repometa SET val = ? WHERE name = ?")) {
                up.setString(1, val);
                up.setString(2, name);
                if (up.executeUpdate() > 0) return;
            }
            try (PreparedStatement in = conn.prepareStatement("INSERT INTO repometa (name, val) VALUES (?,?)")) {
                in.setString(1, name);
                in.setString(2, val);
                in.executeUpdate();
            }
        }
    }

    /** Forget a named repository fact. Throws when the catalog cannot be written. */
    public void deleteMeta(String name) throws SQLException {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement("DELETE FROM repometa WHERE name = ?")) {
            ps.setString(1, name);
            ps.executeUpdate();
        }
    }

    /**
     * True if the catalog holds any row: one index probe, whatever the catalog's size. Fails closed: a catalog that
     * cannot be read answers true, so nothing is treated as empty that might not be.
     */
    public boolean hasFiles() {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1 FROM filelist FETCH FIRST 1 ROWS ONLY")) {
            return rs.next();
        } catch (Exception ex) {
            logger.error("hasFiles error", ex);
            return true;
        }
    }

    /**
     * True only if the catalog is actually queryable. Unlike getRepoCount() (which swallows and
     * returns 0), this lets the health check tell a dead DB apart from an empty one.
     */
    public boolean isCatalogHealthy() {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM filelist")) {
            return rs.next();
        } catch (Exception ex) {
            return false;
        }
    }

    public void initDB() {

        String createFileList = "CREATE TABLE filelist" +
                "(" +
                "   filepath varchar(1000) primary key NOT NULL," +
                "   md5 varchar(255)," +
                "   insync int," +
                "   lastmodified varchar(255)," +
                "   filesize varchar(255)" +
                ")";

        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(createFileList);
        } catch(Exception ex) {
            logger.error("initDB error", ex);
        }
    }

    // All mutating/lookup statements are parameterized (PreparedStatement): file paths and MD5s
    // are external input and were previously concatenated into SQL — a correctness bug (any path
    // containing a quote broke the query) and an injection vector. Parameter binding also lets
    // Derby cache the query plan across the per-file scan loop.

    public void addFile(String filepath, String md5, long lastmodified, long filesize) {
        addFileRow(new Row(filepath, md5, lastmodified, filesize));
    }

    /**
     * Insert or replace one row (a landing that overwrote a cataloged file). insync=0: the new content is offered.
     */
    public void upsertFile(String filepath, String md5, long lastmodified, long filesize) {
        if (updateFile(filepath, md5, 0, lastmodified, filesize) > 0) {
            deleteTombstone(filepath);
        } else {
            addFile(filepath, md5, lastmodified, filesize);
        }
    }

    public List<Map<String,String>> getRepoList() {
        List<Map<String,String>> repoFileList = new ArrayList<>();
        String sql = "SELECT filepath, md5, lastmodified, filesize FROM filelist";
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                Map<String,String> fileMap = new HashMap<>();
                fileMap.put("filepath",rs.getString("filepath"));
                fileMap.put("md5",rs.getString("md5"));
                fileMap.put("lastmodified",rs.getString("lastmodified"));
                fileMap.put("filesize",rs.getString("filesize"));
                repoFileList.add(fileMap);
            }
        } catch(Exception ex) {
            logger.error("getRepoList error", ex);
        }
        return repoFileList;
    }

    /** Catalog rows never confirmed downloaded by a subscriber (insync=0); drives re-offer. */
    public List<Map<String,String>> getFilesNotInSync() {
        List<Map<String,String>> repoFileList = new ArrayList<>();
        String sql = "SELECT filepath, md5, lastmodified, filesize FROM filelist WHERE insync = 0";
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                Map<String,String> fileMap = new HashMap<>();
                fileMap.put("filepath",rs.getString("filepath"));
                fileMap.put("md5",rs.getString("md5"));
                fileMap.put("lastmodified",rs.getString("lastmodified"));
                fileMap.put("filesize",rs.getString("filesize"));
                repoFileList.add(fileMap);
            }
        } catch(Exception ex) {
            logger.error("getFilesNotInSync error", ex);
        }
        return repoFileList;
    }

    /** Paginated catalog read (Derby OFFSET/FETCH). limit <= 0 returns the full list. */
    public List<Map<String,String>> getRepoList(int limit, int offset) {
        if (limit <= 0) return getRepoList();
        List<Map<String,String>> repoFileList = new ArrayList<>();
        String sql = "SELECT filepath, md5, lastmodified, filesize FROM filelist ORDER BY filepath " +
                "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, Math.max(0, offset));
            ps.setInt(2, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String,String> fileMap = new HashMap<>();
                    fileMap.put("filepath",rs.getString("filepath"));
                    fileMap.put("md5",rs.getString("md5"));
                    fileMap.put("lastmodified",rs.getString("lastmodified"));
                    fileMap.put("filesize",rs.getString("filesize"));
                    repoFileList.add(fileMap);
                }
            }
        } catch(Exception ex) {
            logger.error("getRepoList(limit,offset) error", ex);
        }
        return repoFileList;
    }

    public long getRepoCount() {
        long count = 0;
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM filelist")) {
            if (rs.next()) count = rs.getLong(1);
        } catch(Exception ex) {
            logger.error("getRepoCount error", ex);
        }
        return count;
    }

    public Map<String,String> getFileInfo(String filePath) {
        Map<String,String> fileInfo = null;
        String sql = "SELECT filepath, md5, lastmodified, filesize FROM filelist WHERE filepath = ?";
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, filePath);
            try (ResultSet rs = ps.executeQuery()) {
                if(rs.next()) {
                    fileInfo = new HashMap<>();
                    fileInfo.put("filepath", rs.getString("filepath"));
                    fileInfo.put("md5", rs.getString("md5"));
                    fileInfo.put("lastmodified", rs.getString("lastmodified"));
                    fileInfo.put("filesize", rs.getString("filesize"));
                }
            }
        } catch(Exception ex) {
            logger.error("getFileInfo error for " + filePath, ex);
        }
        return fileInfo;
    }

    public long getLastModified(String filepath) {
        long lastModified = -1;
        String sql = "SELECT lastmodified FROM filelist WHERE filepath = ?";
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, filepath);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    lastModified = rs.getLong(1);
                }
            }
        } catch(Exception ex) {
            logger.error("getLastModified error for " + filepath, ex);
        }
        return lastModified;
    }

    public long getFileSize(String filepath) {
        long filesize = -1;
        String sql = "SELECT filesize FROM filelist WHERE filepath = ?";
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, filepath);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    filesize = rs.getLong(1);
                }
            }
        } catch(Exception ex) {
            logger.error("getFileSize error for " + filepath, ex);
        }
        return filesize;
    }

    public String getMD5(String filepath) {
        String md5 = null;
        String sql = "SELECT md5 FROM filelist WHERE filepath = ?";
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, filepath);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    md5 = rs.getString(1);
                }
            }
        } catch(Exception ex) {
            logger.error("getMD5 error for " + filepath, ex);
        }
        return md5;
    }

    public int updateFile(String filepath, String md5, int insync, long lastmodified, long filesize) {
        int queryReturn = -1;
        String sql = "UPDATE filelist SET md5=?, insync=?, filesize=?, lastmodified=? WHERE filepath=?";
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, md5);
            ps.setInt(2, insync);
            ps.setString(3, String.valueOf(filesize));
            ps.setString(4, String.valueOf(lastmodified));
            ps.setString(5, filepath);
            queryReturn = ps.executeUpdate();
        } catch(Exception ex) {
            logger.error("updateFile error for " + filepath, ex);
        }
        return queryReturn;
    }

    public int deleteFile(String filepath) {
        int queryReturn = -1;
        String sql = "DELETE FROM filelist WHERE filepath=?";
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, filepath);
            queryReturn = ps.executeUpdate();
        } catch(Exception ex) {
            logger.error("deleteFile error for " + filepath, ex);
        }
        return queryReturn;
    }

    ///

    public DataSource setupDataSource(String connectURI) {
        return setupDataSource(connectURI,null,null);
    }

    public DataSource setupDataSource(String connectURI, String login, String password) {
        ConnectionFactory connectionFactory;
        if((login == null) && (password == null)) {
            connectionFactory = new DriverManagerConnectionFactory(connectURI, null);
        } else {
            connectionFactory = new DriverManagerConnectionFactory(connectURI, login, password);
        }

        poolableConnectionFactory =
                new PoolableConnectionFactory(connectionFactory, null);

        connectionPool =
                new GenericObjectPool<>(poolableConnectionFactory);

        poolableConnectionFactory.setPool(connectionPool);

        dataSource =
                new PoolingDataSource<>(connectionPool);

        return dataSource;
    }

}
