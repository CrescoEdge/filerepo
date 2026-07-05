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
                initDB();
            }

        } catch (Exception ex) {
            logger.error("DBEngine init failed", ex);
        }
    }

    public boolean shutdown() {
        boolean isShutdown = false;
        try {
            try {
                if(dbsource.exists()) {
                    String shutdownString = "jdbc:derby:" + dbsource.getAbsolutePath() + ";shutdown=true";
                    DriverManager.getConnection(shutdownString);

                    dataSource.close();
                    connectionPool.close();
                }
            } catch (SQLException e) {
                // XJ015 (SQLCODE 50000) = full system shutdown; 08006 (45000) = single db shutdown. Both expected.
                if (e.getErrorCode() == 50000 || e.getErrorCode() == 45000) {
                    isShutdown = true;
                } else {
                    logger.error("DBEngine shutdown error", e);
                }
            }
        }
        catch (Exception ex) {
            logger.error("DBEngine shutdown error", ex);
        }
        return isShutdown;
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
        String sql = "insert into filelist (filepath, md5, insync, lastmodified, filesize) values (?,?,?,?,?)";
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, filepath);
                ps.setString(2, md5);
                ps.setInt(3, 0);
                ps.setString(4, String.valueOf(lastmodified));
                ps.setString(5, String.valueOf(filesize));
                ps.executeUpdate();
                conn.commit();
            }
        } catch(Exception ex) {
            logger.error("addFile error for " + filepath, ex);
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
