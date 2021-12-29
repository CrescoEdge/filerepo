package io.cresco.filerepo;

import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
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
            //String dbConnectionString = plugin.getConfig().getStringParam("db_jdbc", "jdbc:hsqldb:" + dbsource2.getAbsolutePath() + ";ifexists=false");

            Class.forName(dbDriver);
            //Class.forName("org.hsqldb.jdbc.JDBCDriver");

            ds = setupDataSource(dbConnectionString);

                    if (dbsource.exists()) {
                        //logger.info("DB SOURCE EXIST: " + dbsource.getAbsolutePath() );
                    } else {
                        //dbsource.mkdir();
                        //logger.info("CREATING DB DBSOURCE: " + dbsource.getAbsolutePath());
                        initDB();
                    }


        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public boolean shutdown() {
        boolean isShutdown = false;
        try {

            //shutdown database, catch acception
            try {
                //shutdown the database
                //String shutdownString =  "jdbc:derby:" + dbPath + ";shutdown=true";
                if(dbsource.exists()) {
                    String shutdownString = "jdbc:derby:" + dbsource.getAbsolutePath() + ";shutdown=true";
                    //String shutdownString = "jdbc:derby:;shutdown=true";
                    DriverManager.getConnection(shutdownString);

                    //shutdown connections
                    dataSource.close();
                    connectionPool.close();

                }
            } catch (SQLException e) {
                if (e.getErrorCode() == 50000) {
                /*
                XJ015 (with SQLCODE 50000) is the expected (successful)
                SQLSTATE for complete system shutdown. 08006 (with SQLCODE 45000), on the other hand, is the expected SQLSTATE for shutdown of only an individual database.
                 */
                    isShutdown = true;

                } else if (e.getErrorCode() == 45000) {
                    isShutdown = true;

                } else {
                    e.printStackTrace();
                }
            }
            //unload drivers
            //DriverManager.getConnection("jdbc:derby:;shutdown=true");
            //Driver d= new org.apache.derby.jdbc.EmbeddedDriver();
            //Driver d= new org.hsqldb.jdbc.JDBCDriver();
            //DriverManager.deregisterDriver(d);

            //Driver da= new org.apache.derby.jdbc.AutoloadedDriver();
            //DriverManager.deregisterDriver(da);

        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        return isShutdown;
    }


    public void initDB() {

//ControllerState.Mode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent

        String largeFieldType = "clob";


        String createFileList = "CREATE TABLE filelist" +
                "(" +
                "   filepath varchar(1000) primary key NOT NULL," +
                "   md5 varchar(255)," +
                "   insync int," +
                "   lastmodified varchar(255)," +
                "   filesize varchar(255)" +
                ")";


        try {
            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(createFileList);
                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    public void addFile(String filepath, String md5, long lastmodified, long filesize) {

        try {

            //Timestamp timestamp = new Timestamp(lastmodified);
            try (Connection conn = ds.getConnection()) {
                conn.setAutoCommit(false);

                try (Statement stmt = conn.createStatement()) {

                    String insertFilePathString = "insert into filelist (filepath, md5, insync, lastmodified, filesize) " +
                            "values ('" + filepath + "','" + md5 + "'," + 0 + ",'" + String.valueOf(lastmodified) + "','" + String.valueOf(filesize) +"')";

                    stmt.executeUpdate(insertFilePathString);
                    conn.commit();
                }

            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    public List<Map<String,String>> getRepoList() {
        List<Map<String,String>> repoFileList = null;
        try {

            repoFileList = new ArrayList<>();

            String queryString = "SELECT filepath, md5, lastmodified, filesize FROM filelist";

            //logger.error("QUERY: " + queryString);

            try (Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    try(ResultSet rs = stmt.executeQuery(queryString)) {

                        while (rs.next()) {
                            Map<String,String> fileMap = new HashMap<>();
                            fileMap.put("filepath",rs.getString("filepath"));
                            fileMap.put("md5",rs.getString("md5"));
                            fileMap.put("lastmodified",rs.getString("lastmodified"));
                            fileMap.put("filesize",rs.getString("filesize"));
                            repoFileList.add(fileMap);
                        }

                    }
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
            System.out.println(ex.getMessage());
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            System.out.println(errors.toString());
        }

        return repoFileList;
    }

    public long getLastModified(String filepath) {
        long lastModified = -1;
        try {

            String queryString = "SELECT lastmodified FROM filelist WHERE filepath = '" + filepath +"'";

            //logger.error("QUERY: " + queryString);

            try (Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    try(ResultSet rs = stmt.executeQuery(queryString)) {

                        if (rs.next()) {
                            lastModified = rs.getLong(1);
                        }

                    }
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
            System.out.println(ex.getMessage());
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            System.out.println(errors.toString());
        }

        return lastModified;
    }

    public long getFileSize(String filepath) {
        long filesize = -1;
        try {

            String queryString = "SELECT filesize FROM filelist WHERE filepath = '" + filepath +"'";

            //logger.error("QUERY: " + queryString);

            try (Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    try(ResultSet rs = stmt.executeQuery(queryString)) {

                        if (rs.next()) {
                            filesize = rs.getLong(1);
                        }

                    }
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
            System.out.println(ex.getMessage());
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            System.out.println(errors.toString());
        }

        return filesize;
    }

    public String getMD5(String filepath) {
        String md5 = null;
        try {

            String queryString = "SELECT md5 FROM filelist WHERE filepath = '" + filepath +"'";

            //logger.error("QUERY: " + queryString);

            try (Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    try(ResultSet rs = stmt.executeQuery(queryString)) {

                        if (rs.next()) {
                            md5 = rs.getString(1);
                        }

                    }
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
            System.out.println(ex.getMessage());
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            System.out.println(errors.toString());
        }

        return md5;
    }

    public int updateFile(String filepath, String md5, int insync, long lastmodified, long filesize) {
        int queryReturn = -1;
        try {

            String queryString = null;
            queryString = "UPDATE filelist SET filepath='" + filepath + "', md5='" + md5 + "', insync=" + insync + ", filesize='" + String.valueOf(filesize) + "', lastmodified='" + String.valueOf(lastmodified) + "'"
                    + " WHERE filepath='" + filepath + "'";

            try (Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    queryReturn = stmt.executeUpdate(queryString);

                }
            }


        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return queryReturn;
    }

    public int deleteFile(String filepath) {
        int queryReturn = -1;
        try {

            String queryString = null;
            queryString = "DELETE FROM filelist WHERE filepath='" + filepath + "'";

            try (Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    queryReturn = stmt.executeUpdate(queryString);
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return queryReturn;
    }

    ///


    public DataSource setupDataSource(String connectURI) {
        return setupDataSource(connectURI,null,null);
    }

    public DataSource setupDataSource(String connectURI, String login, String password) {
        //
        // First, we'll create a ConnectionFactory that the
        // pool will use to create Connections.
        // We'll use the DriverManagerConnectionFactory,
        // using the connect string passed in the command line
        // arguments.
        //
        ConnectionFactory connectionFactory = null;
        if((login == null) && (password == null)) {
            connectionFactory = new DriverManagerConnectionFactory(connectURI, null);
        } else {
            connectionFactory = new DriverManagerConnectionFactory(connectURI,
                    login, password);
        }


        //
        // Next we'll create the PoolableConnectionFactory, which wraps
        // the "real" Connections created by the ConnectionFactory with
        // the classes that implement the pooling functionality.
        //
        poolableConnectionFactory =
                new PoolableConnectionFactory(connectionFactory, null);



        //
        // Now we'll need a ObjectPool that serves as the
        // actual pool of connections.
        //
        // We'll use a GenericObjectPool instance, although
        // any ObjectPool implementation will suffice.
        //
        connectionPool =
                new GenericObjectPool<>(poolableConnectionFactory);

        // Set the factory's pool property to the owning pool
        poolableConnectionFactory.setPool(connectionPool);



        //
        // Finally, we create the PoolingDriver itself,
        // passing in the object pool we created.
        //
        dataSource =
                new PoolingDataSource<>(connectionPool);

        return dataSource;
    }


}