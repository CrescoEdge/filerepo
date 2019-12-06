package io.cresco.filerepo;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class DBEngine {

    private DataSource ds;
    private Gson gson;
    private Type type;
    private CLogger logger;

    private List<String> tablesNames;

    private PluginBuilder pluginBuilder;

    public DBEngine(PluginBuilder plugin) {

        try {
            logger = plugin.getLogger(DBEngine.class.getName(), CLogger.Level.Info);

            String dataPath = "cresco-data/plugin-data/" + plugin.getPluginID() + "/";
            System.setProperty("derby.system.home", new File(dataPath).getAbsolutePath());

            this.pluginBuilder = plugin;

            tablesNames = new ArrayList<>();
            tablesNames.add("filelist");


            this.gson = new Gson();
            this.type = new TypeToken<Map<String, List<Map<String, String>>>>() {
            }.getType();

            String defaultDBName = "filerepo-db";
            String dbName = plugin.getConfig().getStringParam("db_name", defaultDBName);

            String dbDriver = plugin.getConfig().getStringParam("db_driver", "org.apache.derby.jdbc.EmbeddedDriver");
            String dbConnectionString = plugin.getConfig().getStringParam("db_jdbc", "jdbc:derby:" + dbName + ";create=true");

            Class.forName(dbDriver);

            ds = setupDataSource(dbConnectionString);


                if (dbName.equals(defaultDBName)) {
                    File dbsource = Paths.get(dataPath + defaultDBName).toFile();
                    //File dbsource = new File(defaultDBName);
                    if (dbsource.exists()) {
                        logger.info("DB SOURCE EXIST");
                        //delete(dbsource);
                    } else {
                        //dbsource.mkdir();
                        logger.info("CREATING DB");
                        initDB();
                    }
                }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void initDB() {

//ControllerState.Mode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent

        String largeFieldType = "clob";


        String createFileList = "CREATE TABLE filelist" +
                "(" +
                "   filepath varchar(1000) primary key NOT NULL," +
                "   md5 varchar(255)," +
                "   insync int," +
                "   lastmodified varchar(255)" +
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

    public void addFile(String filepath, String md5, long lastmodified) {

        try {

            //Timestamp timestamp = new Timestamp(lastmodified);
            try (Connection conn = ds.getConnection()) {
                conn.setAutoCommit(false);

                try (Statement stmt = conn.createStatement()) {

                    String insertFilePathString = "insert into filelist (filepath, md5, insync, lastmodified) " +
                            "values ('" + filepath + "','" + md5 + "'," + 0 + ",'" + String.valueOf(lastmodified) + "')";

                    stmt.executeUpdate(insertFilePathString);
                    conn.commit();
                }

            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    public long getLastModified(String filepath) {
        long lastModified = -1;
        try {

            String queryString = "SELECT lastmodified FROM filelist WHERE filepath = '" + filepath +"'";

            logger.error("QUERY: " + queryString);

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

    public int updateFile(String filepath, String md5, int insync, long lastmodified) {
        int queryReturn = -1;
        try {

            String queryString = null;
            queryString = "UPDATE filelist SET filepath=" + filepath + ", md5='" + md5 + "', insync=" + insync + ", lastmodified='" + String.valueOf(lastmodified) + "'"
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

    ///


    public static DataSource setupDataSource(String connectURI) {
        return setupDataSource(connectURI,null,null);
    }

    public static DataSource setupDataSource(String connectURI, String login, String password) {
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
        PoolableConnectionFactory poolableConnectionFactory =
                new PoolableConnectionFactory(connectionFactory, null);



        //
        // Now we'll need a ObjectPool that serves as the
        // actual pool of connections.
        //
        // We'll use a GenericObjectPool instance, although
        // any ObjectPool implementation will suffice.
        //
        ObjectPool<PoolableConnection> connectionPool =
                new GenericObjectPool<>(poolableConnectionFactory);

        // Set the factory's pool property to the owning pool
        poolableConnectionFactory.setPool(connectionPool);



        //
        // Finally, we create the PoolingDriver itself,
        // passing in the object pool we created.
        //
        PoolingDataSource<PoolableConnection> dataSource =
                new PoolingDataSource<>(connectionPool);

        return dataSource;
    }


}