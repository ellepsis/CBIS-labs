package com.ellepsis.CIBS;

import java.sql.*;
import javax.sql.DataSource;
import oracle.jdbc.pool.OracleDataSource;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.InitialContext;
import java.util.Hashtable;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import oracle.jdbc.pool.OracleConnectionPoolDataSource;

public class Lab5 {
    static Connection conn = null;
    static Connection conn2 = null;
    static Statement stmt = null;
    static ResultSet rs = null;
    static Context ctx = null;
    static DataSource ds = null;
    static ConnectionPoolDataSource cpds = null;
    static PooledConnection pc = null;
    static String user;
    static String password;

    public static void main (String args []){

        // Initialize the Context
        String sp = "com.sun.jndi.fscontext.RefFSContextFactory";
        String file = "file:JNDI";
        String dataSourceName  = "myDataSource";
        String connectionPoolName = "myConnectionPool";
        try {
            //Create Hashtable to hold environment properties
            //then open InitialContext
            Hashtable env = new Hashtable();
            env.put (Context.INITIAL_CONTEXT_FACTORY, sp);
            env.put (Context.PROVIDER_URL, file);
            ctx = new InitialContext(env);

            System.out.println("Please, input your username:");
            user = System.console().readLine();
            System.out.println("Please, input your password:");
            password = new String(System.console().readPassword());
            
            //Bind the DataSource object
            bindDataSource(ctx, dataSourceName);
            //Bind the ConnectionPoolDataSource object
            bindConnectionPoolDataSource(ctx, connectionPoolName);
            
            //Retrieve the DataSource object
            ds = (DataSource) ctx.lookup(dataSourceName);
            
            //Retrieve the ConnectionPoolDataSource object
            cpds = (ConnectionPoolDataSource) ctx.lookup(connectionPoolName);

            //Open a connection, submit query, and print results
            conn = ds.getConnection();
            stmt = conn.createStatement();
            String sql = "SELECT count(*) FROM v$session " +
                    "WHERE username = '" + user.toUpperCase() + "'";
            rs = stmt.executeQuery(sql);
            System.out.println("DataSource is used\n" +
                    "Count of connections for user " + user + " is: ");
            while(rs.next())
                System.out.println(rs.getInt(1));

            rs.close();
            stmt.close();
            
            pc = cpds.getPooledConnection();
            conn2 = pc.getConnection();
            stmt = conn2.createStatement();
            rs = stmt.executeQuery(sql);
            System.out.println("\nPooledConnection is used\n"
                    + "Count of connections for user " + user + " is:");
            while(rs.next())
                System.out.println(rs.getInt(1));

            // Close the connections to the data store resources
            ctx.close();
            rs.close();
            stmt.close();
            conn.close();
            conn2.close();
        }
        catch (NamingException ne){
            ne.printStackTrace();
        }
        catch (SQLException se){
            se.printStackTrace();
        //ensure all resources are closed
        }
        finally {
            try {
                pc.close();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            finally {
                try {
                    if(ctx!=null)
                        ctx.close();
                }
                catch (NamingException ne) {
                    ne.printStackTrace();
                }
                finally {
                    try {
                        if(conn!=null)
                            conn.close();
                    }
                    catch (SQLException se) {
                        se.printStackTrace();
                    }
                    finally {
                        try {
                            if(conn2!=null)
                                conn2.close();
                        }
                        catch (SQLException se) {
                            se.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    //Method to bind ConnectionPoolDataSource object

    public static void bindConnectionPoolDataSource(Context ctx, String cpdsn)
        throws SQLException, NamingException {

        //Create an OracleConnectionPoolDataSource instance
        OracleConnectionPoolDataSource ocpds = new OracleConnectionPoolDataSource();

        //Set the connection parameters
        ocpds.setUser(user);
        ocpds.setPassword(password);
        ocpds.setDriverType("thin");
        ocpds.setDatabaseName("orbis");
        ocpds.setServerName("localhost");
        ocpds.setPortNumber(1521);

        //Bind the ConnectionPoolDataSource
        ctx.rebind(cpdsn, ocpds);
    }
    
    //Method to bind DataSource object

    public static void bindDataSource(Context ctx, String dsn)
        throws SQLException, NamingException {

        //Create an OracleDataSource instance
        OracleDataSource ods = new OracleDataSource();

        //Set the connection parameters
        ods.setUser(user);
        ods.setPassword(password);
        ods.setDriverType("thin");
        ods.setDatabaseName("orbis");
        ods.setServerName("localhost");
        ods.setPortNumber(1521);

        //Bind the DataSource
        ctx.rebind(dsn, ods);
    }
    
}
