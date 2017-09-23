package dima;

import com.ellepsis.CIBS.*;
import java.io.UnsupportedEncodingException;
import java.sql.*;
import javax.sql.DataSource;
import oracle.jdbc.pool.OracleDataSource;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.InitialContext;
import java.util.Hashtable;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import oracle.jdbc.pool.OracleConnectionPoolDataSource;

public class S182506 {
    static Connection conn = null;
    static Statement stmt = null;
    static ResultSet rs = null;
    static Context ctx = null;
    static ConnectionPoolDataSource cpds = null;
    static PooledConnection pc = null;
    static String user;
    static String password;

    public static void main (String args []){
        executeLab7();
    }
    
    private static void executeLab7() {
        // Initialize the Context
        String sp = "com.sun.jndi.fscontext.RefFSContextFactory";
        String file = "file:JNDI";
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
            
            //Bind the ConnectionPoolDataSource object
            bindConnectionPoolDataSource(ctx, connectionPoolName);
            
            //Retrieve the ConnectionPoolDataSource object
            cpds = (ConnectionPoolDataSource) ctx.lookup(connectionPoolName);
            
            pc = cpds.getPooledConnection();
            conn = pc.getConnection();
            stmt = conn.createStatement();
            String sql = "SELECT √–”œœ¿, ‘¿Ã»À»ﬂ, »Ãﬂ, Œ“◊≈—“¬Œ, " +
                    "ƒ¿“¿_–Œ∆ƒ≈Õ»ﬂ, Ã≈—“Œ_–Œ∆ƒ≈Õ»ﬂ\n" +
                    "FROM Õ_Àﬁƒ» JOIN\n" +
                    "Õ_”◊≈Õ» »\n" +
                    "ON Õ_Àﬁƒ».»ƒ = Õ_”◊≈Õ» ».◊À¬ _»ƒ\n" +
                    "WHERE √–”œœ¿ = 4108;";
            rs = stmt.executeQuery(sql);
            System.out.println("\nPooledConnection is used\n"
                    + "Count of connections for user " + user + " is:");
            /*while(rs.next())
                System.out.println(rs.getInt(1));*/
            Utils.printRows(rs);

            // Close the connections to the data store resources
            ctx.close();
            rs.close();
            stmt.close();
            conn.close();
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
                }
            }
        }
    }

    //Method to bind ConnectionPoolDataSource object

    private static void bindConnectionPoolDataSource(Context ctx, String cpdsn)
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
}
