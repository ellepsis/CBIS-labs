package dima;

import com.ellepsis.CIBS.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.sql.*;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.InitialContext;
import java.util.Hashtable;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.naming.BinaryRefAddr;
import javax.naming.Reference;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import javax.sql.rowset.CachedRowSet;
import oracle.jdbc.pool.OracleConnectionPoolDataSource;
import oracle.jdbc.rowset.OracleCachedRowSet;

public class S182506 {
    private static final String SP = "com.sun.jndi.fscontext.RefFSContextFactory";
    private static final String FILE = "file:s182506";
    private static final String CONNECTION_POOL_NAME = "myConnectionPool";
    private static final String CACHED_ROW_SET_NAME = "myCachedRowSet";
    private static Connection conn = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;
    private static CachedRowSet crs = null;
    private static Context ctx = null;
    private static ConnectionPoolDataSource cpds = null;
    private static PooledConnection pc = null;
    private static String user;
    private static String password;

    public static void main (String args []){
        initContext();
        bindConnectionToContext();
        if (args != null && args.length != 0) {
            switch (args[0]) {
                case "7":
                    executeLab7();
                    break;
                case "8":
                    executeLab8();
                    break;
                case "9":
                    executeLab9();
                    break;
                default:
                    executeLab7();
                    executeLab8();
                    executeLab9();
            }
        }
        else {
            executeLab7();
            executeLab8();
            executeLab9();
        }
        closeContext();
    }
    
    private static void bindConnectionToContext() {
        try {
            System.out.println("Please, input your username:");
            user = System.console().readLine();
            System.out.println("Please, input your password:");
            password = new String(System.console().readPassword());
            
            //Bind the ConnectionPoolDataSource object
            bindConnectionPoolDataSource(ctx, CONNECTION_POOL_NAME);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    
    private static void initContext() {
        try {
            //Create Hashtable to hold environment properties
            //then open InitialContext
            Hashtable env = new Hashtable();
            env.put (Context.INITIAL_CONTEXT_FACTORY, SP);
            env.put (Context.PROVIDER_URL, FILE);
            ctx = new InitialContext(env);
        }
        catch (NamingException ne){
            ne.printStackTrace();
        }
    }
    
    private static void closeContext() {
        try {
            if(ctx != null)
                ctx.close();
        }
        catch (NamingException ne) {
            ne.printStackTrace();
        }
    }
    
    private static void executeLab9() {
        try {
            try {
                //Retrieve the CachedRowSet object
                crs = readCachedRowSet(ctx);
            } catch (Exception e) {
                e.printStackTrace();
            }
            
            System.out.println("CachedRowSet was read from file!");
            Utils.printRowsWithTypeName(crs);
            
            try {
                //Retrieve the ConnectionPoolDataSource object
                cpds = (ConnectionPoolDataSource) ctx.lookup(CONNECTION_POOL_NAME);
            } catch (NamingException e) {
                e.printStackTrace();
            }
            
            pc = cpds.getPooledConnection();
            conn = pc.getConnection();
            createTableFillDataAndUpdate();
            
            // Close the connections to the data store resources
            crs.close();
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
                    if(conn!=null)
                        conn.close();
                }
                catch (SQLException se) {
                    se.printStackTrace();
                }
            }
        }
    }
    
    private static void executeLab8() {
        try {
            //Retrieve the ConnectionPoolDataSource object
            cpds = (ConnectionPoolDataSource) ctx.lookup(CONNECTION_POOL_NAME);
            
            pc = cpds.getPooledConnection();
            conn = pc.getConnection();
            stmt = conn.createStatement();
            String sql = "SELECT \"FIO\", \"KOLICHESTVO\"\n" +
        "FROM (\n" +
        "SELECT ‘¿Ã»À»ﬂ \"FIO\", COUNT(‘¿Ã»À»ﬂ) \"KOLICHESTVO\"\n" +
        "FROM Õ_Àﬁƒ»\n" +
        "GROUP BY ‘¿Ã»À»ﬂ\n" +
        "HAVING COUNT(‘¿Ã»À»ﬂ) >= 50\n" +
        "UNION\n" +
        "SELECT »Ãﬂ, COUNT(»Ãﬂ)\n" +
        "FROM Õ_Àﬁƒ»\n" +
        "GROUP BY »Ãﬂ\n" +
        "HAVING COUNT(»Ãﬂ) >= 300\n" +
        "UNION\n" +
        "SELECT Œ“◊≈—“¬Œ, COUNT(Œ“◊≈—“¬Œ)\n" +
        "FROM Õ_Àﬁƒ»\n" +
        "GROUP BY Œ“◊≈—“¬Œ\n" +
        "HAVING COUNT(Œ“◊≈—“¬Œ) >= 300\n" +
        ")\n" +
        "ORDER BY \"KOLICHESTVO\"";
            rs = stmt.executeQuery(sql);
            
            crs = new OracleCachedRowSet();
            crs.populate(rs);
            System.out.println("ResultSet was written to CachedRowSet!");
            try {
                bindCachedRowSet(ctx, crs);
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("CachedRowSet was saved to file!");
            
            Utils.printRows(crs);
            
            // Close the connections to the data store resources
            crs.close();
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
                    if(conn!=null)
                        conn.close();
                }
                catch (SQLException se) {
                    se.printStackTrace();
                }
            }
        }
    }
    
    private static void executeLab7() {
        try {
            //Retrieve the ConnectionPoolDataSource object
            cpds = (ConnectionPoolDataSource) ctx.lookup(CONNECTION_POOL_NAME);
            
            pc = cpds.getPooledConnection();
            conn = pc.getConnection();
            stmt = conn.createStatement();
            String sql = "SELECT ‘¿Ã»À»ﬂ, »Ãﬂ, Œ“◊≈—“¬Œ, " +
                    "ƒ¿“¿_–Œ∆ƒ≈Õ»ﬂ, Ã≈—“Œ_–Œ∆ƒ≈Õ»ﬂ\n" +
                    "FROM Õ_Àﬁƒ» JOIN\n" +
                    "Õ_”◊≈Õ» »\n" +
                    "ON Õ_Àﬁƒ».»ƒ = Õ_”◊≈Õ» ».◊À¬ _»ƒ\n" +
                    "WHERE √–”œœ¿ = 4108";
            rs = stmt.executeQuery(sql);
            Utils.printRows(rs);

            // Close the connections to the data store resources
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
                    if(conn!=null)
                        conn.close();
                }
                catch (SQLException se) {
                    se.printStackTrace();
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
    
    public static void bindCachedRowSet(Context ctx, CachedRowSet crs)
            throws Exception {
        // Bind RowSet object.
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
          out = new ObjectOutputStream(bos);   
          out.writeObject(crs);
          out.flush();
          byte[] bytes = bos.toByteArray();
          Reference refCachedRowSet = new Reference(CachedRowSet.class.getName(),
            new BinaryRefAddr(CACHED_ROW_SET_NAME, bytes));
          ctx.rebind(CACHED_ROW_SET_NAME, refCachedRowSet);
        } finally {
          try {
            bos.close();
          } catch (IOException ex) {}
        }
    }

    private static CachedRowSet readCachedRowSet(Context ctx)
            throws Exception {
        ByteArrayInputStream bis = new ByteArrayInputStream((byte[])
                    ((Reference)ctx.lookup(CACHED_ROW_SET_NAME))
                            .get(0).getContent());
        ObjectInput in = null;
        CachedRowSet oracleCachedRowSet = null;
        try {
          in = new ObjectInputStream(bis);
          oracleCachedRowSet = (CachedRowSet)in.readObject();
        }
        finally {
          try {
            if (in != null) {
              in.close();
            }
          } catch (IOException ex) {}
        }
        return oracleCachedRowSet;
    }
    
    private static void createTableFillDataAndUpdate() throws SQLException {
        // Drop table
        stmt = conn.createStatement();
        try {
            stmt.executeQuery("DROP TABLE Temp_Table CASCADE CONSTRAINTS");
            System.out.println("Table 'Temp_Table' was deleted!");
        }
        catch (SQLException e) {
            if (e.getErrorCode() != 942) {
                e.printStackTrace();
                return;
            }
        }
        // Create table "Temp_Table"
        ResultSetMetaData m = crs.getMetaData();
        int columnCount = m.getColumnCount();
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE Temp_Table (\n");
        for (int i = 1; i <= columnCount; i++) {
            sb//.append("\"")
                    .append(m.getColumnName(i))
                    //.append("\"")
                    .append(" ")
                    .append(m.getColumnTypeName(i))
                    .append("(15)");
            if (i == 1)
                sb.append(" CONSTRAINT PK1 PRIMARY KEY");
            else
                sb.append(" NOT NULL");
            if (i != columnCount)
                sb.append(",");
            sb.append("\n");
        }
        sb.append(")");
        stmt.executeQuery(sb.toString());
        System.out.println("Table 'Temp_Table' was created!");
        // Fill data to created table "Temp_Table"
        crs.beforeFirst();
        while (crs.next()) {
            sb = new StringBuilder();
            sb.append("INSERT INTO Temp_Table VALUES (");
            for (int i = 1; i <= columnCount; i++) {
                if (i != columnCount) {
                    sb.append("'");
                    sb.append(crs.getString(i));
                    sb.append("'");
                    sb.append(", ");
                }
                else
                    sb.append(crs.getInt(i));
            }
            sb.append(")");
            stmt.executeQuery(sb.toString());
        }
        System.out.println("Table 'Temp_Table' was filled with data!");
        sb = new StringBuilder();
        sb.append("SELECT Temp_Table.* FROM Temp_Table");
        crs.setPassword(password);
        crs.setCommand(sb.toString());
        crs.execute();
        Utils.printRowsWithTypeName(crs);
        // Update data in the table "Temp_Table"
        crs.beforeFirst();
        crs.setReadOnly(false);
        while (crs.next()) {
            crs.updateString(1, crs.getString(1).toUpperCase());
            crs.updateRow();
        }
        crs.acceptChanges();
        System.out.println("\nTable 'Temp_Table' after updating!");
        Utils.printRowsWithTypeName(crs);
    }
}
