package com.ellepsis.CIBS;

import java.io.*;
import java.sql.SQLException;
import java.util.Hashtable;
import javax.naming.BinaryRefAddr;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.Reference;
import oracle.jdbc.rowset.*;

public class Lab6 {
    // Constant to hold file name used to store the WebRowSet
    static Context ctx = null;
    static String user;
    static String password;

    public static void main(String[] args) throws Exception {
        String sp = "com.sun.jndi.fscontext.RefFSContextFactory";
        String file = "file:JNDI";
        String webRowSetName  = "myWebRowSet";
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
            
            // Bind WebRowSet
            bindWebRowSet(ctx, webRowSetName);

            // Create WebRowSet from JNDI
            OracleWebRowSet wrs = readWebRowSet(ctx, webRowSetName);

            executeUpdates(wrs);
            
            // Close resource
            wrs.close();
        }catch (SQLException se){
            se.printStackTrace();
        }catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void bindWebRowSet(Context ctx, String webRowSetName)
            throws Exception{
        // Instantiate a WebRowSet object, set connection parameters
        OracleWebRowSet wrs = new OracleWebRowSet();
        wrs.setUrl("jdbc:oracle:thin:@localhost:1521:orbis");
        wrs.setUsername(user);
        wrs.setPassword(password);

        // Set and execute the command. Notice the parameter query.
        String sql = "SELECT countries.* FROM countries";
        wrs.setCommand(sql);
        wrs.execute();
        
        // Bind WebRowSet object.
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
          out = new ObjectOutputStream(bos);   
          out.writeObject(wrs);
          out.flush();
          byte[] bytes = bos.toByteArray();
          Reference refWebRowSet = new Reference(OracleWebRowSet.class.getName(),
            new BinaryRefAddr("wrs", bytes));
          ctx.rebind(webRowSetName, refWebRowSet);
        } finally {
          try {
            bos.close();
          } catch (IOException ex) {
            // ignore close exception
          }
        }
        wrs.close();
    }

    private static OracleWebRowSet readWebRowSet(Context ctx, String webRowSetName)
            throws Exception {
        ByteArrayInputStream bis = new ByteArrayInputStream((byte[])
                    ((Reference)ctx.lookup(webRowSetName)).get(0).getContent());
        ObjectInput in = null;
        OracleWebRowSet oracleWebRowSet = null;
        try {
          in = new ObjectInputStream(bis);
          oracleWebRowSet = (OracleWebRowSet)in.readObject();
        }
        finally {
          try {
            if (in != null) {
              in.close();
            }
          } catch (IOException ex) {}
        }
        return oracleWebRowSet;
    }

    private static void executeUpdates(OracleWebRowSet wrs) throws Exception {
        // Delete row if exists
        wrs.beforeFirst();
        while(wrs.next()){
            if (wrs.getInt("ID") == 10)
                wrs.deleteRow();
        }
        wrs.acceptChanges();
        
        wrs.beforeFirst();
        System.out.println("Before update:");
        Utils.printRows(wrs);
        
        // Insert new value
        wrs.setReadOnly(false);
        wrs.moveToInsertRow();
        wrs.updateInt("ID", 10);
        wrs.updateString("NAME", "France");
        wrs.updateLong("POPULATION", 67000);
        wrs.insertRow();
        System.out.println("\nAfter insert new value:");
        wrs.beforeFirst();
        Utils.printRows(wrs);
        
        // Update values
        wrs.beforeFirst();
        while(wrs.next()){
            wrs.updateString("NAME", wrs.getString("NAME").toUpperCase());
            wrs.updateRow();
        }
        wrs.beforeFirst();
        System.out.println("\nAfter update to upper case all rows:");
        Utils.printRows(wrs);

        // Cancel update 2nd row
        wrs.absolute(2);
        wrs.cancelRowUpdates();
        System.out.println("\nAfter rollback 2nd row:");
        wrs.beforeFirst();
        Utils.printRows(wrs);
        System.out.println();
        
        // Cancel of all changes
        System.out.println("After rollback all updates:");
        wrs.restoreOriginal();
        wrs.beforeFirst();
        Utils.printRows(wrs);
        
        /*wrs.beforeFirst();
        java.io.FileWriter writer =
            new java.io.FileWriter("1.xml");
        wrs.writeXml(writer);
        Utils.printRows(wrs);*/
    }

}
