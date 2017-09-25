package com.ellepsis.CIBS.lab9;

import com.ellepsis.CIBS.JDBCConnectionSingleton;
import com.ellepsis.CIBS.Utils;

import javax.rmi.CORBA.Util;
import javax.sql.rowset.CachedRowSet;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.*;

/**
 * @author Ellepsis
 * @since 0.0.1
 */
public class S182685 {
    private static final String FILE_NAME = "S182685_SERIALIZED";
    public static final int ALREADY_EXIST_CODE = 942;

    public static void main(String[] args) {
        CachedRowSet cachedRowSet;
        try {
            cachedRowSet = deserializeRowSet(FILE_NAME);
        } catch (IOException | ClassNotFoundException e) {
            System.out.println("Can't deserialize row set");
            e.printStackTrace();
            return;
        }
        try {
            printCachedRowSet(cachedRowSet);
        } catch (SQLException e) {
            System.err.println("Can't print row set");
            e.printStackTrace();
            return;
        }
        try (Connection connection = JDBCConnectionSingleton.getConnection()) {
            try {
                createTable(connection);
            } catch (SQLException e) {
                System.err.println("Can't create table");
                e.printStackTrace();
                return;
            }
            try {
                insertData(connection, cachedRowSet);
            } catch (SQLException e) {
                System.err.println("Can't insert data from cached row set");
                e.printStackTrace();
                return;
            }
            try {
                System.out.println("\nДо изменений");
                cachedRowSet.beforeFirst();
                printCachedRowSet(cachedRowSet);
                updateTable(connection, cachedRowSet);
                System.out.println("\nПосле изменений");
                cachedRowSet.beforeFirst();
                printCachedRowSet(cachedRowSet);
            } catch (SQLException e) {
                System.err.print("Can't update or read table data");
                e.printStackTrace();
            }
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    private static CachedRowSet deserializeRowSet(String filename) throws IOException, ClassNotFoundException {
        FileInputStream fileInputStream = new FileInputStream(filename);
        ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
        CachedRowSet cachedRowSet = (CachedRowSet) objectInputStream.readObject();
        objectInputStream.close();
        return cachedRowSet;
    }

    private static void printCachedRowSet(CachedRowSet cachedRowSet) throws SQLException {
        ResultSetMetaData metaData = cachedRowSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        StringBuilder sb = new StringBuilder(256);
        for (int i = 1; i <= columnCount; i++) {
            sb.append(metaData.getColumnName(i)).append("(").append(metaData.getColumnTypeName(i)).append(")\t | ");
        }
        sb.append("\n");
        while (cachedRowSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                sb.append(cachedRowSet.getString(i)).append("\t | ");
            }
            sb.append('\n');
        }
        System.out.print(sb.toString());
    }

    private static void createTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try {
                statement.executeUpdate("DROP TABLE lab9");
            } catch (SQLException e) {
                if (e.getErrorCode() == ALREADY_EXIST_CODE) {
                    String msg = e.getMessage();
                    System.out.println("Drop table error " + msg);
                } else {
                    throw e;
                }
            }
            statement.executeUpdate("CREATE TABLE lab9 (\n" +
                    "  ВСЕГО          NUMBER,\n" +
                    "  РАЗНЫХ_ИМЕН    NUMBER,\n" +
                    "  РАЗНЫХ_ОТЧЕСТВ NUMBER\n" +
                    ")");
        }
    }

    private static void insertData(Connection connection, CachedRowSet cachedRowSet) throws SQLException {
        StringBuilder sb = new StringBuilder(1024);
        ResultSetMetaData metaData = cachedRowSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        cachedRowSet.beforeFirst();
        try (Statement statement = connection.createStatement()) {
            while (cachedRowSet.next()) {
                sb.setLength(0);
                sb.append("INSERT INTO lab9 VALUES (");
                for (int i = 1; i <= columnCount; i++) {
                    switch (metaData.getColumnType(i)) {
                        case Types.NUMERIC: {
                            sb.append(cachedRowSet.getInt(i));
                            if (i != columnCount) { // if is a last element, not insert the delimiter
                                sb.append(",");
                            }
                            break;
                        }
                        default:
                            throw new IllegalArgumentException("Can't insert non numeric values");
                    }
                }
                sb.append(")");
                String insertStatement = sb.toString();
                statement.addBatch(insertStatement);
                System.out.println(insertStatement);
            }
            statement.executeBatch();
        }
    }

    /**
     * WARNING!! The method will be corrupt a cachedRowSet parameter.
     *
     * @param connection   connection to the database
     * @param cachedRowSet to be corrupted :)
     */
    private static void updateTable(Connection connection, CachedRowSet cachedRowSet) throws SQLException {
        cachedRowSet.setCommand("Select * from lab9");
        cachedRowSet.setPassword("somepassword");
        cachedRowSet.execute();
        cachedRowSet.beforeFirst();
        while (cachedRowSet.next()) {
            cachedRowSet.updateInt(1, 0);
            cachedRowSet.updateRow();
        }
        cachedRowSet.acceptChanges();
    }
}

