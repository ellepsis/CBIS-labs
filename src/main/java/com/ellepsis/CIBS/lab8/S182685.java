package com.ellepsis.CIBS.lab8;

import com.ellepsis.CIBS.JDBCConnectionSingleton;
import com.ellepsis.CIBS.Utils;
import oracle.jdbc.rowset.OracleCachedRowSet;

import javax.sql.rowset.CachedRowSet;
import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Ellepsis
 * @since 0.0.1
 */
public class S182685 {
    public static final String FILE_NAME = "S182685_SERIALIZED";

    public static void main(String[] args) {
        try (Connection connection = JDBCConnectionSingleton.getConnection()) {
            ResultSet resultSet;
            try {
                resultSet = executeQuery(connection);
            } catch (SQLException e) {
                System.err.println("Can't execute query");
                e.printStackTrace();
                return;
            }
            System.out.println("Result set before conversion to CachedRowSet");
            Utils.printRows(resultSet);
            try {
                createAndPersistCachedRowSet(resultSet, FILE_NAME);
            } catch (IOException e) {
                System.err.println("Can't serialize cashed row set");
                return;
            }
            resultSet.close();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        CachedRowSet cachedRowSet;
        try {
            cachedRowSet = deserializeRowSet(FILE_NAME);
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Can't deserialize result set");
            return;
        }
        System.out.println("Cashed row set result after deserialization");
        try {
            Utils.printRows(cachedRowSet);
        } catch (SQLException e) {
            System.err.println("Can't print rows");
            e.printStackTrace();
        }
    }

    private static void createAndPersistCachedRowSet(ResultSet resultSet, String filename)
            throws SQLException, IOException {
        resultSet.beforeFirst();
        CachedRowSet cachedRowSet = new OracleCachedRowSet();
        cachedRowSet.populate(resultSet);
        serializeRowSet(filename, cachedRowSet);
    }

    private static void serializeRowSet(String filename, CachedRowSet cachedRowSet) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(filename);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
        objectOutputStream.writeObject(cachedRowSet);
        objectOutputStream.flush();
        objectOutputStream.close();
    }

    private static CachedRowSet deserializeRowSet(String filename) throws IOException, ClassNotFoundException {
        FileInputStream fileInputStream = new FileInputStream(filename);
        ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
        CachedRowSet cachedRowSet = (CachedRowSet) objectInputStream.readObject();
        objectInputStream.close();
        return cachedRowSet;
    }

    private static ResultSet executeQuery(Connection connection) throws SQLException {
        Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        return statement.executeQuery("SELECT COUNT(Фамилия) AS Всего, COUNT(DISTINCT ИМЯ) AS Разных_имен, " +
                "COUNT(DISTINCT ОТЧЕСТВО) AS Разных_отчеств\n" +
                "FROM Н_ЛЮДИ\n" +
                "WHERE ФАМИЛИЯ = 'Иванов'\n" +
                "      AND length(ИМЯ) > 1\n" +
                "      AND length(ОТЧЕСТВО) > 1\n" +
                "      AND ИМЯ NOT LIKE '_.'\n" +
                "      AND Н_ЛЮДИ.ФАМИЛИЯ NOT LIKE '_.'");
    }
}
