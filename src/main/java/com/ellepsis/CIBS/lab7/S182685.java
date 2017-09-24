package com.ellepsis.CIBS.lab7;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import oracle.jdbc.pool.OracleDataSource;

import javax.naming.NamingException;
import javax.sql.DataSource;
import java.io.*;
import java.sql.*;

/**
 * @author Ellepsis
 * @since 0.0.1
 */
public class S182685 {

    public final static String DATA_SOURCE_NAME = "ORACLE_DATA_SOURCE";
    private final static String DATA_SOURCE_SERIALIZED_FILE = "DATA_SOURCE_SERIALIZED_FILE";
    private final static String DB_USERNAME = "s182685";
    private final static String DB_PASSWORD = "some password";
    private final static int ORACLE_PORT_NUMBER = 9999;

    public static void main(String[] args) {
        OracleDataSource oracleDataSource = null;
        try {
            oracleDataSource = createDataSource(DB_USERNAME, DB_PASSWORD);
        } catch (SQLException e) {
            System.err.println("Can't create datasource to oracle database");
            e.printStackTrace();
            return;
        }
        try {
            serializeDataSource(DATA_SOURCE_SERIALIZED_FILE, oracleDataSource);
        } catch (IOException e) {
            System.err.println("Can't serialize data source object");
            e.printStackTrace();
            return;
        }
        DataSource dataSource;
        try {
            dataSource = deserializeDataSource(DATA_SOURCE_SERIALIZED_FILE);
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Can't deserialize data source object");
            e.printStackTrace();
            return;
        }
        try (Connection connection = dataSource.getConnection()) {
            executeRequest(connection);
        } catch (SQLException e) {
            System.err.println("Ошибка выполнения запроса: ");
            e.printStackTrace();
        }
    }

    private static OracleDataSource createDataSource(String user, String password)
            throws SQLException {
        OracleDataSource oracleDataSource = new OracleDataSource();
        //Set the connection parameters
        oracleDataSource.setUser(user);
        oracleDataSource.setPassword(password);
        oracleDataSource.setDriverType("thin");
        oracleDataSource.setDatabaseName("orbis");
        oracleDataSource.setServerName("localhost");
        oracleDataSource.setPortNumber(ORACLE_PORT_NUMBER);
        return oracleDataSource;
    }

    private static void serializeDataSource(String filename, DataSource dataSource) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(filename);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
        objectOutputStream.writeObject(dataSource);
        objectOutputStream.flush();
        objectOutputStream.close();
    }

    private static DataSource deserializeDataSource(String filename) throws IOException, ClassNotFoundException {
        FileInputStream fileInputStream = new FileInputStream(filename);
        ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
        DataSource dataSource = (DataSource) objectInputStream.readObject();
        objectInputStream.close();
        return dataSource;
    }

    private static void executeRequest(Connection connection) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT ГРУППА, ФАМИЛИЯ, ИМЯ," +
                " ОТЧЕСТВО, ДАТА_РОЖДЕНИЯ, МЕСТО_РОЖДЕНИЯ\n" +
                "FROM Н_ЛЮДИ\n" +
                "JOIN  Н_УЧЕНИКИ ON Н_ЛЮДИ.ИД = Н_УЧЕНИКИ.ЧЛВК_ИД\n" +
                "WHERE ГРУППА = ?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        preparedStatement.setString(1, "4106");
        boolean execute = preparedStatement.execute();
        if (execute) {
            ResultSet resultSet = preparedStatement.getResultSet();
            System.out.println("Получить список студентов своей группы и вывести" +
                    " на экран номер группы, фамилию, имя, отчество, дату рождения и место рождения");
            printRows(resultSet);
        } else {
            throw new SQLException("Unknown result of executed statement");
        }
    }

    private static void printRows(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        StringBuilder sb = new StringBuilder(256);
        for (int i = 1; i <= columnCount; i++) {
            sb.append("| \t").append(metaData.getColumnName(i)).append("\t");
        }
        System.out.println(sb.toString() + '\n');
        resultSet.afterLast();
        while (resultSet.previous()) {
            sb = new StringBuilder(256);
            for (int i = 1; i <= columnCount; i++) {
                sb.append("| \t").append(resultSet.getString(i)).append("\t");
            }
            System.out.println(sb.toString());
        }
    }
}
