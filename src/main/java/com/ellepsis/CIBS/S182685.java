package com.ellepsis.CIBS;

import java.sql.*;
import java.util.Properties;

/**
 * @author Ellepsis
 * @since 0.0.1
 */
public class S182685 {

    private static final String jdbcUrl = "jdbc:oracle:thin:@localhost:9999:orbis";
    private static final String user = "s182685";

    public static void main(String[] args) {
        Connection connection = null;
        try {
            connection = connect();
        } catch (SQLException e) {
            System.err.println("Can't establish a connection with a database");
            e.printStackTrace();
            return;
        }
        try {
            doWork(connection);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (SQLException e) {
                System.err.println("Can't close the database connection");
            }
        }
    }

    private static void doWork(Connection connection) throws SQLException {
        try {
            createTable(connection);
        } catch (SQLException e) {
            System.err.println("An error has occurred while table creation");
            throw e;
        }
        try {
            fillData(connection);
        } catch (SQLException e) {
            System.err.println("An error has occurred while the data insertion process");
            throw e;
        }
        System.out.println("Данные в таблице до обновления");
        displayTable(connection);
        try {
            fillDataThroughResultSet(connection);
        } catch (SQLException e) {
            System.err.println("An error has occurred while the data insertion process through result set");
            throw e;
        }
        System.out.println("Данные в таблице после обновления");
        displayTable(connection);
        System.out.println("\n\nИнформация о базе данных");
        displayDatabaseMetadata(connection);
    }

    private static Connection connect() throws SQLException {
        String password = "somenewPassword";
        Properties info = new Properties();
        info.setProperty("user", user);
        info.setProperty("password", password);
        return DriverManager.getConnection(jdbcUrl, info);
    }

    private static void createTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try {
                statement.executeUpdate("DROP TABLE driver CASCADE CONSTRAINTS"); //Удаляем таблицу
            } catch (SQLException e) {
                if (e.getErrorCode() == 942) {  //если уже сущетвует, игнорим экзепшен, сообщаем пользователю
                    String msg = e.getMessage();
                    System.out.println("Drop table error " + msg);
                } else {
                    throw e;
                }
            }
            //Выполняем DDL скрипт создания таблицы
            statement.executeUpdate("CREATE TABLE driver (\n" +
                    "  id            NUMBER(3) PRIMARY KEY,\n" +
                    "  first_name    VARCHAR2(32) NOT NULL,\n" +
                    "  second_name   VARCHAR2(32) NOT NULL,\n" +
                    "  birthday_date DATE\n" +
                    ")");
        }
    }

    private static void fillData(Connection connection) throws SQLException {
        //создаем стейтмент, добавляем данные с помощью inset через executeUpdate
        try (Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
            statement.executeUpdate("INSERT INTO driver VALUES (1, 'vasya', 'ivanov', {d '1992-01-01'})");
            statement.executeUpdate("INSERT INTO driver VALUES (2, 'petya', 'vasin', {d '1982-02-3'})");
            statement.executeUpdate("INSERT INTO driver VALUES (3, 'ivan', 'petrov', {d '1987-03-21'})");
            statement.executeUpdate("INSERT INTO driver VALUES (4, 'alex', 'smirnov', {d '1995-4-05'})");
        }
    }

    private static void fillDataThroughResultSet(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
            ResultSet resultSet = statement.executeQuery("SELECT id, first_name, second_name, birthday_date FROM driver");
            while (resultSet.next()) {
                String firstName = firstCharToUpperCase(resultSet.getString(2));
                String secondName = firstCharToUpperCase(resultSet.getString(3));
                resultSet.updateString(2, firstName);
                resultSet.updateString(3, secondName);
                resultSet.updateRow();
            }
        }
    }

    private static void displayDatabaseMetadata(Connection connection) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        System.out.println("Название базы данных: " + metaData.getDatabaseProductName());
        System.out.println("Максимальная длина имени колонки: " + metaData.getMaxColumnNameLength());
        System.out.println("Поддерживает ли ANSI92SQL: " + (metaData.supportsANSI92FullSQL() ? "да" : "нет"));
        System.out.println("Типы таблиц в базе данных: ");
        ResultSet resultSet = metaData.getTableTypes();
        Utils.printRows(resultSet);
        System.out.println("Первая колонка resultSet isAutoIncrement?: " + resultSet.getMetaData().isAutoIncrement(1));

    }

    private static void displayTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM driver");
            Utils.printRows(resultSet);
        } catch (SQLException e) {
            System.err.println("Can't display the table data");
            throw e;
        }
    }

    private static String firstCharToUpperCase(String string) {
        String res = "";
        if (string != null && string.length() > 0) {
            res += Character.toUpperCase(string.charAt(0));
        } else {
            return "";
        }
        if (string.length() > 1) {
            res += string.substring(1, string.length());
        }
        return res;
    }
}

