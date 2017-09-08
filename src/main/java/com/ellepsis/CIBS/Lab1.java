package com.ellepsis.CIBS;

import java.sql.*;

/**
 * @author Ellepsis
 * @since 0.0.1
 */
public class Lab1 {

    public static void main(String[] args) throws SQLException {
        Connection connection;
        try {
            connection = JDBCConnectionSingleton.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
            return;
        }
        try {
            System.out.println("\nВыдать содержимое всех столбцов таблицы Н_ЦИКЛЫ_ДИСЦИПЛИН. ");
            System.out.println("---------------------------------------------------------------");
            printAllColumns(connection);
            System.out.println("\nВыдать содержимое столбцов АББРЕВИАТУРА и НАИМЕНОВАНИЕ той же таблицы.");
            System.out.println("---------------------------------------------------------------");
            printTwoColumns(connection);
            System.out.println("\nПолучить перечень квалификаций, присваиваемых выпускникам нашего университета.");
            System.out.println("---------------------------------------------------------------");
            printQualificationList(connection);
            System.out.println("\nВыдать не повторяющиеся имена людей из таблицы Н_ЛЮДИ.");
            System.out.println("---------------------------------------------------------------");
            printDistinctFirstNames(connection);
            System.out.println("\nКакие состояния студентов (признаки) используются в таблице Н_УЧЕНИКИ.");
            System.out.println("---------------------------------------------------------------");
            printStudentStates(connection);
        } catch (SQLException e) {
            System.err.println("Ошибка SQL");
            e.printStackTrace();
        }
        finally {
            JDBCConnectionSingleton.closeConnection();
        }
    }

    private static void printAllColumns(Connection connection) throws SQLException {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM Н_ЦИКЛЫ_ДИСЦИПЛИН");
            Utils.printRows(resultSet);
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    private static void printTwoColumns(Connection connection) throws SQLException {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT АББРЕВИАТУРА, НАИМЕНОВАНИЕ FROM Н_ЦИКЛЫ_ДИСЦИПЛИН" +
                    " ORDER BY АББРЕВИАТУРА");
            Utils.printRows(resultSet);
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    private static void printQualificationList(Connection connection) throws SQLException {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT НАИМЕНОВАНИЕ FROM Н_КВАЛИФИКАЦИИ" +
                    " ORDER BY НАИМЕНОВАНИЕ");
            Utils.printRows(resultSet);
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    private static void printDistinctFirstNames(Connection connection) throws SQLException {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT DISTINCT ИМЯ FROM Н_ЛЮДИ ORDER BY ИМЯ");
            Utils.printRows(resultSet);
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    private static void printStudentStates(Connection connection) throws SQLException {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT DISTINCT СОСТОЯНИЕ FROM Н_УЧЕНИКИ ORDER BY СОСТОЯНИЕ");
            Utils.printRows(resultSet);
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

}
