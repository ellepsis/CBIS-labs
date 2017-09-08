package com.ellepsis.CIBS;

import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.sql.*;
import java.util.Arrays;

/**
 * @author Ellepsis
 * @since 0.0.1
 */
public class Lab2 {


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
            System.out.println("Создание табиц");
            createTables(connection);
            System.out.println("Наполение данными");
            fillTables(connection);
            System.out.println("Пример выполения statement.execute()");
            someWeirdMethod(connection);
            System.out.println("Заполнение данными в пакетном режиме");
            fillTablesBatch(connection);
            System.out.println("Заполнение данными в пакетном режиме Prepared statement");
            fillTablesPreparedStatement(connection);
            createProc(connection);
            executeProc(connection);
        } catch (SQLException e) {
            System.err.println("Ошибка SQL");
            e.printStackTrace();
        } finally {
            JDBCConnectionSingleton.closeConnection();
        }
    }

    private static void createTables(Connection connection) throws SQLException {
        final String dropCarTableQuery = "DROP TABLE car CASCADE CONSTRAINTS";
        final String createCarTableQuery = "CREATE TABLE car ( " +
                "  id           NUMBER(3) PRIMARY KEY, " +
                "  model        VARCHAR2(64) NOT NULL, " +
                "  release_year NUMBER(4) " +
                ")";
        final String dropCarRentTableQuery = "DROP TABLE car_rent CASCADE CONSTRAINTS";
        final String createCarRentTableQuery = "CREATE TABLE car_rent (\n" +
                "  id           NUMBER(6) PRIMARY KEY,\n" +
                "  start_date   DATE,\n" +
                "  end_date     DATE,\n" +
                "  rent_comment VARCHAR2(1024),\n" +
                "  car_id       NUMBER(3) REFERENCES car (id)\n" +
                ")";
        try {
            createTable(connection, dropCarTableQuery, createCarTableQuery);
        } catch (SQLException e) {
            System.err.println("Ошибка при создании таблицы CAR");
            throw e;
        }
        System.out.println("Таблица CAR создана");
        try {
            createTable(connection, dropCarRentTableQuery, createCarRentTableQuery);
        } catch (SQLException e) {
            System.err.println("Ошибка при создании таблицы CAR_RENT");
            throw e;
        }
        System.out.println("Таблица CAR_RENT создана");
    }

    private static void createTable(Connection connection, String dropQuery, String createQuery) throws SQLException {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            try {
                statement.executeUpdate(dropQuery);
            } catch (SQLException e) {
                if (e.getErrorCode() == 942) {
                    String msg = e.getMessage();
                    System.out.println("Drop table error " + msg);
                } else {
                    throw e;
                }
            }
            statement.executeUpdate(createQuery);
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    private static void fillTables(Connection connection) throws SQLException {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.executeUpdate("INSERT INTO CAR VALUES (1, 'Машина 1', 1993)");
            statement.executeUpdate("INSERT INTO CAR VALUES (3, 'Машина 2', 1345)");
            statement.executeUpdate("INSERT INTO CAR VALUES (5, 'Машина 3', 2134)");
            statement.executeUpdate("INSERT INTO CAR VALUES (6, 'Машина 4', 4234)");
            statement.executeUpdate("INSERT INTO CAR_RENT VALUES (1, {d '2000-9-15'}, {d '2000-9-15'}, 'comment 1', 1)");
            statement.executeUpdate("INSERT INTO CAR_RENT VALUES (2, {d '2000-9-16'}, {d '2010-9-15'}, 'com 1', 5)");
            statement.executeUpdate("INSERT INTO CAR_RENT VALUES (3, {d '2000-9-13'}, {d '2040-9-15'}, 'com2', 1)");
            statement.executeUpdate("INSERT INTO CAR_RENT VALUES (4, {d '2000-12-15'}, {d '2061-9-15'}, 'comfdsa1', 6)");
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    private static void someWeirdMethod(Connection connection) throws SQLException {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute("INSERT INTO CAR VALUES (123, 'Машина 1', 1993)");
            processExecuteResult(statement);
            statement.execute("SELECT * FROM CAR");
            processExecuteResult(statement);
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    private static void processExecuteResult(Statement statement) throws SQLException {
        do {
            ResultSet resultSet = statement.getResultSet();
            if (resultSet != null) {
                Utils.printRows(resultSet);
            } else {
                System.out.println("Updates count: " + statement.getUpdateCount());
            }
        } while (statement.getMoreResults());
    }

    private static void fillTablesBatch(Connection connection) throws SQLException {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.addBatch("INSERT INTO CAR VALUES (10, 'Машина 11', 1993)");
            statement.addBatch("INSERT INTO CAR VALUES (11, 'Машина 12', 1345)");
            statement.addBatch("INSERT INTO CAR VALUES (12, 'Машина 13', 2134)");
            statement.addBatch("INSERT INTO CAR VALUES (13, 'Машина 14', 4234)");
            statement.addBatch("INSERT INTO CAR_RENT VALUES (12, {d '2000-9-15'}, {d '2000-9-15'}, 'comment 1', 1)");
            statement.addBatch("INSERT INTO CAR_RENT VALUES (13, {d '2000-9-16'}, {d '2010-9-15'}, 'com 1', 5)");
            statement.addBatch("INSERT INTO CAR_RENT VALUES (14, {d '2000-9-13'}, {d '2040-9-15'}, 'com2', 1)");
            statement.addBatch("INSERT INTO CAR_RENT VALUES (45, {d '2000-12-15'}, {d '2061-9-15'}, 'comfdsa1', 6)");
            Arrays.stream(statement.executeBatch()).forEach(o -> System.out.println("rows updated: " + o));
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    private static void fillTablesPreparedStatement(Connection connection) throws SQLException {
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement("INSERT INTO car VALUES (?,?,?)");
            for (int i = 0; i < 5; i++) {
                statement.setInt(1, i + 100);
                statement.setInt(3, 2000 + i);
                statement.setString(2, "car #" + i);
                statement.addBatch();
            }
            statement.executeBatch();
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    public static void createProc(Connection connection) throws SQLException {
        Statement st = null;
        try {
            st = connection.createStatement();
            String sql = "CREATE PROCEDURE get_fio "
                    + "(id IN NUMBER, fio OUT VARCHAR) AS "
                    + "BEGIN "
                    + "SELECT человек(id, 'И', 9) INTO fio FROM DUAL; "
                    + "END;";
            try {
                st.executeUpdate("DROP PROCEDURE get_fio");
            } catch (SQLException se) {
                //Игнорировать ошибку удаления процедуры
                if (se.getErrorCode() == 4043) {
                    String msg = se.getMessage();
                    System.out.println("Ошибка при удалении процедуры: " + msg);
                }
            }
            //Создание процедуры
            if (st.executeUpdate(sql) == 0)
                System.out.println("Процедура get_fio создана...");
        } finally {
            if (st != null) {
                st.close();
            }
        }
    }

    public static void executeProc(Connection connection) throws SQLException {
        String sql = "{call get_fio(?,?)}";
        CallableStatement cst = null;
        try {
            cst = connection.prepareCall(sql);
            cst.setInt(1, 121018);
            cst.registerOutParameter(2, Types.VARCHAR);
            cst.execute();
            System.out.println("Результат запроса: " + cst.getString(2));
        } finally {
            if (cst != null) {
                cst.close();
            }
        }
    }


}
