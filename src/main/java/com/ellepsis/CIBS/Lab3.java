package com.ellepsis.CIBS;

import java.sql.*;
import java.util.Arrays;

/**
 * @author Ellepsis
 * @since 0.0.1
 */
public class Lab3 {


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
            System.out.println("\nResult set scrollable\n");
            selectScroll(connection);
            System.out.println("\nResult set scrollable. Reverse order.\n");
            selectScrollReverse(connection);
            System.out.println("\nResult set update.\n");
            selectScrollUpdate(connection);
            System.out.println("\nResult set delete and update.\n");
            selectScrollDeleteInsert(connection);
        } catch (SQLException e) {
            System.err.println("Ошибка SQL");
            e.printStackTrace();
        } finally {
            JDBCConnectionSingleton.closeConnection();
        }
    }

    private static void selectScroll(Connection connection) throws SQLException {
        try (Statement st = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = st.executeQuery("SELECT * FROM car");
            System.out.println("Row number " + rs.getRow() + "; BFR is " + rs.isBeforeFirst());
            while (rs.next()) {
                System.out.print("Row number " + rs.getRow() + "; First is " + rs.isFirst());
                System.out.print(":\t" + rs.getInt(1));
                System.out.println("\t" + rs.getString(2));
                System.out.println("Row number " + rs.getRow() + "; Last is " + rs.isLast());
            }
            System.out.println("Row number " + rs.getRow() + "; ALR is " + rs.isAfterLast());
        }
    }

    private static void selectScrollReverse(Connection connection) throws SQLException {
        try (Statement st = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = st.executeQuery("SELECT * FROM car");
            rs.afterLast();
            System.out.println("Row number " + rs.getRow() + "; BFR is " + rs.isBeforeFirst());
            while (rs.previous()) {
                System.out.print("Row number " + rs.getRow() + "; First is " + rs.isFirst());
                System.out.print(":\t" + rs.getInt(1));
                System.out.println("\t" + rs.getString(2));
                System.out.println("Row number " + rs.getRow() + "; Last is " + rs.isLast());
            }
            System.out.println("Row number " + rs.getRow() + "; ALR is " + rs.isAfterLast());
        }
    }

    private static void selectScrollUpdate(Connection connection) throws SQLException {
        try (Statement st = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
            ResultSet rs = st.executeQuery("SELECT id, model, release_year FROM car");
            while (rs.next()) {
                String model = rs.getString(2);
                System.out.println(rs.getInt(1) + "\t" + model);
                if (rs.getInt(3) > 2000) {
                    rs.updateString(2, "new: " + model);
                    rs.updateRow();
                }
            }
            System.out.println("\n After update: ");
            rs.beforeFirst();
            Utils.printRows(rs);
        }
    }

    private static void selectScrollDeleteInsert(Connection connection) throws SQLException {
        try (Statement st = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
            ResultSet rs = st.executeQuery("SELECT id, model, release_year FROM car");
            int deleted_idx = 500;
            while (rs.next()) {
                System.out.println(rs.getInt(1) + "\t" + rs.getString(2));
                if (rs.getInt(1) > deleted_idx) {
                    deleted_idx = rs.getInt(1);
                    rs.deleteRow();
                }
            }
            rs.moveToInsertRow();
            rs.updateInt(1, deleted_idx + 1);
            rs.updateString(2, "some model");
            rs.updateInt(3, deleted_idx % 10000);
            rs.insertRow();
            //rs = st.executeQuery("SELECT * FROM операторы_связи");
            rs.beforeFirst();
            Utils.printRows(rs);
        }
    }
}
