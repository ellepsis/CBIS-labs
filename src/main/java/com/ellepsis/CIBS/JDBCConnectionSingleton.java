package com.ellepsis.CIBS;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author Ellepsis
 * @since 0.0.1
 */
public class JDBCConnectionSingleton {
    private static final String driver = "oracle.jdbc.OracleDriver";
    private static final String jdbcUrl = "jdbc:oracle:thin:@localhost:9999:orbis";
//    private static final String jdbcUrl = "jdbc:oracle:thin:@localhost:1521:orbis";
    private static final String user = "s182685";
    private static volatile Connection connection;

    public static Connection getConnection() throws ClassNotFoundException, SQLException {
        if (connection == null) {
            synchronized (JDBCConnectionSingleton.class) {
                if (connection == null) {
                    Class.forName(driver); //load jdbc driver
                    String password = new String(System.console().readPassword());
                    connection = DriverManager.getConnection(jdbcUrl, user, password);
                }
            }
        }
        return connection;
    }

    public static void closeConnection() throws SQLException {
        synchronized (JDBCConnectionSingleton.class) {
            connection.close();
            connection = null;
        }
    }
}
