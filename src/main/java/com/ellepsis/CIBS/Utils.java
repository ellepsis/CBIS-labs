package com.ellepsis.CIBS;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import javax.sql.rowset.CachedRowSet;

/**
 * @author Ellepsis
 * @since 0.0.1
 */
public class Utils {
    public static void printRows(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        StringBuilder sb = new StringBuilder(256);
        for (int i = 1; i <= columnCount; i++) {
            sb.append("| \t").append(metaData.getColumnName(i)).append("\t");
        }
        System.out.println(sb.toString() + '\n');
        while (resultSet.next()) {
            sb = new StringBuilder(256);
            for (int i = 1; i <= columnCount; i++) {
                sb.append("| \t").append(resultSet.getString(i)).append("\t");
            }
            System.out.println(sb.toString());
        }
    }
    
    public static void printRows(CachedRowSet cachedRowSet) throws SQLException {
        cachedRowSet.beforeFirst();
        ResultSetMetaData metaData = cachedRowSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        StringBuilder sb = new StringBuilder(256);
        for (int i = 1; i <= columnCount; i++) {
            sb.append("| \t").append(metaData.getColumnName(i)).append("\t");
        }
        System.out.println(sb.toString() + '\n');
        while (cachedRowSet.next()) {
            sb = new StringBuilder(256);
            for (int i = 1; i <= columnCount; i++) {
                sb.append("| \t").append(cachedRowSet.getString(i)).append("\t");
            }
            System.out.println(sb.toString());
        }
    }
    
    public static void printRowsWithTypeName(CachedRowSet cachedRowSet) throws SQLException {
        cachedRowSet.beforeFirst();
        ResultSetMetaData metaData = cachedRowSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        StringBuilder sb = new StringBuilder(256);
        for (int i = 1; i <= columnCount; i++) {
            sb.append("| \t").append(metaData.getColumnName(i)).append("\t");
        }
        System.out.println(sb.toString());
        sb = new StringBuilder(256);
        for (int i = 1; i <= columnCount; i++) {
            sb.append("| \t").append(metaData.getColumnTypeName(i)).append("\t");
        }
        System.out.println(sb.toString() + '\n');
        while (cachedRowSet.next()) {
            sb = new StringBuilder(256);
            for (int i = 1; i <= columnCount; i++) {
                sb.append("| \t").append(cachedRowSet.getString(i)).append("\t");
            }
            System.out.println(sb.toString());
        }
    }
}
