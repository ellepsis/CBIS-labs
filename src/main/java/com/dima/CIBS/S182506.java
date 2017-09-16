package com.dima.CIBS;

// Step 1: no import

public class S182506 {

    private static final String URL_PART1 = "jdbc:oracle:thin:";
    private static final String URL_PART2 = "@localhost:1521:orbis";
    private static final String USER = "s182506";

    public static void main(String[] args) {
        java.sql.Connection connection = null;
        try {
            connection = getConnection();
        } catch (java.sql.SQLException e) {
            System.err.println("Can't establish a connection with a database");
            e.printStackTrace();
            return;
        }
        try {
            executeSQLQueries(connection);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                // Step 6: close database
                connection.close();
            } catch (java.sql.SQLException e) {
                System.err.println("Can't close the database connection");
            }
        }
    }

    private static void executeSQLQueries(java.sql.Connection connection)
            throws java.sql.SQLException {
        try {
            readDataBaseMetaData(connection);
        } catch (java.sql.SQLException e) {
            System.err.println("An error has occurred while reading metadata");
            e.printStackTrace();
            return;
        }
        try {
            createTable(connection);
        } catch (java.sql.SQLException e) {
            System.err.println("An error has occurred while table creation");
            e.printStackTrace();
            return;
        }
        try {
            fillData(connection);
        } catch (java.sql.SQLException e) {
            System.err.println("An error has occurred while filling data "
                    + "to the table");
            e.printStackTrace();
            return;
        }
        try {
            printTable(connection);
        } catch (java.sql.SQLException e) {
            System.err.println("An error has occurred while print data "
                    + "from the table");
            e.printStackTrace();
            return;
        }
        try {
            fillDataThroughResultSet(connection);
        } catch (java.sql.SQLException e) {
            System.err.println("An error has occurred while filling data "
                    + "through result set");
            e.printStackTrace();
            return;
        }
        try {
            printTable(connection);
        } catch (java.sql.SQLException e) {
            System.err.println("An error has occurred while print data "
                    + "from the table");
            e.printStackTrace();
        }
    }

    private static java.sql.Connection getConnection()
            throws java.sql.SQLException {
        // Step 2: no driver registration (need use key -D for jvm)
        System.out.println("Please, input your password:");
        String password = new String(System.console().readPassword());
        // Step 3: java.sql.DriverManager.getConnection(url)
        java.sql.Connection connection =
                java.sql.DriverManager.getConnection(URL_PART1 + USER + "/" +
                        password + URL_PART2);
        return connection;
    }

    private static void createTable(java.sql.Connection connection)
            throws java.sql.SQLException {
        java.sql.Statement statement = connection.createStatement();
        try {
            statement.executeQuery("DROP TABLE countries CASCADE CONSTRAINTS");
            System.out.println("Table 'countries' was deleted");
        }
        catch (java.sql.SQLException e) {
            if (e.getErrorCode() != 942) {
                e.printStackTrace();
                return;
            }
        }
         // create table "countries"
        statement.executeQuery("CREATE TABLE countries (\n" +
                    "id         NUMBER(3) CONSTRAINT PK PRIMARY KEY,\n" +
                    "name       VARCHAR2(64) NOT NULL,\n" +
                    "population NUMBER(12) NOT NULL\n" +
                    ")");
        System.out.println("Table 'countries' was created");
        statement.close();
    }

    private static void fillData(java.sql.Connection connection)
            throws java.sql.SQLException {
        // Step 4: java.sql.Statement, executeBatch()
        // create statement, use executeBatch()
        java.sql.Statement statement = connection.
                createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE,
                        java.sql.ResultSet.CONCUR_UPDATABLE);
        statement.addBatch("INSERT INTO countries VALUES (1, 'Russia', 150000000)");
        statement.addBatch("INSERT INTO countries VALUES (2, 'Japan', 350000000)");
        statement.addBatch("INSERT INTO countries VALUES (3, 'China', 1350000000)");
        statement.addBatch("INSERT INTO countries VALUES (4, 'India', 1180000000)");
        int[] count = statement.executeBatch();
        int sum = 0;
        for (int i = 0; i < count.length; i++)
                sum += count[i];
        System.out.println(sum + " row(s) was/were inserted");
        statement.close();
    }

    private static void fillDataThroughResultSet(java.sql.Connection connection)
            throws java.sql.SQLException {
        // Step 5: TYPE_FORWARD_ONLY, CONCUR_UPDATABLE
        java.sql.Statement statement =
                connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                        java.sql.ResultSet.CONCUR_UPDATABLE);
        java.sql.ResultSet resultSet =
                statement.executeQuery("SELECT countries.* FROM countries");
        int count = 0;
        while (resultSet.next()) {
            Long population = resultSet.getLong(3) / 1000;
            resultSet.updateLong(3, population);
            resultSet.updateRow();
            count++;
        }
        System.out.println(count +
                " row(s) was/were updated through result set");
        statement.close();
        resultSet.close();
    }

    private static void printTable(java.sql.Connection connection)
            throws java.sql.SQLException {
        try (java.sql.Statement statement = connection.createStatement()) {
            java.sql.ResultSet resultSet =
                    statement.executeQuery("SELECT * FROM countries");
            printResultSet(resultSet);
            statement.close();
            resultSet.close();
        } catch (java.sql.SQLException e) {
            System.err.println("Can't display the table data");
            throw e;
        }
    }
    
    private static void printResultSet(java.sql.ResultSet resultSet)
            throws java.sql.SQLException {
        java.sql.ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        StringBuilder sb = new StringBuilder(256);
        for (int i = 1; i <= columnCount; i++) {
            sb.append("| \t").append(metaData.getColumnName(i)).append("\t");
        }
        System.out.println(sb.toString());
        while (resultSet.next()) {
            sb = new StringBuilder(256);
            for (int i = 1; i <= columnCount; i++) {
                sb.append("| \t").append(resultSet.getString(i)).append("\t");
            }
            System.out.println(sb.toString());
        }
        System.out.println();
    }

    private static void readDataBaseMetaData(java.sql.Connection connection)
            throws java.sql.SQLException {
        // Step 5: DatabaseMetaData
        java.sql.DatabaseMetaData databaseMetaData = connection.getMetaData();
        System.out.println("Database name: " +
                databaseMetaData.getDatabaseProductName());
        System.out.println("Max row size: " +
                databaseMetaData.getMaxRowSize());
        System.out.println("All tables are selectable: " +
                databaseMetaData.allTablesAreSelectable());
        java.sql.ResultSet resultSet = databaseMetaData.getTableTypes();
        System.out.println("Existing table types: ");
        printResultSet(resultSet);
        resultSet.close();
    }

}
