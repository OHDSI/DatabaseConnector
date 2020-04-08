package org.ohdsi.databaseConnector;

import static org.ohdsi.databaseConnector.BatchColumnType.BIGINT;
import static org.ohdsi.databaseConnector.BatchColumnType.DATE;
import static org.ohdsi.databaseConnector.BatchColumnType.DATETIME;
import static org.ohdsi.databaseConnector.BatchColumnType.INTEGER;
import static org.ohdsi.databaseConnector.BatchColumnType.NUMERIC;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataLoader {

    private static List<String> BIGQUARY_JDBC_DRIVER_CLASSES = Stream.of(
            "com.simba.googlebigquery.jdbc.jdbc42.S42ConnectionHandle",
            "com.simba.googlebigquery.jdbc.jdbc42.S42Connection",
            "com.simba.googlebigquery.jdbc.jdbc42.S41ConnectionHandle",
            "com.simba.googlebigquery.jdbc.jdbc41.S41Connection")
            .collect(Collectors.toList());

    private int columnCount;
    private int rowCount;
    private Connection connection;
    private Object[] columns;
    private int[] columnTypes;

    public DataLoader(int columnCount, int rowCount, Connection connection, Object[] columns, int[] columnTypes) {

        this.columnCount = columnCount;
        this.rowCount = rowCount;
        this.connection = connection;
        this.columns = columns;
        this.columnTypes = columnTypes;
    }

    public void load(String sql) throws SQLException {

        if (isBatchAvailable()) {
            batchLoad(sql);
        } else {
            multiValueLoad(sql);
        }
    }


    private void batchLoad(String sql) throws SQLException {

        PreparedStatement statement = connection.prepareStatement(sql);
        for (int i = 0; i <= rowCount; i++) {
            for (int j = 0; j < columnCount; j++) {
                Object value = getValue(i, j);
                statement.setObject(j + 1, value);
            }
            statement.addBatch();
        }

        statement.executeBatch();
        connection.commit();
    }

    /**
     * Not all drivers support batch operations, for example GoogleBigQueryJDBC42.jar.
     * In order to save data most  efficiently, we implement saving through an insert with multiple values.
     *
     * @param sql       sql
     * @throws SQLException
     */
    private void multiValueLoad(String sql) throws SQLException {

        //create insert query by adding multiply values like insert values (?,?),(?,?),(?,?)
        String params = String.join(",", Collections.nCopies(columnCount, "?"));
        String sqlWithValues = sql + Collections.nCopies(rowCount - 1, String.format(", (%s)", params)).stream().collect(Collectors.joining());

        PreparedStatement statement = connection.prepareStatement(sqlWithValues);
        for (int i = 0; i <= rowCount; i++) {
            for (int j = 0; j < columnCount; j++) {
                int base = columnCount * i;
                int position = base + j + 1;
                Object value = getValue(i, j);
                statement.setObject(position, value);
            }
        }
        statement.executeUpdate();
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
        statement.close();
    }


    protected boolean isBatchAvailable() {

        return !BIGQUARY_JDBC_DRIVER_CLASSES.contains(connection.getClass().getName());
    }

    private Object getValue(int rowIndex, int columnIndex) {

        if (columnTypes[columnIndex] == INTEGER) {
            int value = ((int[]) columns[columnIndex])[rowIndex];
            if (value == Integer.MIN_VALUE) return null;
            return value;
        } else if (columnTypes[columnIndex] == NUMERIC) {
            double value = ((double[]) columns[columnIndex])[rowIndex];
            if (Double.isNaN(value)) return null;
            return value;
        } else if (columnTypes[columnIndex] == DATE) {
            String value = ((String[]) columns[columnIndex])[rowIndex];
            return java.sql.Date.valueOf(value);
        } else if (columnTypes[columnIndex] == DATETIME) {
            String value = ((String[]) columns[columnIndex])[rowIndex];
            return java.sql.Timestamp.valueOf(value);
        } else if (columnTypes[columnIndex] == BIGINT) {
            long value = ((long[]) columns[columnIndex])[rowIndex];
            if (value == Long.MIN_VALUE) return null;
            return value;
        } else {
            String value = ((String[]) columns[columnIndex])[rowIndex];
            return value;
        }
    }

}