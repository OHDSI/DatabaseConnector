package org.ohdsi.databaseConnector;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataLoader {

    private int ROW_LIMIT_PER_SAVE_FOR_BATCH = 1000;
    private int ROW_LIMIT_PER_SAVE_FOR_INSERT = 100;

    private static List<String> BIGQUARY_JDBC_DRIVER_CLASSES = Stream.of(
            "com.simba.googlebigquery.jdbc.jdbc42.S42ConnectionHandle",
            "com.simba.googlebigquery.jdbc.jdbc42.S42Connection",
            "com.simba.googlebigquery.jdbc.jdbc42.S41ConnectionHandle",
            "com.simba.googlebigquery.jdbc.jdbc41.S41Connection")
            .collect(Collectors.toList());

    private int columnCount;
    private Connection connection;

    public DataLoader(int columnCount, Connection connection) {

        this.columnCount = columnCount;
        this.connection = connection;
    }

    public void load(String sql, List<List<Object>> rows) throws SQLException {


        if (isBatchAvailable()) {
            List<List<List<Object>>> rowsPartitionsByLimit = Lists.partition(rows, ROW_LIMIT_PER_SAVE_FOR_BATCH);
            for (List<List<Object>> rowPartition : rowsPartitionsByLimit) {
                batchLoad(sql, rowPartition);
            }
        } else {
            List<List<List<Object>>> rowsPartitionsByLimit = Lists.partition(rows, ROW_LIMIT_PER_SAVE_FOR_INSERT);
            for (List<List<Object>> rowPartition : rowsPartitionsByLimit) {
                multiValueLoad(sql, rowPartition);
            }
        }
    }

    protected boolean isBatchAvailable() {

        return !BIGQUARY_JDBC_DRIVER_CLASSES.contains(connection.getClass().getName());
    }

    private void batchLoad(String sql, List<List<Object>> rows) throws SQLException {

        PreparedStatement statement = connection.prepareStatement(sql);

        for (int i = 0; i < rows.size(); i++) {
            List<Object> row = rows.get(i);
            for (int j = 0; j < row.size(); j++) {
                statement.setObject(j + 1, row.get(j));
            }
            statement.addBatch();
        }

        statement.executeBatch();
        connection.commit();
    }

    /**
     * Not all drivers support batch operations, for example GoogleBigQueryJDBC42.jar.
     * In order to save data most  efficiently, we implement saving through an insert with multiple values.
     * @param sql sql
     * @param rows rows
     * @throws SQLException
     */
    private void multiValueLoad(String sql, List<List<Object>> rows) throws SQLException {

        //create insert query by adding multiply values like insert values (?,?),(?,?),(?,?)
        String params = String.join(",", Collections.nCopies(columnCount, "?"));
        String sqlWithValues = sql + Strings.repeat(String.format(", (%s)", params), rows.size() - 1);

        PreparedStatement statement = connection.prepareStatement(sqlWithValues);

        for (int i = 0; i < rows.size(); i++) {
            List<Object> row = rows.get(i);
            for (int j = 0; j < columnCount; j++) {
                int base = columnCount * i;
                int position = base + j + 1;
                statement.setObject(position, row.get(j));
            }
        }
        statement.executeUpdate();
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
        statement.close();
    }

}