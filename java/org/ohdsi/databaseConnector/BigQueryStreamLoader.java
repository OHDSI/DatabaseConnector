package org.ohdsi.databaseConnector;

import com.google.api.client.repackaged.com.google.common.base.Splitter;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;

public class BigQueryStreamLoader {

    private static List<String> BIGQUARY_JDBC_DRIVER_CLASSES = Stream.of(
            "com.simba.googlebigquery.jdbc.jdbc42.S42ConnectionHandle",
            "com.simba.googlebigquery.jdbc.jdbc42.S42Connection",
            "com.simba.googlebigquery.jdbc.jdbc42.S41ConnectionHandle",
            "com.simba.googlebigquery.jdbc.jdbc41.S41Connection")
            .collect(Collectors.toList());
    private int ROW_LIMIT_PER_SAVE = 10000;

    private BigQuery bigQuery;

    public static boolean isStreamLoadAvailable(Connection connection) {

        return BIGQUARY_JDBC_DRIVER_CLASSES.contains(connection.getClass().getName());
    }

    public BigQueryStreamLoader() {

    }

    public void init(Connection connection) {

        try {
            String jdbcConnectionString = connection.getMetaData().getURL();
            List<String> params = Splitter.on(";").splitToList(jdbcConnectionString);
            bigQuery = getBigQuery(
                    getParamByName(params, "ProjectId"),
                    getParamByName(params, "OAuthPvtKeyPath")
            );
        } catch (Exception e) {
            throw new RuntimeException("Cannot initialize bigquery stream loader", e);
        }
    }

    public void load(String sql, List<List<Object>> raws) {

        try {
            Statement parse = CCJSqlParserUtil.parse(sql);
            String schema;
            String table;
            List<String> columns;
            if (parse instanceof Insert) {
                Insert insert = (Insert) parse;
                schema = insert.getTable().getSchemaName();
                table = insert.getTable().getName();
                columns = insert.getColumns().stream().map(Column::getColumnName).collect(Collectors.toList());
            } else if (parse instanceof Update) {
                Update update = (Update) parse;
                schema = update.getTable().getSchemaName();
                table = update.getTable().getName();
                columns = update.getColumns().stream().map(Column::getColumnName).collect(Collectors.toList());
            } else {
                throw new RuntimeException("Cannot use stream to batch operation. Only insert and update are supported.");
            }

            load(schema, table, columns, raws);

        } catch (Exception e) {
            throw new RuntimeException("Cannot use stream to batch operation", e);
        }
    }

    private void load(String datasetName, String tableName, List<String> columnNames, List<List<Object>> rows) {

        TableId tableId = TableId.of(datasetName, tableName);

        List<List<List<Object>>> rowsPartitionsByLimit = Lists.partition(rows, ROW_LIMIT_PER_SAVE);

        rowsPartitionsByLimit.forEach(rowPartition -> {
            InsertAllRequest.Builder insertAllRequestBuilder = InsertAllRequest.newBuilder(tableId);
            rowPartition.forEach(row -> {
                Map<String, Object> rowContent = new HashMap<>();
                for (int columnIndex = 0; columnIndex < columnNames.size(); columnIndex++) {
                    String columnName = columnNames.get(columnIndex);
                    Object value = row.get(columnIndex);
                    rowContent.put(columnName, value);
                }
                insertAllRequestBuilder.addRow(rowContent);
            });

            InsertAllResponse response = bigQuery.insertAll(insertAllRequestBuilder.build());

            if (response.hasErrors()) {
                String exceptionMessages = response.getInsertErrors().entrySet().stream()
                        .map(entry -> String.format("%s:%s", entry.getKey(), entry.getValue()))
                        .collect(Collectors.joining(";"));
                throw new RuntimeException(exceptionMessages);
            }
        });

    }

    private BigQuery getBigQuery(String projectId,String keyPath) throws FileNotFoundException {

        File initialFile = new File(keyPath);
        InputStream jsonStream = new FileInputStream(initialFile);

        return RemoteBigQueryHelper
                .create(projectId, jsonStream)
                .getOptions()
                .getService();
    }

    private String getParamByName(List<String> params, String paramName) {

        return params.stream()
                .filter(str -> startsWithIgnoreCase(str, paramName))
                .map(str -> replaceFirstIgnoreCase(str, paramName))
                .map(String::trim)
                .map(str -> replaceFirstIgnoreCase(str, "="))
                .map(String::trim)
                .findFirst()
                .orElseThrow(() -> new RuntimeException(String.format("Cannot get %s params from jdbc connection string", paramName)));
    }

    private String replaceFirstIgnoreCase(String str, String prefix) {

        if (!startsWithIgnoreCase(str, prefix)) {
            return str;
        }
        return str.substring(prefix.length());
    }

    private boolean startsWithIgnoreCase(String str, String prefix) {

        if (Objects.equals(str, prefix)) {
            return true;
        }

        if (str == null || prefix == null) {
            return false;
        }
        String strUpper = str.toUpperCase();
        String prefixUpper = prefix.toUpperCase();
        return strUpper.startsWith(prefixUpper);
    }

}