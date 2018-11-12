package org.ohdsi.databaseConnector;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Date;

public class BatchedQuery {
	public static int				NUMERIC			= 1;
	public static int				STRING			= 2;
	public static int				DATE			= 3;
	public static int				DATETIME		= 4;
	public static int				FETCH_SIZE		= 2048;
	private static SimpleDateFormat	DATE_FORMAT		= new SimpleDateFormat("yyyy-MM-dd");
	private static SimpleDateFormat	DATETIME_FORMAT	= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private Object[]				columns;
	private int[]					columnTypes;
	private String[]				columnNames;
	private String[]				columnSqlTypes;
	private int						rowCount;
	private int						totalRowCount;
	private int						batchSize;
	private ResultSet				resultSet;
	private Connection				connection;
	private boolean					done;

	private void reserveMemory() {
		System.gc();
		long available = Runtime.getRuntime().maxMemory() - (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
		int bytesPerRow = 0;
		for (int columnIndex = 0; columnIndex < columnTypes.length; columnIndex++)
			if (columnTypes[columnIndex] == NUMERIC)
				bytesPerRow += 8;
			else if (columnTypes[columnIndex] == STRING)
				bytesPerRow += 512;
			else
				bytesPerRow += 24;
		batchSize = (int) Math.round((available / 10d) / (double) bytesPerRow);
		columns = new Object[columnTypes.length];
		for (int columnIndex = 0; columnIndex < columnTypes.length; columnIndex++)
			if (columnTypes[columnIndex] == NUMERIC)
				columns[columnIndex] = new double[batchSize];
			else if (columnTypes[columnIndex] == STRING)
				columns[columnIndex] = new String[batchSize];
			else
				columns[columnIndex] = new String[batchSize];
		rowCount = 0;
	}

	public BatchedQuery(Connection connection, String query) throws SQLException {
		this.connection = connection;
		if (connection.getAutoCommit())
			connection.setAutoCommit(false);
		Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		statement.setFetchSize(FETCH_SIZE);
		resultSet = statement.executeQuery(query);
		resultSet.setFetchSize(FETCH_SIZE);
		ResultSetMetaData metaData = resultSet.getMetaData();
		columnTypes = new int[metaData.getColumnCount()];
		columnSqlTypes = new String[metaData.getColumnCount()];
		for (int columnIndex = 0; columnIndex < metaData.getColumnCount(); columnIndex++) {
			columnSqlTypes[columnIndex] = metaData.getColumnTypeName(columnIndex + 1);
			int type = metaData.getColumnType(columnIndex + 1);
			if (type == Types.BIGINT || type == Types.DECIMAL || type == Types.DOUBLE || type == Types.FLOAT || type == Types.INTEGER || type == Types.NUMERIC
					|| type == Types.REAL || type == Types.SMALLINT || type == Types.TINYINT)
				columnTypes[columnIndex] = NUMERIC;
			else if (type == Types.DATE)
				columnTypes[columnIndex] = DATE;
			else if (type == Types.TIMESTAMP)
				columnTypes[columnIndex] = DATETIME;
			else
				columnTypes[columnIndex] = STRING;
		}
		columnNames = new String[metaData.getColumnCount()];
		for (int columnIndex = 0; columnIndex < metaData.getColumnCount(); columnIndex++)
			columnNames[columnIndex] = metaData.getColumnLabel(columnIndex + 1);
		reserveMemory();
		done = false;
		totalRowCount = 0;
	}

	public void fetchBatch() throws SQLException {
		rowCount = 0;
		while (rowCount < batchSize && resultSet.next()) {
			for (int columnIndex = 0; columnIndex < columnTypes.length; columnIndex++)
				if (columnTypes[columnIndex] == NUMERIC) {
					((double[]) columns[columnIndex])[rowCount] = resultSet.getDouble(columnIndex + 1);
					if (resultSet.wasNull())
						((double[]) columns[columnIndex])[rowCount] = Double.NaN;
				} else if (columnTypes[columnIndex] == STRING)
					((String[]) columns[columnIndex])[rowCount] = resultSet.getString(columnIndex + 1);
				else if (columnTypes[columnIndex] == DATE) {
					Date date = resultSet.getDate(columnIndex + 1);
					if (date == null)
						((String[]) columns[columnIndex])[rowCount] = null;
					else
						((String[]) columns[columnIndex])[rowCount] = DATE_FORMAT.format(date);
				} else {
					Timestamp timestamp = resultSet.getTimestamp(columnIndex + 1);
					if (timestamp == null)
						((String[]) columns[columnIndex])[rowCount] = null;
					else
						((String[]) columns[columnIndex])[rowCount] = DATETIME_FORMAT.format(timestamp);
					
				}
			rowCount++;
		}
		if (rowCount < batchSize) {
			done = true;
			connection.setAutoCommit(true);
		}
		totalRowCount += rowCount;
	}

	public void clear() {
		try {
			resultSet.close();
			columns = null;
			finalize();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	public double[] getNumeric(int columnIndex) {
		double[] column = ((double[]) columns[columnIndex - 1]);
		if (column.length > rowCount) {
			double[] newColumn = new double[rowCount];
			System.arraycopy(column, 0, newColumn, 0, rowCount);
			return newColumn;
		} else
			return column;
	}

	public String[] getString(int columnIndex) {
		String[] column = ((String[]) columns[columnIndex - 1]);
		if (column.length > rowCount) {
			String[] newColumn = new String[rowCount];
			System.arraycopy(column, 0, newColumn, 0, rowCount);
			return newColumn;
		} else
			return column;
	}

	public boolean isDone() {
		return done;
	}

	public boolean isEmpty() {
		return (rowCount == 0);
	}

	public int[] getColumnTypes() {
		return columnTypes;
	}

	public String[] getColumnSqlTypes() {
		return columnSqlTypes;
	}

	public String[] getColumnNames() {
		return columnNames;
	}

	public int getTotalRowCount() {
		return totalRowCount;
	}
}
