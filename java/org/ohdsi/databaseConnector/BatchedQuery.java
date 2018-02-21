package org.ohdsi.databaseConnector;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Date;

public class BatchedQuery {
	public static int				NUMERIC		= 1;
	public static int				STRING		= 2;
	public static int				DATE		= 3;
	public static int				FETCH_SIZE	= 2048;
	private static SimpleDateFormat	DATE_FORMAT	= new SimpleDateFormat("yyyy-MM-dd");

	private Object[]				columns;
	private int[]					columnTypes;
	private String[]				columnNames;
	private int						rowCount;
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
		for (int columnIndex = 0; columnIndex < metaData.getColumnCount(); columnIndex++) {
			int type = metaData.getColumnType(columnIndex + 1);
			if (type == Types.BIGINT || type == Types.DECIMAL || type == Types.DOUBLE || type == Types.FLOAT || type == Types.INTEGER || type == Types.NUMERIC
					|| type == Types.REAL || type == Types.SMALLINT || type == Types.TINYINT)
				columnTypes[columnIndex] = NUMERIC;
			else if (type == Types.DATE)
				columnTypes[columnIndex] = DATE;
			else
				columnTypes[columnIndex] = STRING;
		}
		columnNames = new String[metaData.getColumnCount()];
		for (int columnIndex = 0; columnIndex < metaData.getColumnCount(); columnIndex++)
			columnNames[columnIndex] = metaData.getColumnLabel(columnIndex + 1);
		reserveMemory();
		done = false;
	}

	public void fetchBatch() throws SQLException {
		rowCount = 0;
		while (rowCount < batchSize && resultSet.next()) {
			for (int columnIndex = 0; columnIndex < columnTypes.length; columnIndex++)
				if (columnTypes[columnIndex] == NUMERIC)
					((double[]) columns[columnIndex])[rowCount] = resultSet.getDouble(columnIndex + 1);
				else if (columnTypes[columnIndex] == STRING)
					((String[]) columns[columnIndex])[rowCount] = resultSet.getString(columnIndex + 1);
				else {
					Date date = resultSet.getDate(columnIndex + 1);
					if (date == null)
						((String[]) columns[columnIndex])[rowCount] = null;
					else
						((String[]) columns[columnIndex])[rowCount] = DATE_FORMAT.format(date);
				}
			rowCount++;
		}
		if (rowCount < batchSize) {
			done = true;
			connection.setAutoCommit(true);
		}
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

	public String[] getColumnNames() {
		return columnNames;
	}
}
