package org.ohdsi.databaseConnector;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.sql.Date;

public class BatchedQuery {
	public static int				NUMERIC			= 1;
	public static int				STRING			= 2;
	public static int				DATE			= 3;
	public static int				DATETIME		= 4;
	public static int				INTEGER64		= 5;
	public static int				INTEGER			= 6;
	public static int				FETCH_SIZE		= 2048;
	public static double            MAX_BATCH_SIZE  = 1000000;
	public static long              CHECK_MEM_ROWS  = 10000;
	private static String           SPARK           = "spark";
    public static double 			NA_DOUBLE 		= Double.longBitsToDouble(0x7ff00000000007a2L);
    public static int 				NA_INTEGER   	= Integer.MIN_VALUE;
    public static long 				NA_LONG   		= Long.MIN_VALUE;
	
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
	private ByteBuffer				byteBuffer;
	private long                    remainingMemoryThreshold;
	private String                  dbms;
	
	private static double[] convertToInteger64ForR(long[] value, ByteBuffer byteBuffer) {
		double[] result = new double[value.length];
		byteBuffer.clear();
		for (int i = 0; i < value.length; i++)
			byteBuffer.putLong(value[i]);
		byteBuffer.flip();
		for (int i = 0; i < value.length; i++)
			result[i] = byteBuffer.getDouble();
		return result;
	}
	
	public static double[] validateInteger64() {
		long[] values = new long[] { 1, -1, (long) Math.pow(2, 33), (long) Math.pow(-2, 33) };
		return convertToInteger64ForR(values, ByteBuffer.allocate(8 * values.length));
	}
	
	public static double getAvailableHeapSpace() {
		return(getAvailableHeapSpace(true));
	}
	
	private static long getAvailableHeapSpace(boolean collectGarbage) {
		if (collectGarbage)
			System.gc();
		return Runtime.getRuntime().maxMemory() - (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
	}
	
	private void reserveMemory() {
		long availableMemoryAtStart = getAvailableHeapSpace(true);
		// Try to estimate bytes needed per row. Note that we could severely underestimate if data contains very large 
		// strings.
		int bytesPerRow = 8; // Always have the byte buffer
		for (int columnIndex = 0; columnIndex < columnTypes.length; columnIndex++)
			if (columnTypes[columnIndex] == NUMERIC)
				bytesPerRow += 8;
			else if (columnTypes[columnIndex] == INTEGER)
				bytesPerRow += 4;
			else if (columnTypes[columnIndex] == INTEGER64)
				bytesPerRow += 8;
			else if (columnTypes[columnIndex] == DATE)
				bytesPerRow += 4;
			else if (columnTypes[columnIndex] == DATETIME)
				bytesPerRow += 8;
			else // String
				bytesPerRow += 512;
		batchSize = (int) Math.min(MAX_BATCH_SIZE, Math.round((availableMemoryAtStart / 10d) / (double) bytesPerRow));
		remainingMemoryThreshold = Math.max(bytesPerRow * CHECK_MEM_ROWS * 10, availableMemoryAtStart / 10);
		columns = new Object[columnTypes.length];
		for (int columnIndex = 0; columnIndex < columnTypes.length; columnIndex++)
			if (columnTypes[columnIndex] == NUMERIC)
				columns[columnIndex] = new double[batchSize];
			else if (columnTypes[columnIndex] == INTEGER)
				columns[columnIndex] = new int[batchSize];
			else if (columnTypes[columnIndex] == INTEGER64)
				columns[columnIndex] = new long[batchSize];
			else if (columnTypes[columnIndex] == STRING)
				columns[columnIndex] = new String[batchSize];
			else if (columnTypes[columnIndex] == DATE)
				columns[columnIndex] = new int[batchSize];
			else if (columnTypes[columnIndex] == DATETIME)
				columns[columnIndex] = new double[batchSize];
			else
				columns[columnIndex] = new String[batchSize];
		byteBuffer = ByteBuffer.allocate(8 * batchSize);
		rowCount = 0;
	}

	private void cleanUpStrings() {
		for (int columnIndex = 0; columnIndex < columns.length; columnIndex++) 
			if (columnTypes[columnIndex] == STRING) {
				String[] column = ((String[]) columns[columnIndex]);
				for (int i = 0; i < column.length; i++) 
					column[i] = null;
			}
	}
	
	private void trySettingAutoCommit(boolean value) throws SQLException  {
		if (dbms.equals(SPARK))
			return;
		try {
			connection.setAutoCommit(value);
		} catch (SQLFeatureNotSupportedException exception) {
			// Do nothing
		}
	}
	
	public BatchedQuery(Connection connection, String query, String dbms) throws SQLException {
		this.connection = connection;
		this.dbms = dbms;
		trySettingAutoCommit(false);
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
			String className = metaData.getColumnClassName(columnIndex + 1);
			int precision = metaData.getPrecision(columnIndex + 1);
			int scale = metaData.getScale(columnIndex + 1);
			if (type == Types.INTEGER || type == Types.SMALLINT || type == Types.TINYINT 
					|| (dbms.equals("oracle") && className.equals("java.math.BigDecimal") && precision > 0 && precision != 19 && scale == 0))
				columnTypes[columnIndex] = INTEGER;
			else if (type == Types.BIGINT
					|| (dbms.equals("oracle") && className.equals("java.math.BigDecimal") && precision > 0 && scale == 0))
				columnTypes[columnIndex] = INTEGER64;
			else if (type == Types.DECIMAL || type == Types.DOUBLE || type == Types.FLOAT || type == Types.NUMERIC || type == Types.REAL)
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
		cleanUpStrings();
		rowCount = 0;
		while (rowCount < batchSize) {
			if (resultSet.next()) {
				for (int columnIndex = 0; columnIndex < columnTypes.length; columnIndex++)
					if (columnTypes[columnIndex] == NUMERIC) {
						((double[]) columns[columnIndex])[rowCount] = resultSet.getDouble(columnIndex + 1);
						if (resultSet.wasNull())
							((double[]) columns[columnIndex])[rowCount] = NA_DOUBLE;
					} else if (columnTypes[columnIndex] == INTEGER64) {
						((long[]) columns[columnIndex])[rowCount] = resultSet.getLong(columnIndex + 1);
						if (resultSet.wasNull())
							((long[]) columns[columnIndex])[rowCount] = NA_LONG;
					} else if (columnTypes[columnIndex] == INTEGER) {
						((int[]) columns[columnIndex])[rowCount] = resultSet.getInt(columnIndex + 1);
						if (resultSet.wasNull())
							((int[]) columns[columnIndex])[rowCount] = NA_INTEGER;
					} else if (columnTypes[columnIndex] == STRING)
						((String[]) columns[columnIndex])[rowCount] = resultSet.getString(columnIndex + 1);
					else if (columnTypes[columnIndex] == DATE) {
						Date date = resultSet.getDate(columnIndex + 1);
						if (date == null)
							((int[]) columns[columnIndex])[rowCount] = NA_INTEGER;
						else
							((int[]) columns[columnIndex])[rowCount] = (int)date.toLocalDate().toEpochDay();
					} else {
						Timestamp timestamp = resultSet.getTimestamp(columnIndex + 1);
						if (timestamp == null)
							((double[]) columns[columnIndex])[rowCount] = NA_DOUBLE;
						else
							((double[]) columns[columnIndex])[rowCount] = timestamp.getTime() / 1000;

					}
				rowCount++;
				if (rowCount % CHECK_MEM_ROWS == 0) {
					if (getAvailableHeapSpace(false) < remainingMemoryThreshold)
						if (getAvailableHeapSpace(true) < remainingMemoryThreshold)
							break;
				}
			} else {
				done = true;
				trySettingAutoCommit(true);
				break;
			}

		}
		totalRowCount += rowCount;
	}
	
	public void clear() {
		try {
			resultSet.close();
			columns = null;
			byteBuffer = null;
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
	
	public int[] getInteger(int columnIndex) {
		int[] column = ((int[]) columns[columnIndex - 1]);
		if (column.length > rowCount) {
			int[] newColumn = new int[rowCount];
			System.arraycopy(column, 0, newColumn, 0, rowCount);
			return newColumn;
		} else
			return column;
	}
	
	public double[] getInteger64(int columnIndex) {
		long[] column = ((long[]) columns[columnIndex - 1]);
		if (column.length > rowCount) {
			long[] newColumn = new long[rowCount];
			System.arraycopy(column, 0, newColumn, 0, rowCount);
			return convertToInteger64ForR(newColumn, byteBuffer);
		} else
			return convertToInteger64ForR(column, byteBuffer);
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
