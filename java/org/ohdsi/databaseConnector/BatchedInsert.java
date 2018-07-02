package org.ohdsi.databaseConnector;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BatchedInsert {
	public static int	INTEGER	= 0;
	public static int	NUMERIC	= 1;
	public static int	STRING	= 2;
	public static int	DATE	= 3;

	private Object[]	columns;
	private int[]		columnTypes;
	private Connection	connection;
	private int			columnCount;
	private int			rowCount;
	private boolean		autoCommit;
	private String		sql;

	public BatchedInsert(Connection connection, String sql, int columnCount) throws SQLException {
		this.connection = connection;
		this.sql = sql;
		this.columnCount = columnCount;
		columns = new Object[columnCount];
		columnTypes = new int[columnCount];
		autoCommit = connection.getAutoCommit();
		if (autoCommit) {
			connection.setAutoCommit(false);
		}
	}

	public void executeBatch() {
		for (int i = 0; i < columnCount; i++) {
			if (columns[i] == null)
				throw new RuntimeException("Column " + (i + 1) + " not set");
			if (columnTypes[i] == INTEGER) {
				if (((int[]) columns[i]).length != rowCount)
					throw new RuntimeException("Column " + (i) + " data not of correct length");
			} else if (columnTypes[i] == NUMERIC) {
				if (((double[]) columns[i]).length != rowCount)
					throw new RuntimeException("Column " + (i) + " data not of correct length");
			} else {
				if (((String[]) columns[i]).length != rowCount)
					throw new RuntimeException("Column " + (i) + " data not of correct length");
			}
		}
		try {
			connection.setAutoCommit(false);
			PreparedStatement statement = connection.prepareStatement(sql);
			for (int i = 0; i < rowCount; i++) {
				for (int j = 0; j < columnCount; j++) {
					if (columnTypes[j] == INTEGER) {
						int value = ((int[]) columns[j])[i];
						if (value == Integer.MIN_VALUE)
							statement.setObject(j + 1, null);
						else
							statement.setInt(j + 1, value);
					} else if (columnTypes[j] == NUMERIC) {
						double value = ((double[]) columns[j])[i];
						if (Double.isNaN(value))
							statement.setObject(j + 1, null);
						else
							statement.setDouble(j + 1, value);
					} else if (columnTypes[j] == DATE) {
						String value = ((String[]) columns[j])[i];
						if (value == null)
							statement.setObject(j + 1, null);
						else
							statement.setDate(j + 1, java.sql.Date.valueOf(value));
					} else {
						String value = ((String[]) columns[j])[i];
						if (value == null)
							statement.setObject(j + 1, null);
						else
							statement.setString(j + 1, value);
					}
				}
				statement.addBatch();
			}
			statement.executeBatch();
			connection.commit();
			statement.close();
			connection.clearWarnings();
		} catch (SQLException e) {
			e.printStackTrace();
			if (e instanceof BatchUpdateException) {
				System.err.println(((BatchUpdateException) e).getNextException().getMessage());
			}
		} finally {
			for (int i = 0; i < columnCount; i++) {
				columns[i] = null;
			}
			rowCount = 0;
		}
	}

	public void setInteger(int columnIndex, int[] column) {
		columns[columnIndex - 1] = column;
		columnTypes[columnIndex - 1] = INTEGER;
		rowCount = column.length;
	}

	public void setNumeric(int columnIndex, double[] column) {
		columns[columnIndex - 1] = column;
		columnTypes[columnIndex - 1] = NUMERIC;
		rowCount = column.length;
	}

	public void setDate(int columnIndex, String[] column) {
		columns[columnIndex - 1] = column;
		columnTypes[columnIndex - 1] = DATE;
		rowCount = column.length;
	}

	public void setString(int columnIndex, String[] column) {
		columns[columnIndex - 1] = column;
		columnTypes[columnIndex - 1] = STRING;
		rowCount = column.length;
	}
}
