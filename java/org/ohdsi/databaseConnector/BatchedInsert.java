package org.ohdsi.databaseConnector;

import static org.ohdsi.databaseConnector.BatchColumnType.*;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class BatchedInsert {

	private Object[]	columns;
	private int[]		columnTypes;
	private Connection	connection;
	private int			columnCount;
	private int			rowCount;
	private String		sql;

	public BatchedInsert(Connection connection, String sql, int columnCount) throws SQLException {
		this.connection = connection;
		this.sql = sql;
		this.columnCount = columnCount;
		columns = new Object[columnCount];
		columnTypes = new int[columnCount];
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

			trySettingAutoCommit(connection, false);

			DataLoader dataLoader = new DataLoader(columnCount, rowCount, connection, columns, columnTypes);
			dataLoader.load(sql);

			connection.clearWarnings();
			trySettingAutoCommit(connection, true);
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




	private void trySettingAutoCommit(Connection connection, boolean value) throws SQLException {
		try {
			connection.setAutoCommit(value);
		} catch (SQLFeatureNotSupportedException exception) {
			// Do nothing
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
	
	public void setDateTime(int columnIndex, String[] column) {
		columns[columnIndex - 1] = column;
		columnTypes[columnIndex - 1] = DATETIME;
		rowCount = column.length;
	}

	public void setBigint(int columnIndex, long[] column) {
		columns[columnIndex - 1] = column;
		columnTypes[columnIndex - 1] = BIGINT;
		rowCount = column.length;
	}

	public void setString(int columnIndex, String[] column) {
		columns[columnIndex - 1] = column;
		columnTypes[columnIndex - 1] = STRING;
		rowCount = column.length;
	}

	public void setInteger(int columnIndex, int column) {
		columns[columnIndex - 1] = new int[] { column };
		columnTypes[columnIndex - 1] = INTEGER;
		rowCount = 1;
	}

	public void setNumeric(int columnIndex, double column) {
		columns[columnIndex - 1] = new double[] { column };
		columnTypes[columnIndex - 1] = NUMERIC;
		rowCount = 1;
	}

	public void setDate(int columnIndex, String column) {
		columns[columnIndex - 1] = new String[] { column };
		columnTypes[columnIndex - 1] = DATE;
		rowCount = 1;
	}
	
	public void setDateTime(int columnIndex, String column) {
		columns[columnIndex - 1] = new String[] { column };
		columnTypes[columnIndex - 1] = DATETIME;
		rowCount = 1;
	}

	public void setString(int columnIndex, String column) {
		columns[columnIndex - 1] = new String[] { column };
		columnTypes[columnIndex - 1] = STRING;
		rowCount = 1;
	}

	public void setBigint(int columnIndex, long column) {
		columns[columnIndex - 1] = new long[] { column };
		columnTypes[columnIndex - 1] = BIGINT;
		rowCount = 1;
	}
}
