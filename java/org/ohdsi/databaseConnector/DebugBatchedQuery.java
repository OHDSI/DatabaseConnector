package org.ohdsi.databaseConnector;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DebugBatchedQuery {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws SecurityException, NoSuchMethodException, IllegalArgumentException, MalformedURLException,
			IllegalAccessException, InvocationTargetException, ClassNotFoundException, SQLException {
		File jarFile = new File("C:\\Users\\mschuemi\\Git\\DatabaseConnector\\inst\\java\\RedshiftJDBC4-1.2.10.1009.jar");
		Method method = URLClassLoader.class.getDeclaredMethod("addURL", new Class[] { URL.class });
		method.setAccessible(true);
		method.invoke(ClassLoader.getSystemClassLoader(), new Object[] { jarFile.toURI().toURL() });

		Class.forName("com.amazon.redshift.jdbc4.Driver");
		String url = "";
		String user = "";
		String password = "";
		Connection connection = DriverManager.getConnection(url, user, password);
		Statement stat = connection.createStatement();
		stat.execute("CREATE TABLE #temp (a DATE, b INT);");
		stat.execute("INSERT INTO #temp (b) VALUES (3);");
		
		BatchedQuery query = new BatchedQuery(connection, "SELECT * FROM #temp;");
		int[] types = query.getColumnTypes();
		if (!query.isDone()) {
			query.fetchBatch();
			String[] dates = query.getString(1);
		}
	}

}
