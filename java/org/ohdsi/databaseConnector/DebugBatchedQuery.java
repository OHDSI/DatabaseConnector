package org.ohdsi.databaseConnector;

import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.sql.SQLException;

public class DebugBatchedQuery {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws SecurityException, NoSuchMethodException, IllegalArgumentException, MalformedURLException,
			IllegalAccessException, InvocationTargetException, ClassNotFoundException, SQLException {

		Compression.createZipFile(new String[] {"c:/temp/vignetteFeatureExtraction"}, "c:/temp", "c:/temp/data2.zip", 9);
//		File jarFile = new File("C:\\Users\\mschuemi\\Git\\DatabaseConnector\\inst\\java\\RedshiftJDBC4-1.2.10.1009.jar");
//		Method method = URLClassLoader.class.getDeclaredMethod("addURL", new Class[] { URL.class });
//		method.setAccessible(true);
//		method.invoke(ClassLoader.getSystemClassLoader(), new Object[] { jarFile.toURI().toURL() });
//
//		Class.forName("com.amazon.redshift.jdbc4.Driver");
//		String url = "";
//		String user = "";
//		String password = "";
//		Connection connection = DriverManager.getConnection(url, user, password);
//		Statement stat = connection.createStatement();
//		stat.execute("CREATE TABLE #temp (a DATE, b INT);");
//		stat.execute("INSERT INTO #temp (b) VALUES (3);");
//		
//		BatchedQuery query = new BatchedQuery(connection, "SELECT * FROM #temp;");
//		int[] types = query.getColumnTypes();
//		if (!query.isDone()) {
//			query.fetchBatch();
//			String[] dates = query.getString(1);
//		}
	}

}
