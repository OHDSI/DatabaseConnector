package org.ohdsi.databaseConnector;

public class DebugBatchedQuery {

	public static void main(String[] args) throws Exception {

//		Compression.createZipFile(new String[] {"c:/temp/skeleton2"}, "c:/temp", "c:/temp/data2.zip", 9);
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
		
		
//		File jarFile = new File("C:\\Users\\mschuemi\\jdbcDrivers\\ojdbc8.jar");
//		Method method = URLClassLoader.class.getDeclaredMethod("addURL", new Class[] { URL.class });
//		method.setAccessible(true);
//		method.invoke(ClassLoader.getSystemClassLoader(), new Object[] { jarFile.toURI().toURL() });
//
//		Class.forName("oracle.jdbc.driver.OracleDriver");
//		String url = "jdbc:oracle:thin:@";
//		String user = "";
//		String password = "";
//		Connection connection = DriverManager.getConnection(url, user, password);
//		
//		BatchedQuery query = new BatchedQuery(connection, "SELECT * FROM ohdsi.temp", "oracle");	
	}

}
