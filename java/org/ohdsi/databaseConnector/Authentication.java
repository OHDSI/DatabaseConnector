package org.ohdsi.databaseConnector;

import java.io.File;
import java.io.IOException;

public class Authentication {

	public static void addPathToJavaLibrary(String path) throws IOException {
		File dir = new File(path);

		if (!dir.exists() || !dir.isDirectory()) {
			throw new IOException("Provided path is not a valid directory: " + path);
		}

		// Search for the SQL Server auth DLL (matches sqljdbc_auth.dll or mssql-jdbc_auth-*.dll)
		File[] dlls = dir.listFiles((d, name) -> {
			String lowerName = name.toLowerCase();
			return ((lowerName.contains("mssql") && lowerName.contains("auth")) || lowerName.startsWith("sqljdbc42")) &&
					lowerName.endsWith(".dll");
		});

		if (dlls == null || dlls.length == 0) {
			throw new IOException("No authentication DLL found in directory: " + path);
		}

		// Assuming there is only one relevant DLL in the folder, or we try loading the first one we find
		try {
			System.load(dlls[0].getAbsolutePath());
		} catch (UnsatisfiedLinkError e) {
			throw new IOException("Failed to load native library from: " + dlls[0].getAbsolutePath(), e);
		}
	}
}