package org.ohdsi.databaseConnector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class Compression {

	/**
	 * 
	 * @param fileName
	 *            Absolute path to a single file or folder to compress.
	 * @param folder
	 *            The root folder. All files will be stored with relative paths relative to this folder.
	 * @param zipFileName
	 *            Absolute path to the zip file to be created.
	 * @param compressionLevel
	 *            A value between 1 and 9 (inclusive).
	 */
	public static void createZipFile(String fileName, String folder, String zipFileName, int compressionLevel) {
			createZipFile(new String[] {fileName}, folder, zipFileName, compressionLevel);
	}
	
	/**
	 * 
	 * @param fileNames
	 *            Absolute paths to files to compress (can include folders).
	 * @param folder
	 *            The root folder. All files will be stored with relative paths relative to this folder.
	 * @param zipFileName
	 *            Absolute path to the zip file to be created.
	 * @param compressionLevel
	 *            A value between 0 and 9 (inclusive).
	 */
	public static void createZipFile(String[] fileNames, String folder, String zipFileName, int compressionLevel) {
		try {
			folder = normalizeName(folder);

			// Open file output stream:
			FileOutputStream out = new FileOutputStream(zipFileName);

			// Zip files and folders:
			ZipOutputStream zipOut = new ZipOutputStream(out);
			String skip = new File(zipFileName).getCanonicalPath().toLowerCase();
			for (String fileName : fileNames) {
				fileName = normalizeName(fileName);
				addFile(fileName, folder, zipOut, skip);
			}
			// Close streams:
			zipOut.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static String normalizeName(String name) {
		return name.replaceAll("\\\\", "/");
	}

	private static void addFile(String fileName, String rootFolder, ZipOutputStream zipOut, String skip) {
		try {
			File file = new File(fileName);
			if (file.getCanonicalPath().toLowerCase().equals(skip))
				return;
			String name = fileName.replace(rootFolder + "/", "");
			if (file.isDirectory()) {
				if (!fileName.equals(rootFolder)) {
					zipOut.putNextEntry(new ZipEntry(name + "/"));
					zipOut.closeEntry();
				}
				for (File subFile : file.listFiles()) {
					String subFileName = subFile.getAbsolutePath();
					subFileName = normalizeName(subFileName);
					addFile(subFileName, rootFolder, zipOut, skip);
				}
			} else if (file.isFile()) {
				zipOut.putNextEntry(new ZipEntry(name));
				copyStream(new FileInputStream(file), zipOut);
				zipOut.closeEntry();

			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void copyStream(InputStream in, OutputStream out) {
		int bufferSize = 1024;
		int bytes;
		byte[] buffer;
		buffer = new byte[bufferSize];
		try {
			while ((bytes = in.read(buffer)) != -1) {
				if (bytes == 0) {
					bytes = in.read();
					if (bytes < 0)
						break;
					out.write(bytes);
					out.flush();
					continue;
				}
				out.write(buffer, 0, bytes);
				out.flush();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
