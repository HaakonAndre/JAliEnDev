package alien.test.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;


import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcessBuilder;

/**
 * @author ron
 * @since Sep 09, 2011
 */
public class Functions {

	/**
	 * @param commands
	 * @param verbose 
	 * @return success of the shell executions
	 */
	public static boolean execShell(final ArrayList<TestCommand> commands,
			boolean verbose) {
		boolean state = true;
		for (TestCommand c : commands) {
			if (verbose)
				c.verbose();
			state = c.exec() && state;
			if (!state)
				break;
		}
		return state;
	}

	/**
	 * @param dir
	 * @return if it could create the directory
	 */
	public static boolean makeDirs(String dir) {

		if (!(new File(dir).mkdirs())) {
			System.out.println("Could not create directory: " + dir);
			return false;
		}
		return true;
	}

	/**
	 * @param s
	 *            command, single space separated
	 * @return stdout of executed command
	 */
	public static String callGetStdOut(String s) {
		return callGetStdOut(s, false);
	}

	/**
	 * @param s
	 *            command, single space separated
	 * @param verbose
	 * @return stdout of executed command
	 */
	public static String callGetStdOut(String s, boolean verbose) {
		TestCommand c = new TestCommand(s);
		if (verbose)
			c.verbose();
		c.exec();
		return c.getStdOut();
	}
	

	/**
	 * @param s
	 *            command, single space separated
	 * @param verbose
	 * @return stdout of executed command
	 */
	public static String callGetStdOut(String[] s) {
		
		TestCommand c = new TestCommand(s);
		c.exec();
		return c.getStdOut();
	}

	/**
	 * @param fFileName
	 * @return file content
	 * @throws Exception 
	 */
	public static String getFileContent(String fFileName) throws Exception{

		StringBuilder text = new StringBuilder();
		String NL = System.getProperty("line.separator");
		Scanner scanner = new Scanner(new FileInputStream(fFileName));
		try {
			while (scanner.hasNextLine()) {
				text.append(scanner.nextLine() + NL);
			}
		} finally {
			scanner.close();
		}
		return text.toString();
	}
	
	
	/**
	 * Write String[] as a file, with String[0] as file name and String[1] as file content
	 * @param file
	 * @throws Exception
	 */
	public static void writeOutFile(String[] file) throws Exception {
		writeOutFile(file[0],file[1]);
	}
	
	/**
	 * @param file_name
	 * @param content
	 * @throws Exception
	 */
	public static void writeOutFile(String file_name, String content) throws Exception {
		 Writer out = new OutputStreamWriter(new FileOutputStream(file_name));
		    try {
		      out.write(content);
		    }
		    finally {
		      out.close();
		    }
	}
	

	/**
	 * @param command
	 * @return full location of the command
	 */
	public static String which(String command) {

		final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(
				Arrays.asList("which", command));

		pBuilder.returnOutputOnExit(true);

		pBuilder.timeout(7, TimeUnit.SECONDS);
		try {
			final ExitStatus exitStatus = pBuilder.start().waitFor();

			if (exitStatus.getExtProcExitStatus() == 0)
				return exitStatus.getStdOut().trim();

		} catch (Exception e) {
			// ignore
		}
		System.err.println("Command [" + command + "] not found.");
		return null;

	}

	
	/**
	 * @param zip
	 * @param extractTo
	 * @throws IOException
	 */
	public static final void unzip(File zip, File extractTo) throws IOException {
		
	    ZipFile archive = new ZipFile(zip);
	    Enumeration<? extends ZipEntry> e = archive.entries();
	    while (e.hasMoreElements()) {
	      ZipEntry entry = (ZipEntry) e.nextElement();
	      File file = new File(extractTo, entry.getName());
	      if (entry.isDirectory() && !file.exists()) {
	        file.mkdirs();
	      } else {
	        if (!file.getParentFile().exists()) {
	          file.getParentFile().mkdirs();
	        }

	        InputStream in = archive.getInputStream(entry);
	        BufferedOutputStream out = new BufferedOutputStream(
	            new FileOutputStream(file));

	        byte[] buffer = new byte[8192];
	        int read;

	        while (-1 != (read = in.read(buffer))) {
	          out.write(buffer, 0, read);
	        }

	        in.close();
	        out.close();
	      }
	    }
	  }
	
	
}
