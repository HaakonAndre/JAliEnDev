package utils;

import java.io.File;
import java.util.StringTokenizer;

/**
 * @author ron
 * @since Oct 7, 2011
 */
public class ExternalCalls {
	
	/**
	 * @param program
	 * @return the full path to the program in env[PATH], or <code>null</code> if it could not be located anywhere
	 */
	public static String programExistsInPath(final String program){
		final StringTokenizer st = new StringTokenizer(System.getenv("PATH"), ":");
		
		while (st.hasMoreTokens()){
			final File dir = new File(st.nextToken());
			
			if (dir.isDirectory() && dir.canRead()){
				final File test = new File(dir, program);
				
				if (test.exists() && test.isFile() && test.canExecute())
					return test.getAbsolutePath();
			}
		}
	
		return null;
	}

}
