package utils;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author ron
 * @since Oct 7, 2011
 */
public class ExternalCalls {
	
	/**
	 * @param program
	 * @return if program is in env[PATH]
	 */
	public static boolean programExistsInPath(final String program){
	
		final StringTokenizer st = new StringTokenizer(
			System.getenv("PATH"), ":");
		
		while (st.hasMoreTokens()){

			final File dir = new File(st.nextToken());
			
			if(dir.isDirectory())
				for(String f: dir.list())
					if(program.equals(f))
						return true;
				
			else
				if(program.equals(dir.getName()))
					return true;
		}
	
		return false;
	}

}
