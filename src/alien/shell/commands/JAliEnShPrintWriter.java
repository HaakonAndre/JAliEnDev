package alien.shell.commands;

import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;

public class JAliEnShPrintWriter extends UIPrintWriter{

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils
			.getLogger(JAliEnShPrintWriter.class.getCanonicalName());

	public static final String errTag = ":::STDERR:";
	
	public static final String lastLineTag = ":::LINEFLUSH:";

	private OutputStream os;

	JAliEnShPrintWriter(OutputStream os) {
		this.os = os;
	}

	private void print(String line) {
		try {
			os.write(line.getBytes());
			os.flush();
		} catch (IOException e) {
			e.printStackTrace();
			logger.log(Level.FINE, "Could not write to OutputStream" + line, e);
		}
	}

	protected void printOutln(String line) {
		print(line + "\n");
	}

	protected void printErrln(String line) {
		print(errTag + line + "\n");
	}
	
	protected void flush(){
		print(lastLineTag+"\n");
	}
}
