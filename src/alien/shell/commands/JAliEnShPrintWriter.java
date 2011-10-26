package alien.shell.commands;

import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;

/**
 * @author ron
 * @since July 15, 2011
 */
public class JAliEnShPrintWriter extends UIPrintWriter{

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils
			.getLogger(JAliEnShPrintWriter.class.getCanonicalName());

	
	/**
	 * error String tag to mark a println for stderr
	 */
	public static final String errTag = String.valueOf((char) 5);
	
	/**
	 * String tag to mark the last line of an output
	 */
	public static String outputterminator = String.valueOf((char) 7);

	/**
	 * String tag to mark the last line of an transaction stream
	 */
	public static String streamend  = String.valueOf((char) 0);
	
	/**
	 * String tag to mark separated fields
	 */
	public static String fieldseparator = String.valueOf((char) 1);
	
	/**
	 * String tag to signal pending action
	 */
	public static String pendingSignal = String.valueOf((char) 9);
	

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
	
	protected void printOutln() {
		printOutln("");
	}
	
	protected void printOutln(String line) {
		print(line + "\n");
	}

	protected void printErrln() {
		printErrln("");
	}
	
	protected void printErrln(String line) {
		print(errTag + line + "\n");
	}
	

	protected void setenv(String cDir, String user, String cDirtiled) {
		print(outputterminator+cDir + fieldseparator + user + fieldseparator + cDirtiled +"\n");
	}
	
	protected void flush(){
		print(streamend + "\n");
	}
	
	protected void pending(){
		print(pendingSignal + "\n");
	}
	
}
