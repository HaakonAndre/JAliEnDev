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
public class JShPrintWriter extends UIPrintWriter{

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils
			.getLogger(JShPrintWriter.class.getCanonicalName());

	
	/**
	 * 
	 */
	public static final String degradedSignal = String.valueOf((char) 25);
	
	
	/**
	 * 
	 */
	public static final String lineTerm = String.valueOf((char) 0);
	/**
	 * 
	 */
	public static final String SpaceSep = String.valueOf((char) 1);
	
	/**
	 * 
	 */
	public static final String pendSignal = String.valueOf((char) 9);
	
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
	


	/**
	 * marker for -Colour argument
	 */
	protected boolean bColour = true;
	
	@Override
	protected void blackwhitemode(){
		bColour = false;
	}
	
	@Override
	protected void colourmode(){
		bColour = true;
	}
	
	/**
	 * color status
	 * @return state of the color mode
	 */
	@Override
	protected boolean colour(){
		return bColour;
	}
	
	
	private OutputStream os;

	/**
	 * @param os
	 */
	public JShPrintWriter(final OutputStream os) {
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
	
	@Override
	protected void printOut(final String line) {
		print(line);
	}
	
	@Override
	protected void printErr(final String line) {
		print(errTag + line);
	}
	
	@Override
	protected void setenv(String cDir, String user, String cRole) {
		print(outputterminator+cDir + fieldseparator + user + fieldseparator + cRole +"\n");
	}
	
	@Override
	protected void flush(){
		print(streamend + "\n");
	}
	
	@Override
	protected void pending(){
		print(pendingSignal + "\n");
	}

	
	@Override
	protected void degraded(){
		print(degradedSignal);
	}

	@Override
	void nextResult() {
		// ignored
	}

	@Override
	void setField(String key, String value) {
		// ignored
	}

	@Override
	void setReturnCode(final int exitCode, final String errorMessage) {
		printErr(errorMessage);
	}
	
}
