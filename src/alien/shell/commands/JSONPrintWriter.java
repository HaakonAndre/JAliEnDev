package alien.shell.commands;

import java.io.IOException;
import java.io.OutputStream;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import alien.config.ConfigUtils;

public class JSONPrintWriter extends UIPrintWriter {
	
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(JSONPrintWriter.class.getCanonicalName());
	
	/**
	 * error String tag to mark a println for stderr
	 */
	public static final String errTag = "ERR: ";
	
	private final OutputStream os;
	
	private JSONArray jsonArray;
	
	private JSONObject currentResult;
	
	/**
	 * @param os
	 */
	public JSONPrintWriter(final OutputStream os) {
		this.os = os;
		jsonArray = new JSONArray();		
	}

	@Override
	protected void blackwhitemode() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void colourmode() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected boolean colour() {
		// TODO Auto-generated method stub
		return false;
	}

	private void print(final String line) {
		
	}
	
	@Override
	protected void printOut(String line) {
		//
	}

	@Override
	protected void printErr(String line) {
		print(errTag + line);
	}

	@Override
	protected void setenv(String cDir, String user, String cRole) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void flush() {
		nextResult();
		
		try {
			JSONObject replyObject = new JSONObject();
			replyObject.put("document", jsonArray);
			os.write(replyObject.toJSONString().getBytes());
			os.flush();
		} catch (final IOException e) {
			e.printStackTrace();
			logger.log(Level.FINE, "Could not write JSON to client OutputStream", e);
		}

		jsonArray.clear();
	}

	@Override
	protected void pending() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void degraded() {
		// TODO Auto-generated method stub
		
	}

	@Override
	void nextResult() {
		if (currentResult != null) {
			jsonArray.add(currentResult);
			currentResult = null;
		}
	}

	@Override
	void setField(String key, String value) {
		if (currentResult == null)
			currentResult = new JSONObject();

		currentResult.put(key, value);
	}

	@Override
	void setReturnCode(int exitCode, String errorMessage) {
		printErr(errorMessage);
	}
	
	@Override
	protected boolean isRootPrinter() {
		return true;
	}

}
