package alien.shell.commands;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import lazyj.Format;


/**
 * @author costing
 * @since Mar 27 2014
 */
public class XMLPrintWriter extends UIPrintWriter {
	
	/**
	 * OutputSteam that will contain the information in Root format
	 */
	private OutputStream os;

	/**
	 * @param os OutputSteam that will contain the information in Root format
	 */
	public XMLPrintWriter(final OutputStream os) {
		this.os = os;
	}
	
	private final Map<String, String> metaInfo = new TreeMap<>();
	
	private final List<Map<String, String>> results = new ArrayList<>();
	
	private Map<String, String> currentResult = null;
	
	@Override
	protected void blackwhitemode() {
		// nothing
	}

	@Override
	protected void colourmode() {
		// nothing
	}

	@Override
	protected boolean colour() {
		return false;
	}

	@Override
	protected void printOut(String line) {
		// nothing
	}

	@Override
	protected void printErr(String line) {
		// nothing
	}

	@Override
	protected void setenv(final String cDir, final String user, final String cRole) {
		metaInfo.put("pwd", cDir);
		metaInfo.put("user", user);
		metaInfo.put("role", cRole);
	}

	@Override
	protected void flush() {
		nextResult();
		
		final PrintWriter pw = new PrintWriter(os);
		
		pw.print("<document");
		dumpMap(pw, metaInfo);
		pw.println(">");
		
		for (final Map<String, String> result: results){
			pw.print("<result");
			dumpMap(pw, result);
			pw.println("/>");
		}
		
		pw.println("</document>");
		
		pw.flush();
	}
	
	private static void dumpMap(final PrintWriter pw, final Map<String, String> result) {
		for (final Map.Entry<String, String> entry : result.entrySet()) {
			pw.print(" " + entry.getKey() + "=\"" + Format.escHtml(entry.getValue()) + "\"");
		}
	}

	@Override
	protected void pending() {
		// nothing
	}

	@Override
	protected void degraded() {
		// nothing
	}
	
	@Override
	protected boolean isRootPrinter(){
		return true;
	}

	@Override
	void nextResult() {
		if (currentResult!=null){
			results.add(currentResult);
			currentResult = null;
		}
	}

	@Override
	void setField(final String key, final String value) {
		if (currentResult==null){
			currentResult = new TreeMap<>();
		}
		
		currentResult.put(key, value);
	}
}
