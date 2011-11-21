package alien.shell;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import lia.util.process.ExternalProcessBuilder;
import lia.util.process.ExternalProcess.ExitStatus;
import utils.SystemProcess;

/**
 * @author ron
 * @since November 11, 2011
 */
public class FileEditor {

	
	public static final String[] editors = {"edit","vim","emacs","more","less","nano"};
	
	
	
	private static final String vim = which("vim");
	
	private static final String emacs = which("emacs");
	
	private static final String more = which("more");
	
	private static final String less = which("less");
	
	private static final String nano = which("nano");
	
	private String editor;
	
	/**
	 * @param editorname
	 * @throws IOException 
	 */
	public FileEditor(final String editorname) throws IOException{
		if("vim".equals(editorname))
			editor = vim;
		else if("emacs".equals(editorname))
			editor = emacs;
		else if("more".equals(editorname))
			editor = more;
		else if("less".equals(editorname))
			editor = less;
		else if("nano".equals(editorname))
			editor = nano;
		else
			editor = vim;
		
		if(editor==null)
			throw new IOException();
		
	}
	

	/**
	 * @param filename
	 */
	public void edit(final String filename) {

		SystemProcess edit = new SystemProcess(editor + " " + filename);

		edit.execute();

		try {
			edit.wait();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IllegalMonitorStateException e) {
			// ignore
		}
	}
	
	
	
	
	private static String which(String command) {

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
		//System.err.println("Command [" + command + "] not found.");
		return null;

	}

	

}
