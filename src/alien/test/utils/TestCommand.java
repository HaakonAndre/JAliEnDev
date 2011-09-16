package alien.test.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import lia.util.process.ExternalProcessBuilder;
import lia.util.process.ExternalProcess.ExitStatus;

/**
 * @author ron
 * @since Sep 16, 2011
 */
public class TestCommand {

	private String stderr = "";

	private String stdout = "";

	private boolean verbose = false;

	private ArrayList<String> command = new ArrayList<String>();

	public TestCommand() {

	}

	public TestCommand(ArrayList<String> command) {
		for (String c : command)
			this.command.add(c.trim());
	}

	public TestCommand(String[] command) {
		for (String c : command)
			this.command.add(c.trim());
	}

	/**
	 * @param command
	 */
	public TestCommand(String command) {
		for (String c : command.split(" "))
			this.command.add(c);
	}
	
	/**
	 * @param comm
	 */
	public void addCommand(String comm) {
		command.add(comm.trim());
	}

	/**
	 * @return command
	 */
	public String getCommand() {
		String out = "";
		for (String c : command)
			out += c + " ";
		return out.trim();
	}
	
	/**
	 * @return stdout
	 */
	public String getStdOut(){
		return stdout;
	}

	/**
	 * @return stderr
	 */
	public String getStdErr(){
		return stderr;
	}
	
	/**
	 * set verbose
	 */
	public void verbose(){
		verbose = true;
	}
	
	/**
	 * @return success of the shell execution
	 */
	public boolean exec() {
	
		if(verbose)
			System.out.println("EXEC: " + getCommand());
	
		final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(
				new ArrayList<String>(command));
		
		pBuilder.returnOutputOnExit(true);
		
		// pBuilder.directory();

		pBuilder.timeout(10, TimeUnit.MINUTES);

		pBuilder.redirectErrorStream(true);

		try {
			final ExitStatus exitStatus;

			exitStatus = pBuilder.start().waitFor();

			stdout = exitStatus.getStdOut().trim();
			stderr = exitStatus.getStdErr().trim();
			if (exitStatus.getExtProcExitStatus() != 0) {

			System.out.println("Error while executing [" + getCommand() + "]...");
			System.out.println("STDOUT: " + stdout);
			System.out.println("STDERR: " + stderr);
			return false;
			}

		} catch (final InterruptedException ie) {
			System.err
					.println("Interrupted while waiting for the following command to finish : "
							+ command.toString());
			return false;
		} catch (IOException e) {
			return false;
		}
		return true;

	}
}
