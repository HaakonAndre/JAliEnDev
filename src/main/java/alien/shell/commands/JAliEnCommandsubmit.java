package alien.shell.commands;

import java.io.File;
import java.io.IOException;
import java.util.List;

import alien.api.ServerException;
import alien.io.protocols.TempFileManager;
import alien.shell.ShellColor;
import alien.taskQueue.JDL;
import alien.taskQueue.TaskQueueUtils;
import lazyj.Utils;

/**
 * @author ron
 * @since June 9, 2011
 */
public class JAliEnCommandsubmit extends JAliEnCommandcat {

	@Override
	public void run() {

		long queueId = 0;
		commander.printOutln("Submitting " + alArguments.get(0));

		final File fout = catFile(alArguments.get(0));

		try {
			if (fout != null && fout.exists() && fout.isFile() && fout.canRead()) {
				final String content = Utils.readFile(fout.getAbsolutePath());

				if (content != null) {
					try {
						final JDL jdl;
						final String[] args = getArgs();

						try {
							jdl = TaskQueueUtils.applyJDLArguments(content, args);
						}
						catch (final IOException ioe) {
							commander.setReturnCode(1, "Error submitting " + alArguments.get(0) + ", JDL error: " + ioe.getMessage());
							return;
						}
						jdl.set("JDLPath", alArguments.get(0));

						queueId = commander.q_api.submitJob(jdl);
						if (queueId > 0) {
							commander.printOutln("Your new job ID is " + ShellColor.blue() + queueId + ShellColor.reset());
							commander.printOut("jobId", String.valueOf(queueId));
						}
						else
							commander.setReturnCode(2, "Error submitting " + alArguments.get(0));

					}
					catch (final ServerException e) {
						commander.setReturnCode(2, "Error submitting " + alArguments.get(0) + ", " + e.getMessage());
					}
				}
				else
					commander.setReturnCode(3, "Could not read the contents of " + fout.getAbsolutePath());
			}
			else
				commander.setReturnCode(4, "Not able to get the file " + alArguments.get(0));
		}
		finally {
			TempFileManager.release(fout);
		}
	}

	/**
	 * @return the arguments as a String array
	 */
	public String[] getArgs() {
		return alArguments.size() > 1 ? alArguments.subList(1, alArguments.size()).toArray(new String[0]) : null;
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("submit", "<URL>"));
		commander.printOutln();
		commander.printOutln(helpParameter("<URL> => <LFN>"));
		commander.printOutln(helpParameter("<URL> => file:///<local path>"));
		commander.printOutln();
	}

	/**
	 * Constructor needed for the command factory in commander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandsubmit(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);
	}
}
