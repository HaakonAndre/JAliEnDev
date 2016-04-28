package alien.shell.commands;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import alien.api.ServerException;
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
		if (!isSilent())
			out.printOutln("Submitting " + alArguments.get(0));

		final File fout = catFile(alArguments.get(0));

		if (fout != null && fout.exists() && fout.isFile() && fout.canRead()) {
			final String content = Utils.readFile(fout.getAbsolutePath());

			if (out.isRootPrinter()) {
				if (content != null)
					try {
						final JDL jdl;
						final String[] args = alArguments.size() > 1 ? alArguments.subList(1, alArguments.size() - 1).toArray(new String[0]) : null;

						try {
							jdl = TaskQueueUtils.applyJDLArguments(content, commander.user, commander.role, args);
						} catch (final IOException ioe) {
							if (!isSilent()) {
								out.setField("Error submitting ", alArguments.get(0));

								out.setField("JDL error: ", ioe.getMessage());
							}
							return;
						}
						jdl.set("JDLPath", alArguments.get(0));

						queueId = commander.q_api.submitJob(jdl);
						if (queueId > 0) {
							if (!isSilent())
								out.setField("Your new job ID is ", ShellColor.blue() + queueId + ShellColor.reset());
						} else if (!isSilent())
							out.setField("Error submitting ", alArguments.get(0));
					}

					catch (final ServerException e) {
						if (!isSilent())
							out.setField("Error submitting ", alArguments.get(0) + "," + e.getMessage());
					}

			} else if (content != null)
				try {
					final JDL jdl;
					final String[] args = alArguments.size() > 1 ? alArguments.subList(1, alArguments.size() - 1).toArray(new String[0]) : null;

					try {
						jdl = TaskQueueUtils.applyJDLArguments(content, commander.user, commander.role, args);
					} catch (final IOException ioe) {
						if (!isSilent())
							out.printErrln("Error submitting " + alArguments.get(0) + ", JDL error: " + ioe.getMessage());
						return;
					}
					jdl.set("JDLPath", alArguments.get(0));

					queueId = commander.q_api.submitJob(jdl);
					if (queueId > 0) {
						if (!isSilent())
							out.printOutln("Your new job ID is " + ShellColor.blue() + queueId + ShellColor.reset());
					} else if (!isSilent())
						out.printErrln("Error submitting " + alArguments.get(0));
				}

				catch (final ServerException e) {
					if (!isSilent())
						out.printErrln("Error submitting " + alArguments.get(0) + ", " + e.getMessage());
				}
			else if (!isSilent())
				out.printErrln("Could not read the contents of " + fout.getAbsolutePath());
		}

		else if (!isSilent()) {
			out.printErrln("Not able to get the file " + alArguments.get(0));
			out.setReturnCode(1, "Not able to get the file " + alArguments.get(0));
		}
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln(helpUsage("submit", "<URL>"));
		out.printOutln();
		out.printOutln(helpParameter("<URL> => <LFN>"));
		out.printOutln(helpParameter("<URL> => file:///<local path>"));
		out.printOutln();
	}

	/**
	 * Constructor needed for the command factory in commander
	 *
	 * @param commander
	 * @param out
	 *
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandsubmit(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) {
		super(commander, out, alArguments);
	}
}
