package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

import alien.taskQueue.Job;
import alien.user.AuthorizationChecker;
import joptsimple.OptionException;

/**
 * @author ron
 * @since June 9, 2011
 */
public class JAliEnCommandkill extends JAliEnBaseCommand {

	private final List<Long> queueIds;

	@Override
	public void run() {

		final List<Job> jobs = commander.q_api.getJobs(queueIds);

		for (final Job job : jobs)
			if (AuthorizationChecker.canModifyJob(job, commander.user, commander.role))
				commander.q_api.killJob(job.queueId);
		if (out.isRootPrinter())
			out.setReturnCode(1, "not implemented yet ");
		else
			out.printErrln("--- not implemented yet ---");

	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {

		out.printOutln();
		out.printOutln(helpUsage("kill", "<jobId> [<jobId>[,<jobId>]]"));
		out.printOutln();
	}

	/**
	 * cat cannot run without arguments
	 *
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * Constructor needed for the command factory in commander
	 *
	 * @param commander
	 * @param out
	 *
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandkill(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);

		queueIds = new ArrayList<>(alArguments.size());

		for (final String id : alArguments)
			try {
				queueIds.add(Long.valueOf(id));
			} catch (final NumberFormatException e) {
				throw new JAliEnCommandException("Invalid job ID: " + id, e);
			}
	}

}