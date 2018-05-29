package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import alien.api.taskQueue.ResubmitJob;
import joptsimple.OptionException;

/**
 * @author mmmartin
 * @since November 22, 2017
 */
public class JAliEnCommandresubmit extends JAliEnBaseCommand {

	private final List<Long> queueIds;

	@Override
	public void run() {
		for (final long queueId : queueIds) {
			ResubmitJob rj = commander.q_api.resubmitJob(queueId);
			Entry<Integer, String> rc = (rj != null ? rj.resubmitEntry() : null);

			if (rc == null) {
				commander.printErrln("Problem with the resubmit request" + queueId);
			}
			else {
				switch (rc.getKey().intValue()) {
				case 0:
					commander.printOutln(rc.getValue());
					break;
				default:
					commander.printErrln(rc.getValue());
					break;
				}
			}
		}
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln("resubmit: resubmits a job or a group of jobs by IDs");
		commander.printOutln("        Usage:");
		commander.printOutln("                resubmit <jobid1> [<jobid2>....]");
		commander.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * Constructor needed for the command factory in commander
	 * 
	 * @param commander
	 * @param alArguments
	 * @throws OptionException
	 */
	public JAliEnCommandresubmit(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		queueIds = new ArrayList<>(alArguments.size());

		for (final String id : alArguments)
			try {
				queueIds.add(Long.valueOf(id));
			} catch (final NumberFormatException e) {
				throw new JAliEnCommandException("Invalid job ID: " + id, e);
			}
	}
}
