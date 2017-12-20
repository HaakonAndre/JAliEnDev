package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

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
			Entry<Integer, String> rc = commander.q_api.resubmitJob(queueId);

			switch (rc.getKey().intValue()) {
			case 0:
				if (out.isRootPrinter())
					out.setField("message", rc.getValue());
				else
					out.printOutln(rc.getValue());
				break;
			default:
				if (out.isRootPrinter())
					out.setField("message", rc.getValue());
				else
					out.printErrln(rc.getValue());
				break;
			}
		}
	}

	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln("resubmit: resubmits a job or a group of jobs by IDs");
		out.printOutln("        Usage:");
		out.printOutln("                resubmit <jobid1> [<jobid2>....]");
		out.printOutln();

	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * Constructor needed for the command factory in commander
	 * 
	 * @param commander
	 * @param out
	 * @param alArguments
	 * @throws OptionException
	 */
	public JAliEnCommandresubmit(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
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
