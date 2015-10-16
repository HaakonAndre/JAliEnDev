package alien.shell.commands;

import java.util.ArrayList;

import joptsimple.OptionException;

/**
 * 
 */
public class JAliEnCommandresubmit extends JAliEnBaseCommand {

	@Override
	public void run() {
		// TODO Auto-generated method stub

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
	 * @param commander
	 * @param out
	 * @param alArguments
	 * @throws OptionException
	 */
	public JAliEnCommandresubmit(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
	}
}
