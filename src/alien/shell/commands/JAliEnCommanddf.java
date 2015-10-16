package alien.shell.commands;

import java.util.ArrayList;

import joptsimple.OptionException;

/**
 *
 */
public class JAliEnCommanddf extends JAliEnBaseCommand {

	@Override
	public void run() {
		// TODO Auto-generated method stub

	}

	@Override
	public void printHelp() {
		out.printOutln("Shows free disk space");
		out.printOutln("Usage: df");
		out.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

	/**
	 * @param commander
	 * @param out
	 * @param alArguments
	 * @throws OptionException
	 */
	public JAliEnCommanddf(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
	}
}
