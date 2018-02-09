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
		commander.printOutln("Shows free disk space");
		commander.printOutln("Usage: df");
		commander.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

	/**
	 * @param commander
	 * @param alArguments
	 * @throws OptionException
	 */
	public JAliEnCommanddf(final JAliEnCOMMander commander, final ArrayList<String> alArguments) throws OptionException {
		super(commander, alArguments);
	}
}
