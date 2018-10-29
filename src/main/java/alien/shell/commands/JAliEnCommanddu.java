package alien.shell.commands;

import java.util.List;

import joptsimple.OptionException;

/**
 *
 */
public class JAliEnCommanddu extends JAliEnBaseCommand {

	@Override
	public void run() {
		// TODO Auto-generated method stub

	}

	@Override
	public void printHelp() {
		commander.printOutln("Gives the disk space usge of a directory");
		commander.printOutln("Usage: du [-hf] <dir>");
		commander.printOutln();
		commander.printOutln("Options:");
		commander.printOutln("	-h: Give the output in human readable format");
		commander.printOutln("	-f: Count only files (ignore the size of collections)");
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
	public JAliEnCommanddu(final JAliEnCOMMander commander,final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
	}
}
