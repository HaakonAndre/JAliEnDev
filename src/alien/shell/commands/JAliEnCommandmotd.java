package alien.shell.commands;

import java.util.ArrayList;

/**
 * @author ron
 *
 */
public class JAliEnCommandmotd extends JAliEnBaseCommand {

	/**
	 * @param commander
	 * @param alArguments
	 */
	public JAliEnCommandmotd(final JAliEnCOMMander commander, final ArrayList<String> alArguments) {
		super(commander, alArguments);
	}

	@Override
	public void run() {

		final String sMotdMessage = "\n###############################################################\n" + "# AliEn Service Message: All is well in the Grid world. Enjoy!#\n"
				+ "##############################################################\n" + "* Operational problems: Latchezar.Betev@cern.ch\n"
				+ "* Bug reports: http://savannah.cern.ch/bugs/?group=alien&func=additem\n" + "###############################################################\n";

		commander.printOutln(sMotdMessage);

	}

	@Override
	public void printHelp() {
		// ignore

	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

}
