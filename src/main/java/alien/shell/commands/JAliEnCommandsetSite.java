package alien.shell.commands;

import java.util.List;

import joptsimple.OptionException;

/**
 * @author costing
 * @since 2019-07-04
 */
public class JAliEnCommandsetSite extends JAliEnBaseCommand {

	private String targetSiteName;

	/**
	 * execute the command
	 */
	@Override
	public void run() {
		commander.setSite(targetSiteName);
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("setSite", "[site name]"));
		commander.printOutln(helpStartOptions());
		commander.printOutln();
	}

	/**
	 * setSite cannot run without arguments
	 *
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * Constructor needed for the command factory in JAliEnCOMMander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandsetSite(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		if (alArguments.size() > 0)
			targetSiteName = alArguments.get(0);
	}

}
