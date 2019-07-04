package alien.shell.commands;

import java.io.IOException;
import java.util.List;

import joptsimple.OptionException;
import lazyj.Utils;

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
		if (targetSiteName.equalsIgnoreCase("auto")) {
			try {
				String autoSiteName = Utils.download("http://alimonitor.cern.ch/services/getClosestSite.jsp", null);

				if (autoSiteName != null && autoSiteName.length() > 0)
					targetSiteName = autoSiteName.trim();
				else {
					commander.printErrln("Could not map you to a site name at the moment, keeping previous value of " + commander.getSite());
					return;
				}
			}
			catch (final IOException ioe) {
				commander.printErrln("Could not retrieve the site name from the external service: " + ioe.getMessage());
				return;
			}
		}

		commander.printOutln("Setting close site to " + targetSiteName + " (was " + commander.getSite() + ")");

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
