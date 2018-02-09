package alien.shell.commands;

import java.util.ArrayList;
import java.util.logging.Level;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.RemoveLFNfromString;
import alien.catalogue.FileSystemUtils;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author ron
 * @since Oct 27, 2011
 * @author sraje (Shikhar Raje, IIIT Hyderabad)
 * @since June 25, 2012
 */
public class JAliEnCommandrm extends JAliEnBaseCommand {
	/**
	 * Variable for -f "Force" flag and -i "Interactive" flag. These 2 flags contradict each other, and hence only 1 variable for them. (Source: GNU Man pages for rm).
	 *
	 * @val True if interactive; False if forced.
	 */
	boolean bIF = false;

	/**
	 * Variable for -r "Recursive" flag
	 */
	boolean bR = false;

	/**
	 * Variable for -v "Verbose" flag
	 */
	boolean bV = false;

	@Override
	public void run() {
		for (final String path : alArguments) {

			if (path == null) {
				logger.log(Level.WARNING, "Could not get LFN: " + path);
				return;
			}

			String fullPath = FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), path);

			final RemoveLFNfromString rlfn = new RemoveLFNfromString(commander.getUser(), fullPath, bR);

			try {
				final RemoveLFNfromString a = Dispatcher.execute(rlfn); // Remember, all checking is being done server side now.

				if (!a.wasRemoved())
					JAliEnCOMMander.setReturnCode(1, "Failed to remove [" + fullPath + "]");
			} catch (final ServerException e) {
				e.getCause().printStackTrace();
				JAliEnCOMMander.setReturnCode(1, "Failed to remove [" + fullPath + "]");
			}
		}
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("rm", " <LFN> [<LFN>[,<LFN>]]"));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-f", "ignore nonexistent files, never prompt"));
		commander.printOutln(helpOption("-r, -R", "remove directories and their contents recursively"));
		commander.printOutln(helpOption("-i", "prompt before every removal (for JSh clients)"));
		commander.printOutln();
	}

	/**
	 * rm cannot run without arguments
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
	 *
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandrm(final JAliEnCOMMander commander, final ArrayList<String> alArguments) throws OptionException {
		super(commander, alArguments);
		try {
			final OptionParser parser = new OptionParser();
			parser.accepts("i");
			parser.accepts("f");
			parser.accepts("r");
			parser.accepts("R");
			parser.accepts("v");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			bIF = options.has("i");
			bIF = !options.has("f");
			bR = options.has("r") || options.has("R");
			bV = options.has("v");
		} catch (@SuppressWarnings("unused") final OptionException e) {
			printHelp();
			// throw e;
		}
	}
}
