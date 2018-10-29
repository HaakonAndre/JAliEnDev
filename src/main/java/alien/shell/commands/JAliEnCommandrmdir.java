package alien.shell.commands;

import java.util.List;
import java.util.logging.Level;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.user.AuthorizationChecker;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author ron
 * @since Oct 27, 2011
 */
public class JAliEnCommandrmdir extends JAliEnBaseCommand {

	private boolean bP = false;
	private List<String> alPaths = null;

	@Override
	public void run() {
		for (final String path : alPaths) {

			final LFN dir = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), path), false);

			if (dir != null && dir.exists) {
				if (dir.isDirectory()) {
					if (AuthorizationChecker.canWrite(dir, commander.user)) {
						if (bP) {
							commander.printOutln("Inside Parent Directory");
							if (!commander.c_api.removeCatalogueDirectory(dir.getCanonicalName())) {
								commander.printErrln("Could not remove directory (or non-existing parents): " + path);

								logger.log(Level.WARNING, "Could not remove directory (or non-existing parents): " + path);

							}
						}
						else
							if (!commander.c_api.removeCatalogueDirectory(dir.getCanonicalName())) {
								commander.printErrln("Could not remove directory: " + path);
								logger.log(Level.WARNING, "Could not remove directory: " + path);

							}
					}
					else
						commander.setReturnCode(1, "Permission denied on directory: [" + path + "]");

				}
				else
					commander.setReturnCode(2, "Not a directory: [" + path + "]");
			}
			else
				commander.setReturnCode(3, "No such file or directory: [" + path + "]");
		}
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("rmdir", " [<option>] <directory>"));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("--ignore-fail-on-non-empty", "  ignore each failure that is solely because a directory is non-empty"));
		commander.printOutln(helpOption("-p ", "--parents   Remove DIRECTORY and its ancestors.  E.g., 'rmdir -p a/b/c' is similar to 'rmdir a/b/c a/b a'."));
		commander.printOutln(helpOption("-v ", "--verbose  output a diagnostic for every directory processed"));
		commander.printOutln(helpOption(" ", "--help      display this help and exit"));
		commander.printOutln(helpOption(" ", "--version  output version information and exit"));
		commander.printOutln(helpOption("-silent", "execute command silently"));
		commander.printOutln();
	}

	/**
	 * mkdir cannot run without arguments
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
	public JAliEnCommandrmdir(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
		try {

			final OptionParser parser = new OptionParser();

			parser.accepts("p");
			parser.accepts("v");
			parser.accepts("s");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			alPaths = optionToString(options.nonOptionArguments());

			if (options.has("s"))
				silent();
			bP = options.has("p");

		} catch (final OptionException e) {
			printHelp();
			throw e;
		}

	}
}
