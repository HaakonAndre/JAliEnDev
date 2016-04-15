package alien.shell.commands;

import java.util.ArrayList;
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

			final LFN dir = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDir().getCanonicalName(), path), false);

			if (dir != null && dir.exists) {
				if (dir.isDirectory()) {

					if (AuthorizationChecker.canWrite(dir, commander.user)) {

						if (out.isRootPrinter()) {
							if (bP)
								out.setField("message", "Inside Parent Directory");
							if (!commander.c_api.removeCatalogueDirectory(dir.getCanonicalName())) {
								if (!isSilent())
									out.setField("Could not remove directory (or non-existing parents): ", path);
							} else if (!commander.c_api.removeCatalogueDirectory(dir.getCanonicalName()))
								if (!isSilent())
									out.setField("Could not remove directory: ", path);
						} else if (bP) {
							out.printOutln("Inside Parent Directory");
							if (!commander.c_api.removeCatalogueDirectory(dir.getCanonicalName())) {
								if (!isSilent())
									out.printErrln("Could not remove directory (or non-existing parents): " + path);

								logger.log(Level.WARNING, "Could not remove directory (or non-existing parents): " + path);

							}
						} else if (!commander.c_api.removeCatalogueDirectory(dir.getCanonicalName())) {
							if (!isSilent())
								out.printErrln("Could not remove directory: " + path);
							logger.log(Level.WARNING, "Could not remove directory: " + path);

						}
					} else if (!isSilent()) {
						out.printErrln("Permission denied on directory: [" + path + "]");
						out.setReturnCode(1, "Permission denied on directory: [" + path + "]");
					}

				} else if (!isSilent()) {
					out.printErrln("Not a directory: [" + path + "]");
					out.setReturnCode(2, "Not a directory: [" + path + "]");
				}

			} else if (!isSilent()) {
				out.printErrln("No such file or directory: [" + path + "]");
				out.setReturnCode(3, "No such file or directory: [" + path + "]");
			}
		}
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {

		out.printOutln();
		out.printOutln(helpUsage("rmdir", " [<option>] <directory>"));
		out.printOutln(helpStartOptions());
		out.printOutln(helpOption("--ignore-fail-on-non-empty", "  ignore each failure that is solely because a directory is non-empty"));
		out.printOutln(helpOption("-p ", "--parents   Remove DIRECTORY and its ancestors.  E.g., 'rmdir -p a/b/c' is similar to 'rmdir a/b/c a/b a'."));
		out.printOutln(helpOption("-v ", "--verbose  output a diagnostic for every directory processed"));
		out.printOutln(helpOption(" ", "--help      display this help and exit"));
		out.printOutln(helpOption(" ", "--version  output version information and exit"));
		out.printOutln(helpOption("-silent", "execute command silently"));
		out.printOutln();
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
	 * @param out
	 *
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandrmdir(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
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
