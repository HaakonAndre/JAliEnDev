package alien.shell.commands;

import java.util.ArrayList;

import joptsimple.OptionException;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.user.AuthorizationChecker;

/**
 * @author ron
 * @since Oct 27, 2011
 */
public class JAliEnCommandrm extends JAliEnBaseCommand {

	public void run() {

		for (String path : alArguments) {

			LFN dir = commander.c_api.getLFN(FileSystemUtils
					.getAbsolutePath(commander.user
							.getName(), commander
							.getCurrentDir()
							.getCanonicalName(), path));

			if (dir!=null && dir.exists) {
				if (!dir.isDirectory()) {
					if (AuthorizationChecker.canWrite(dir, commander.user)) {

						if (!commander.c_api
								.removeLFN(dir.getCanonicalName())) {
							out.printErrln("Could not remove LFN: "
									+ path);
							out.printErrln("Sorry, this command is not implemented yet.");
						}

					} else {
						if (!silent)
							out.printErrln("Permission denied on LFN: ["
									+ path + "]");
					}

				} else {
					if (!silent)
						out.printErrln("LFN is a directory: [" + path + "]");
				}
			} else {
				if (!silent)
					out.printErrln("No such file or directory: [" + path + "]");
			}
		}
	}

	/**
	 * printout the help info
	 */
	public void printHelp() {

		out.printOutln();
		out.printOutln(helpUsage("rm",
				" <LFN> [<LFN>[,<LFN>]]"));
		out.printOutln(helpStartOptions());
		out.printOutln(helpOption("-silent","execute command silently"));
		out.printOutln();
	}

	/**
	 * mkdir cannot run without arguments
	 * 
	 * @return <code>false</code>
	 */
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * the command's silence trigger
	 */
	private boolean silent = false;

	/**
	 * set command's silence trigger
	 */
	public void silent() {
		silent = true;
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
	public JAliEnCommandrm(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);

	}
}
