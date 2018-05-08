package alien.shell.commands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import alien.catalogue.FileSystemUtils;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

// TODO check that the user has indeed permissions on the target entry, server side
// TODO validate that the user can become the target user name and the target role name (implicitly validating the strings)
// TODO recursive flag
// TODO multiple parameters to the command
// TODO parse groups
// FIXME passes empty group which fails the query
// TODO check that groups/users exist

/**
 * chown
 */
public class JAliEnCommandchown extends JAliEnBaseCommand {
	private String user;
	private String group;
	private String file;
	private boolean recursive;

	@Override
	public void run() {
		if (this.user == null || this.file == null) {
			commander.printErr("No user or file entered");
			return;
		}

		// boolean result = false;
		final String path = FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), this.file);
		// run chown command
		final HashMap<String, Boolean> results = commander.c_api.chownLFN(path, this.user, this.group, this.recursive);

		if (results == null) {
			commander.printErr("Failed to chown file(s)");
			return;
		}

		for (final String filename : results.keySet()) {
			final Boolean b = results.get(filename);

			if (b == null || !b.booleanValue())
				commander.printErrln(filename + ": unable to chown");
		}
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln("Usage: chown -R <user>[.<group>] <file>");
		commander.printOutln();
		commander.printOutln("Changes an owner or a group for a file");
		commander.printOutln("-R : do a recursive chown");
		commander.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * @param commander
	 * @param alArguments
	 * @throws OptionException
	 */
	public JAliEnCommandchown(final JAliEnCOMMander commander, final ArrayList<String> alArguments) throws OptionException {
		super(commander, alArguments);
		try {
			final OptionParser parser = new OptionParser();
			parser.accepts("R");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			final List<String> params = optionToString(options.nonOptionArguments());

			this.recursive = options.has("R");

			// check 2 arguments
			if (params.size() != 2)
				return;

			// get user/group
			final String[] usergrp = params.get(0).split("\\.");
			System.out.println(Arrays.toString(usergrp));
			this.user = usergrp[0];
			if (usergrp.length == 2)
				this.group = usergrp[1];

			// get file
			this.file = params.get(1);
		} catch (final OptionException e) {
			// printHelp();
			throw e;
		}
	}

}
