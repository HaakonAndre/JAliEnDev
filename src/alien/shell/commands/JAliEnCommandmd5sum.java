package alien.shell.commands;

import java.util.ArrayList;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author costing
 *
 */
public class JAliEnCommandmd5sum extends JAliEnBaseCommand {
	private ArrayList<String> alPaths = null;

	@Override
	public void run() {
		for (final String lfnName : this.alPaths) {
			final LFN lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), lfnName));
			if (lfn == null)
				out.printErrln("LFN does exist");
			else
				if (lfn.md5 != null) {
					out.printOutln("md5: " + lfn.md5);
					System.out.println(lfn);
				}
				else {
					out.printErrln("Can not get md5 for this file");
					System.out.println(lfn);
				}
			// GUID guid = GUIDUtils.getGUID( );
		}

	}

	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln(helpUsage("md5sum", "<filename1> [<filename2>] ..."));
		out.printOutln();

	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * Constructor needed for the command factory in JAliEnCOMMander
	 *
	 * @param commander
	 * @param out
	 *
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandmd5sum(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
		try {
			final OptionParser parser = new OptionParser();
			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			alPaths = new ArrayList<>(options.nonOptionArguments().size());
			alPaths.addAll(optionToString(options.nonOptionArguments()));
		} catch (final OptionException e) {
			printHelp();
			throw e;
		}
	}

}
