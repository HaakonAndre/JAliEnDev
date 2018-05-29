package alien.shell.commands;

import java.util.List;
import java.util.Set;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandwhereis extends JAliEnBaseCommand {

	/**
	 * marker for -G argument
	 */
	private boolean bG = false;

	/**
	 * marker for -R argument
	 */
	private boolean bR = false;

	/**
	 * entry the call is executed on, either representing a LFN or a GUID
	 */
	private String lfnOrGuid = null;

	/**
	 * execute the whereis
	 */
	@Override
	public void run() {

		String guid = null;

		if (bG)
			guid = lfnOrGuid;
		else {
			final LFN lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), lfnOrGuid));
			if (lfn != null && lfn.guid != null)
				guid = lfn.guid.toString();
		}
		// what message in case of error?
		if (guid != null) {

			Set<PFN> pfns = commander.c_api.getPFNs(guid);

			if (bR)
				if (pfns.toArray()[0] != null)
					if (((PFN) pfns.toArray()[0]).pfn.toLowerCase().startsWith("guid://"))
						pfns = commander.c_api.getGUID(((PFN) pfns.toArray()[0]).pfn.substring(8, 44)).getPFNs();

			commander.printOutln("the file " + lfnOrGuid.substring(lfnOrGuid.lastIndexOf("/") + 1, lfnOrGuid.length()) + " is in\n");
			for (final PFN pfn : pfns) {

				final String se = commander.c_api.getSE(pfn.seNumber).seName;
				commander.printOutln("\t\t SE => " + padRight(se, 30) + " pfn =>" + pfn.pfn + "\n");
			}
		}
		else
			commander.printOutln("No such file: [" + lfnOrGuid + "]");
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("whereis", "[-options] [<filename>]"));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-g", "use the lfn as guid"));
		commander.printOutln(helpOption("-r", "resolve links (do not give back pointers to zip archives)"));
		commander.printOutln();
	}

	/**
	 * whereis cannot run without arguments
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
	public JAliEnCommandwhereis(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
		try {
			final OptionParser parser = new OptionParser();
			parser.accepts("g");
			parser.accepts("r");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			bG = options.has("g");
			bR = options.has("r");

			if (options.nonOptionArguments().iterator().hasNext())
				lfnOrGuid = options.nonOptionArguments().iterator().next().toString();
			else
				printHelp();
		} catch (final OptionException e) {
			printHelp();
			throw e;
		}
	}

}
