package alien.shell.commands;

import java.util.List;
import java.util.Set;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUIDUtils;
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

		if (bG) {
			if (GUIDUtils.isValidGUID(lfnOrGuid))
				guid = lfnOrGuid;
			else {
				commander.printErrln("This is not a valid GUID: " + lfnOrGuid);
				return;
			}
		}
		else {
			final LFN lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), lfnOrGuid));

			if (lfn != null && lfn.isFile() && lfn.guid != null)
				guid = lfn.guid.toString();
			else
				if (GUIDUtils.isValidGUID(lfnOrGuid)) {
					bG = true;
					guid = lfnOrGuid;
				}
				else
					if (lfn != null && !lfn.isFile()) {
						commander.printErrln("The path you indicated is not a file: " + lfnOrGuid);
						return;
					}
		}

		if (guid != null) {
			Set<PFN> pfns = commander.c_api.getPFNs(guid);

			if (pfns != null && pfns.size() > 0) {
				if (bR)
					if (pfns.toArray()[0] != null) {
						String archiveGUID = null;

						for (final PFN p : pfns)
							if (p.pfn.startsWith("guid://"))
								archiveGUID = p.pfn.substring(8, 44);

						if (archiveGUID != null) {
							pfns = commander.c_api.getPFNs(archiveGUID);

							if (pfns == null) {
								commander.printErrln("Archive with GUID " + archiveGUID + " doesn't exist");
								return;
							}

							if (pfns.size() == 0) {
								commander.printErrln("Archive with GUID " + archiveGUID + " doesn't have any replicas, this file cannot be used");
								return;
							}
						}
						else
							bR = false; // disable the archive lookup flag because this file is not member of an archive
					}

				if (bG)
					commander.printOutln("the GUID " + guid + " is in" + (bR ? "side this archive" : "") + "\n");
				else
					commander.printOutln("the file " + lfnOrGuid.substring(lfnOrGuid.lastIndexOf("/") + 1, lfnOrGuid.length()) + " is in" + (bR ? "side this archive" : "") + "\n");

				for (final PFN pfn : pfns) {
					final String se = pfn.seNumber > 0 ? "SE => " + commander.c_api.getSE(pfn.seNumber).seName : "ZIP archive member";
					commander.printOutln("\t " + padRight(se, 30) + " pfn => " + pfn.pfn + "\n");
				}
			}
			else
				if (pfns == null)
					commander.printErrln("GUID " + guid + " does not exist in the catalogue");
				else
					commander.printErrln("GUID " + guid + " has no replicas, this is a lost file");
		}
		else
			commander.printErrln("This file doesn't exist in the catalogue: " + lfnOrGuid);

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
