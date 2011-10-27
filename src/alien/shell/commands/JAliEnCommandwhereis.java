package alien.shell.commands;

import java.util.ArrayList;
import java.util.Set;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import alien.api.catalogue.CatalogueApiUtils;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.perl.commands.AlienTime;

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
	public void execute() {

		String guid = null;
		if (bG) {
			guid = lfnOrGuid;
		} else {
			LFN lfn = CatalogueApiUtils.getLFN(FileSystemUtils.getAbsolutePath(
					commander.user.getName(), commander.getCurrentDir()
							.getCanonicalName(), lfnOrGuid));
			if(lfn!=null && lfn.guid!=null)
				guid = lfn.guid.toString();
		}
		// what message in case of error?
		if (guid != null) {

			Set<PFN> pfns = CatalogueApiUtils.getPFNs(guid);

			if (bR)
				if (pfns.toArray()[0] != null)
					if (((PFN) pfns.toArray()[0]).pfn.toLowerCase().startsWith(
							"guid://"))
						pfns = CatalogueApiUtils.getGUID(
								((PFN) pfns.toArray()[0]).pfn.substring(8, 44))
								.getPFNs();

			if (!silent)
				out.printOutln(AlienTime.getStamp()
						+ "The file "
						+ lfnOrGuid.substring(lfnOrGuid.lastIndexOf("/") + 1,
								lfnOrGuid.length()) + " is in\n");
			for (PFN pfn : pfns) {

				String se = CatalogueApiUtils.getSE(pfn.seNumber).seName;
				if (!silent)
					out.printOutln("\t\t SE => " + padRight(se, 30) + " pfn =>"
							+ pfn.pfn + "\n");
			}
		} else {
			if (!silent)
				out.printOutln("No such file: [" + lfnOrGuid + "]");
		}
	}

	/**
	 * printout the help info
	 */
	public void printHelp() {
		
		out.printOutln();
		out.printOutln(helpUsage("whereis","[-options] [<filename>]"));
		out.printOutln(helpStartOptions());
		out.printOutln(helpOption("-g","use the lfn as guid"));
		out.printOutln(helpOption("-r","resolve links (do not give back pointers to zip archives)"));
	}

	/**
	 * whereis cannot run without arguments
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
	 * Constructor needed for the command factory in JAliEnCOMMander
	 * @param commander 
	 * @param out 
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException 
	 */
	public JAliEnCommandwhereis(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException {
		super(commander, out,alArguments);
		try {
			final OptionParser parser = new OptionParser();
			parser.accepts("g");

			final OptionSet options = parser.parse(alArguments
					.toArray(new String[] {}));

			bG = options.has("g");

			if (options.nonOptionArguments().iterator().hasNext())
				lfnOrGuid = options.nonOptionArguments().iterator().next();
			else
				printHelp();
		} catch (OptionException e) {
			printHelp();
			throw e;
		}
	}

}
