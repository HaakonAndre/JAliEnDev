package alien.shell.commands;

import java.util.ArrayList;
import java.util.Set;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
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
	@Override
	public void run() {

		String guid = null;
		if (bG) {
			guid = lfnOrGuid;
		} else {
			LFN lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(
					commander.user.getName(), commander.getCurrentDir()
							.getCanonicalName(), lfnOrGuid));
			if(lfn!=null && lfn.guid!=null)
				guid = lfn.guid.toString();
		}
		// what message in case of error?
		if (guid != null) {

			Set<PFN> pfns = commander.c_api.getPFNs(guid);

			if (bR)
				if (pfns.toArray()[0] != null)
					if (((PFN) pfns.toArray()[0]).pfn.toLowerCase().startsWith(
							"guid://"))
						pfns = commander.c_api.getGUID(
								((PFN) pfns.toArray()[0]).pfn.substring(8, 44))
								.getPFNs();

			if (!isSilent())
				out.printOutln(AlienTime.getStamp()
						+ "The file "
						+ lfnOrGuid.substring(lfnOrGuid.lastIndexOf("/") + 1,
								lfnOrGuid.length()) + " is in\n");
			for (PFN pfn : pfns) {

				String se = commander.c_api.getSE(pfn.seNumber).seName;
				if (!isSilent())
					out.printOutln("\t\t SE => " + padRight(se, 30) + " pfn =>"
							+ pfn.pfn + "\n");
			}
		} else {
			if (!isSilent())
				out.printOutln("No such file: [" + lfnOrGuid + "]");
		}
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		
		out.printOutln();
		out.printOutln(helpUsage("whereis","[-options] [<filename>]"));
		out.printOutln(helpStartOptions());
		out.printOutln(helpOption("-g","use the lfn as guid"));
		out.printOutln(helpOption("-r","resolve links (do not give back pointers to zip archives)"));
		out.printOutln();
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
