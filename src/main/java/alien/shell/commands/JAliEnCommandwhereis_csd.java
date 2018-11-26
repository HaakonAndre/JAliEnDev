package alien.shell.commands;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFNCSDUtils;
import alien.catalogue.LFN_CSD;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author mmmartin
 * @since November 23, 2018
 */
public class JAliEnCommandwhereis_csd extends JAliEnBaseCommand {

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
		whereis();
	}

	/**
	 * get the pfns
	 */
	public void whereis() {

		final ArrayList<String> slfn = new ArrayList<>();
		slfn.add(bG ? lfnOrGuid : FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), lfnOrGuid));

		commander.printOutln("whereis for: " + slfn);

		final Collection<LFN_CSD> lfns = commander.c_api.getLFNCSD(slfn, bG);
		final Optional<LFN_CSD> lfnco = lfns.stream().findFirst();

		if (lfnco.isPresent()) {
			final LFN_CSD lfnc = lfnco.get();
			HashMap<Integer, String> wi = lfnc.whereis();

			// check for empty or null pfn set
			if (wi == null || wi.size() == 0)
				commander.printErrln("Empty PFNs");
			else {
				Integer se = Integer.valueOf(0);
				String pfn = "";

				for (Integer senumber : wi.keySet()) {
					se = senumber;
					pfn = wi.get(se);
				}

				// if recursive and we have links, resolve them
				if (bR && wi.size() == 1 && se == Integer.valueOf(0)) {

					if (LFNCSDUtils.isValidLFN(pfn)) {
						lfnOrGuid = pfn;
						bG = false;
						whereis();
						return;
					}

					commander.printErrln("Following link chain led to incorrect LFN: " + pfn);
				}
				else {
					// PFNs for physical files with senumber-pfn map
					for (Integer senumber : wi.keySet()) {
						String seName = "no_se (link or zip member)";
						if (senumber != Integer.valueOf(0))
							seName = commander.c_api.getSE(senumber.intValue()).seName;
						commander.printOutln("\t The entry is in SE: " + seName + "\t with PFN: " + wi.get(senumber));
					}
				}

			}
		}
		else {
			commander.printErrln("Didn't find the LFN/UUID " + lfnOrGuid);
		}

	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("whereis", "[-options] [<filename>]"));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-g", "use the lfn as uuid"));
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
	public JAliEnCommandwhereis_csd(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
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
