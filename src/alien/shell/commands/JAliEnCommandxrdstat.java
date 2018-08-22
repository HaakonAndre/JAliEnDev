package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.io.protocols.Factory;
import alien.io.protocols.Xrootd;
import alien.se.SE;
import alien.shell.ShellColor;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author costing
 * @since 2018-08-21
 */
public class JAliEnCommandxrdstat extends JAliEnBaseCommand {
	private ArrayList<String> alPaths = null;

	@Override
	public void run() {
		Xrootd xrootd = null;

		for (final String lfnName : this.alPaths) {
			final LFN lfn = commander.c_api.getRealLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), lfnName));

			final GUID referenceGUID;

			if (lfn == null) {
				if (GUIDUtils.isValidGUID(lfnName)) {
					referenceGUID = commander.c_api.getGUID(lfnName);

					if (referenceGUID == null) {
						commander.printErrln("This GUID does not exist in the catalogue: " + lfnName);
						continue;
					}
				}
				else {
					commander.printErrln("This LFN does not exist in the catalogue: " + lfnName);
					continue;
				}
			}
			else
				if (lfn.guid != null)
					referenceGUID = commander.c_api.getGUID(lfn.guid.toString());
				else {
					commander.printErrln("Could not get the GUID of " + lfn.getCanonicalName());
					continue;
				}

			commander.printOutln("Checking the replicas of " + (lfn != null ? lfn.getCanonicalName() : referenceGUID.guid));

			for (final PFN p : referenceGUID.getPFNs()) {
				final SE se = p.getSE();

				if (se != null)
					commander.printOut("\t" + padRight(p.getSE().originalName, 20) + "\t" + p.getPFN() + "\t");
				else
					commander.printOut("\t(unknown SE)\t" + p.getPFN() + "\t");

				if (xrootd == null)
					xrootd = (Xrootd) Factory.xrootd.clone();

				try {
					final String status = xrootd.xrdstat(p, false, false, false);

					commander.printOutln(ShellColor.jobStateGreen() + "OK" + ShellColor.reset());
					commander.printOutln("\t\t" + status);
				} catch (final Throwable t) {
					final String error = t.getMessage();

					commander.printOutln(ShellColor.jobStateRed() + "ERR" + ShellColor.reset());
					commander.printOutln("\t\t" + error);
				}
			}
		}
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("xrdstat", "<filename1> [<or UUID>] ..."));
		commander.printOutln();
	}

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
	public JAliEnCommandxrdstat(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
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
