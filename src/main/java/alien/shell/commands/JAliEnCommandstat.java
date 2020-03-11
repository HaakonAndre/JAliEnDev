package alien.shell.commands;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.se.SE;
import alien.se.SEUtils;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.Format;

/**
 * @author costing
 * @since 2018-08-15
 */
public class JAliEnCommandstat extends JAliEnBaseCommand {
	private ArrayList<String> alPaths = null;

	@Override
	public void run() {
		for (final String lfnName : this.alPaths) {
			final LFN lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), lfnName));

			if (lfn == null) {
				if (GUIDUtils.isValidGUID(lfnName)) {
					final GUID g = commander.c_api.getGUID(lfnName);

					if (g == null) {
						commander.printErrln("This GUID does not exist in the catalogue: " + lfnName);
						continue;
					}

					commander.printOutln("GUID: " + lfnName);
					commander.printOutln("Owner: " + g.owner + ":" + g.gowner);
					commander.printOutln("Permissions: " + g.perm);
					commander.printOutln("Size: " + g.size + " (" + Format.size(g.size) + ")");
					commander.printOut("guid", lfnName);
					commander.printOut("owner", g.owner);
					commander.printOut("gowner", g.gowner);
					commander.printOut("perm", g.perm);
					commander.printOut("size", String.valueOf(g.size));

					if (g.md5 != null && g.md5.length() > 0) {
						commander.printOutln("MD5: " + g.md5);
						commander.printOut("md5", g.md5);
					}

					final long gTime = GUIDUtils.epochTime(g.guid);

					commander.printOutln("Created: " + (new Date(gTime)) + " (" + gTime + ") by " + GUIDUtils.getMacAddr(g.guid));
					commander.printOut("mtime", String.valueOf(gTime));

					commander.printOutln("Last change: " + g.ctime + " (" + g.ctime.getTime() + ")");
					commander.printOut("ctime", String.valueOf(g.ctime.getTime() / 1000));

					final Set<PFN> pfns = g.getPFNs();

					if (pfns == null || pfns.size() == 0)
						commander.printOutln("No physical replicas");
					else {
						commander.printOutln("Replicas:");

						for (final PFN p : pfns) {
							String seName;

							if (p.seNumber > 0) {
								final SE se = SEUtils.getSE(p.seNumber);

								if (se == null)
									seName = "SE #" + p.seNumber + " no longer exists";
								else
									seName = "SE => " + se.seName;
							}
							else
								seName = "ZIP archive member";

							commander.printOutln("\t " + padRight(seName, 30) + " pfn => " + p.pfn + "\n");
						}
					}
				}
				else
					commander.setReturnCode(ErrNo.ENOENT, lfnName);
			}
			else {
				commander.printOutln("File: " + lfn.getCanonicalName());
				commander.printOutln("Type: " + lfn.type);
				commander.printOutln("Owner: " + lfn.owner + ":" + lfn.gowner);
				commander.printOutln("Permissions: " + lfn.perm);
				commander.printOutln("Last change: " + lfn.ctime + " (" + lfn.ctime.getTime() + ")");
				commander.printOut("file", lfn.getCanonicalName());
				commander.printOut("type", String.valueOf(lfn.type));
				commander.printOut("owner", lfn.owner);
				commander.printOut("gowner", lfn.gowner);
				commander.printOut("perm", lfn.perm);
				commander.printOut("ctime", String.valueOf(lfn.ctime.getTime() / 1000));

				if (!lfn.isDirectory()) {
					commander.printOutln("Size: " + lfn.size + " (" + Format.size(lfn.size) + ")");
					commander.printOutln("MD5: " + lfn.md5);
					commander.printOut("size", String.valueOf(lfn.size));
					commander.printOut("md5", lfn.md5);

					if (lfn.guid != null) {
						final long gTime = GUIDUtils.epochTime(lfn.guid);

						commander.printOutln("GUID: " + lfn.guid);
						commander.printOutln("\tGUID created on " + (new Date(gTime)) + " (" + gTime + ") by " + GUIDUtils.getMacAddr(lfn.guid));
						commander.printOut("guid", String.valueOf(lfn.guid));
						commander.printOut("guidctime", String.valueOf(gTime));
					}
				}

				if (lfn.jobid > 0) {
					commander.printOutln("Job ID: " + lfn.jobid);
					commander.printOut("jobid", String.valueOf(lfn.jobid));
				}
			}

			commander.printOutln();
		}
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("stat", "<filename1> [<or uuid>] ..."));
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
	public JAliEnCommandstat(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
		try {
			final OptionParser parser = new OptionParser();
			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			alPaths = new ArrayList<>(options.nonOptionArguments().size());
			alPaths.addAll(optionToString(options.nonOptionArguments()));
		}
		catch (final OptionException e) {
			printHelp();
			throw e;
		}
	}

}
