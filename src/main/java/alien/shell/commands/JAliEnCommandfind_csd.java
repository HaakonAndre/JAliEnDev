package alien.shell.commands;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFNUtils;
import alien.catalogue.LFN_CSD;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.Format;

/**
 * @author mmmartin
 * @since November 23, 2018
 */
public class JAliEnCommandfind_csd extends JAliEnBaseCommand {

	/**
	 * marker for -x argument : return the LFNCSD list through XmlCollection
	 */
	private boolean bX = false;
	private boolean bH = false;

	private String xmlCollectionName = null;

	/**
	 * marker for -a argument : show hidden .files
	 */
	private boolean bA = false;

	/**
	 * marker for -d argument : directory names
	 */
	private boolean bD = false;

	/**
	 * marker for -Y argument: latest version for OCDB
	 */
	private boolean bY = false;

	/**
	 * marker for -j argument : filter files for jobid
	 */
	private boolean bJ = false;

	/**
	 * marker for -l argument : long format, optionally human readable file sizes
	 */
	private boolean bL = false;

	private List<String> alPaths = null;

	private Collection<LFN_CSD> lfns = null;

	private Long queueid = Long.valueOf(0);

	/**
	 * returns the LFNCSDs that were the result of the find
	 *
	 * @return the output file
	 */

	public Collection<LFN_CSD> getLFNs() {
		return lfns;
	}

	/**
	 * execute the get
	 */
	@Override
	public void run() {
		if (alPaths.size() < 2) {
			printHelp();
			return;
		}

		int flags = 0;
		String query = "";

		if (bD)
			flags = flags | LFNUtils.FIND_INCLUDE_DIRS;
		if (bY) {
			for (int i = 2; i < alPaths.size(); i++)
				query += alPaths.get(i) + " ";
			flags = flags | LFNUtils.FIND_BIGGEST_VERSION;
		}
		if (bX)
			flags = flags | LFNUtils.FIND_SAVE_XML;
		if (bJ)
			flags = flags | LFNUtils.FIND_FILTER_JOBID;

		String xmlCollectionPath = xmlCollectionName != null ? FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), xmlCollectionName) : null;

		lfns = commander.c_api.find_csd(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), alPaths.get(0)), alPaths.get(1), query, flags, xmlCollectionPath,
				queueid);

		if (lfns != null) {
			if (bX) {
				return;
			}

			for (final LFN_CSD lfn : lfns) {
				commander.outNextResult();
				commander.printOut("lfn", lfn.getCanonicalName());

				if (bL) {
					commander.printOut("permissions", FileSystemUtils.getFormatedTypeAndPerm(lfn));
					commander.printOut("user", lfn.owner);
					commander.printOut("group", lfn.gowner);
					commander.printOut("size", (bH ? Format.size(lfn.size) : String.valueOf(lfn.size)));
					commander.printOut("ctime", " " + lfn.ctime);

					// print long
					commander.printOutln(FileSystemUtils.getFormatedTypeAndPerm(lfn) + padSpace(3) + padLeft(lfn.owner, 8) + padSpace(1) + padLeft(lfn.gowner, 8) + padSpace(1)
							+ padLeft(bH ? Format.size(lfn.size) : String.valueOf(lfn.size), 12) + padSpace(1) + format(lfn.ctime) + padSpace(1) + padSpace(4) + lfn.getCanonicalName());
				}
				else
					commander.printOutln(lfn.getCanonicalName());
			}
		}

	}

	private static final DateFormat formatter = new SimpleDateFormat("MMM dd HH:mm");

	private static synchronized String format(final Date d) {
		return formatter.format(d);
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("find", "<path>  <pattern> flags"));
		commander.printOutln();

		commander.printOutln(helpStartOptions());

		commander.printOutln(helpOption("-a", "show hidden .* files"));
		commander.printOutln(helpOption("-c", "collection filename (put the output in a collection)"));
		commander.printOutln(helpOption("-y", "(FOR THE OCDB) return only the biggest version of each file"));
		commander.printOutln(helpOption("-x", "xml collection name (return the LFN list through XmlCollection)"));
		commander.printOutln(helpOption("-d", "return also the directories"));
		commander.printOutln(helpOption("-l[h]", "long format, optionally human readable file sizes"));
		commander.printOutln(helpOption("-j <queueid>", "filter files created by a certain job"));
		commander.printOutln();
	}

	/**
	 * find cannot run without arguments
	 *
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * Constructor needed for the command factory in commander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandfind_csd(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
		try {

			final OptionParser parser = new OptionParser();

			parser.accepts("l");
			parser.accepts("s");
			parser.accepts("x").withRequiredArg();
			parser.accepts("a");
			parser.accepts("h");
			parser.accepts("d");
			parser.accepts("y");
			parser.accepts("j").withRequiredArg().ofType(Long.class);

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			if (options.has("x") && options.hasArgument("x")) {
				bX = true;
				xmlCollectionName = (String) options.valueOf("x");
			}

			if (options.has("j") && options.hasArgument("j")) {
				bJ = true;
				queueid = (Long) options.valueOf("j");
			}

			alPaths = optionToString(options.nonOptionArguments());

			bL = options.has("l");
			bA = options.has("a");
			bD = options.has("d");
			bH = options.has("h");
			bY = options.has("y");
		} catch (final OptionException e) {
			printHelp();
			throw e;
		}
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();

		sb.append("\n { JAliEnCommandfind received\n");
		sb.append("Arguments: ");

		if (bL)
			sb.append(" -l ");
		if (bA)
			sb.append(" -a ");
		if (bX)
			sb.append(" -x ");
		if (bD)
			sb.append(" -d ");
		if (bJ)
			sb.append(" -j ");

		sb.append("}");

		return sb.toString();
	}

}
