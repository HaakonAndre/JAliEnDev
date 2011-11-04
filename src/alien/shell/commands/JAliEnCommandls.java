package alien.shell.commands;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.Log;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandls extends JAliEnBaseCommand {

	/**
	 * marker for -l argument
	 */
	private boolean bL = false;

	/**
	 * marker for -a argument
	 */
	private boolean bA = false;

	/**
	 * marker for -F argument
	 */
	private boolean bF = false;

	/**
	 * marker for -c argument
	 */
	private boolean bC = false;
	
	/**
	 * marker for -b argument
	 */
	private boolean bB = false;

	private ArrayList<String> alPaths = null;

	/**
	 * list of the LFNs that came up by the ls command
	 */
	private List<LFN> directory = null;

	/**
	 * execute the ls
	 */
	@Override
	public void run() {

		int iDirs = alPaths.size();

		if (iDirs == 0)
			alPaths.add(commander.getCurrentDir().getCanonicalName());

		for (String sPath : alPaths) {
			
			// listing current directory
			if (!sPath.startsWith("/"))
				sPath = commander.getCurrentDir().getCanonicalName() + sPath;

			Log.log(Log.INFO, "Spath = \"" + sPath + "\"");

			directory = commander.c_api.getLFNs(sPath);

			if (directory != null) {
				for (LFN localLFN : directory) {
					
					if (!bA && localLFN.getFileName().startsWith("."))
						continue;

					String ret = "";
					if (bB) {
						if (localLFN.type == 'd')
							continue;
						ret += localLFN.guid.toString().toUpperCase() + padSpace(3)
								+ localLFN.getName();
					} else {
						if(bC)
							ret += localLFN.getCanonicalName();
						else{
							if (bL)
								ret += FileSystemUtils
									.getFormatedTypeAndPerm(localLFN)
									+ padSpace(3)
									+ padLeft(localLFN.owner, 8)
									+ padSpace(1)
									+ padLeft(localLFN.gowner, 8)
									+ padSpace(1)
									+ padLeft(String.valueOf(localLFN.size), 12)
									+ padSpace(1)
									+ format(localLFN.ctime)
									+ padSpace(4) + localLFN.getFileName();
					
							else
								ret += localLFN.getFileName();

							if (bF && (localLFN.type == 'd'))
								ret += "/";
						}
					}
					if (!isSilent())
						out.printOutln(ret);
				}
			}else
				out.printOutln("No such file or directory: [" + sPath + "]");

		}
		if (out.isRootPrinter())
			out.setReturnArgs(deserializeForRoot());
	}

	private static final DateFormat formatter = new SimpleDateFormat(
			"MMM dd HH:mm");

	private static synchronized String format(final Date d) {
		return formatter.format(d);
	}

	/**
	 * get the directory listing of the ls
	 * 
	 * @return list of the LFNs
	 */
	protected List<LFN> getDirectoryListing() {
		return directory;
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln(helpUsage("ls","[-options] [<directory>]"));
		out.printOutln(helpStartOptions());
		out.printOutln(helpOption("-l","long format"));
		out.printOutln(helpOption("-a","show hidden .* files"));
		out.printOutln(helpOption("-F","add trailing / to directory names"));
		out.printOutln(helpOption("-b","print in guid format"));
		out.printOutln(helpOption("-c","print canonical paths"));
		out.printOutln();
	}

	/**
	 * ls can run without arguments
	 * 
	 * @return <code>true</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

	/**
	 * serialize return values for gapi/root
	 * 
	 * @return serialized return
	 */
	@Override
	public String deserializeForRoot() {
		String ret = "";
		if (directory != null) {
			String col = RootPrintWriter.columnseparator;
			String desc = RootPrintWriter.fielddescriptor;
			String sep = RootPrintWriter.fieldseparator;

			if (bL) {
				for (LFN lfn : directory) {
					ret += col;
					ret += desc + "group" + sep + lfn.gowner;
					ret += desc + "permissions" + sep + lfn.perm;
					ret += desc + "date" + sep + lfn.ctime;
					ret += desc + "name" + sep + lfn.lfn;
					if(bF && (lfn.type == 'd'))
						ret += "/";
					ret += desc + "user" + sep + lfn.owner;
					ret += desc + "path" + sep + lfn.dir;
					ret += desc + "md5" + sep + lfn.md5;
					ret += desc + "size" + sep + lfn.size;

				}
			} else if(bB){
				for (LFN lfn : directory) {
					ret += col;
					ret += desc + "path" + sep + lfn.dir;
					ret += desc + "guid" + sep + lfn.guid;
				}
			}else {
				for (LFN lfn : directory) {
					ret += col;
					ret += desc + "name" + sep + lfn.lfn;
					if(bF && (lfn.type == 'd'))
						ret += "/";
					ret += desc + "path" + sep + lfn.dir;
				}
			}

			return ret;
		}
		return super.deserializeForRoot();

	}

	/**
	 * Constructor needed for the command factory in commander
	 * 
	 * @param commander
	 * @param out
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException 
	 */
	public JAliEnCommandls(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
		try {

			final OptionParser parser = new OptionParser();

			parser.accepts("l");
			parser.accepts("bulk");
			parser.accepts("b");
			parser.accepts("a");
			parser.accepts("F");
			parser.accepts("c");

			final OptionSet options = parser.parse(alArguments
					.toArray(new String[] {}));

			alPaths = new ArrayList<String>(options.nonOptionArguments().size());
			alPaths.addAll(options.nonOptionArguments());

			bL = options.has("l");
			// bBulk = options.has("bulk");
			bB = options.has("b");
			bA = options.has("a");
			bF = options.has("F");
			bC = options.has("c");
		} catch (OptionException e) {
			printHelp();
			throw e;
		}
	}

}
