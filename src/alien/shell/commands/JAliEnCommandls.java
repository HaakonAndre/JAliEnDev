package alien.shell.commands;

import java.text.DateFormat;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.Log;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;

/**
 * @author ron
 * @since June 4, 2011
 * running ls command with possible options  <br />                                                                                                                                                                  
 *     -l                     :  long format <br />                                                                                                                                                                    
 *     -a                     :  show hidden .* files <br />                                                                                                                                                            
 *     -F                     :  add trailing / to directory names <br />                                                                                                                                               
 *     -b                     :  print in GUID format  <br />                                                                                                                                                          
 *     -c                     :  print canonical paths  <br />                                                                                                                                                                                                                 
 */
public class JAliEnCommandls extends JAliEnBaseCommand {

	/**
	 * marker for -l argument : long format
	 */
	private boolean bL = false;

	/**
	 * marker for -a argument : show hidden .files
	 */
	private boolean bA = false;

	/**
	 * marker for -F argument : add trailing / to directory names
	 */
	private boolean bF = false;

	/**
	 * marker for -c argument : print canonical paths
	 */
	private boolean bC = false;

	/**
	 * marker for -b argument : print in GUID format
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

			Log.log(Log.INFO, "LS: listing for directory = \"" + sPath + "\"");

			final List<LFN> subdirectory = commander.c_api.getLFNs(sPath);

			if (subdirectory != null) {
				if (directory==null)
					directory = new ArrayList<>(subdirectory);
				else
					directory.addAll(subdirectory);

				for (LFN localLFN : subdirectory) {

					logger.log(Level.FINE, localLFN.toString());

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

					logger.info("LS line : "+ret);

					if (!isSilent())
						out.printOutln(ret);
				}
			}
			else{
				logger.log(Level.SEVERE, "No such file or directory: [" + sPath + "]");
				out.printOutln("No such file or directory: [" + sPath + "]");
			}

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
		logger.log(Level.INFO, toString());

		final StringBuilder ret = new StringBuilder();

		final SimpleDateFormat sdt = new SimpleDateFormat("MMM dd HH:mm");

		if (directory != null) {
			String col = RootPrintWriter.columnseparator;
			String desc = RootPrintWriter.fielddescriptor;
			String sep = RootPrintWriter.fieldseparator;

			for (final LFN lfn : directory) {
				if (!bA && lfn.getFileName().startsWith("."))
					continue;

				if(bB){
					if(lfn.type != 'd') {
						ret.append(col);
						ret.append(desc).append("path").append(sep).append(lfn.getCanonicalName());
						ret.append(desc).append("guid").append(sep).append(lfn.guid);
					}
				}
				else if(bC){
					ret.append(col);
					ret.append(desc).append("name").append(sep).append(lfn.getCanonicalName());
				}
				else if(bL){
					ret.append(col);
					ret.append(desc).append("group").append(sep).append(lfn.gowner);
					ret.append(desc).append("permissions").append(sep).append(FileSystemUtils.getFormatedTypeAndPerm(lfn));
					ret.append(desc).append("date").append(sep).append(sdt.format(lfn.ctime));
					ret.append(desc).append("name").append(sep).append(lfn.getFileName());

					if(bF && (lfn.type == 'd'))
						ret.append('/');

					ret.append(desc).append("user").append(sep).append(lfn.owner);
					ret.append(desc).append("path").append(sep).append(lfn.getParentName());
					ret.append(desc).append("md5").append(sep).append(lfn.md5);
					ret.append(desc).append("size").append(sep).append(lfn.size);
				}
				else{
					ret.append(col);
					ret.append(desc).append("name").append(sep).append(lfn.getFileName());
				
					if(bF && (lfn.type == 'd'))
						ret.append('/');

					ret.append(desc).append("path").append(sep).append(lfn.getParentName());
				}
			}

			return ret.toString();
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

			alPaths = new ArrayList<>(options.nonOptionArguments().size());
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


	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("\n { JAliEnCommandls received\n");
		sb.append("Arguments: ");

		if(bL) sb.append(" -l ");
		if(bA) sb.append(" -a ");
		if(bF) sb.append(" -f ");
		if(bC) sb.append(" -c ");
		if(bB) sb.append(" -b ");

		sb.append("}");

		return sb.toString();
	}
}
