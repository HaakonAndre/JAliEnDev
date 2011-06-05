package alien.shell.commands;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import lazyj.Log;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.perl.commands.AlienTime;
import alien.ui.api.CatalogueApiUtils;

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
	 * marker for -b argument
	 */
	private boolean bB = false;

	// public long timingChallenge = 0;

	/**
	 * list of the LFNs that came up by the ls command
	 */
	private List<LFN> directory = null;

	/**
	 * execute the ls
	 */
	public void execute() {

		ArrayList<String> alPaths = new ArrayList<String>(alArguments.size());

		for (String arg : alArguments) {
			if ("-b".equals(arg))
				bB = true;
			else if ("-a".equals(arg))
				bA = true;
			else if ("-l".equals(arg))
				bL = true;
			else if ("-F".equals(arg))
				bF = true;
			else
				alPaths.add(arg);
		}

		// long lStart = System.currentTimeMillis();

		int iDirs = alPaths.size();

		if (iDirs == 0)
			alPaths.add(JAliEnCOMMander.curDir.getCanonicalName());

		for (String sPath : alPaths) {
			// listing current directory
			if (!sPath.startsWith("/"))
				sPath = JAliEnCOMMander.curDir.getCanonicalName() + sPath;

			Log.log(Log.INFO, "Spath = \"" + sPath + "\"");

			final LFN entry = CatalogueApiUtils.getLFN(sPath);

			// what message in case of error?
			if (entry != null) {

				if (entry.type == 'd') {
					directory = entry.list();
				} else
					directory = Arrays.asList(entry);

				// if (iDirs != 1) {
				// System.out.println(sPath + "\n");
				// }

				for (LFN localLFN : directory) {

					if (!bA && localLFN.getFileName().startsWith("."))
						continue;

					String ret = "";
					if (bB) {
						if (localLFN.type == 'd')
							continue;
						ret += localLFN.guid.toString().toUpperCase() + "	"
								+ localLFN.getName();
					} else {

						if (bL)
							ret += FileSystemUtils
									.getFormatedTypeAndPerm(localLFN)
									+ "   "
									+ localLFN.owner
									+ " "
									+ localLFN.gowner
									+ " "
									+ padLeft(String.valueOf(localLFN.size), 12)
									+ " "
									+ format(localLFN.ctime)
									+ "            " + localLFN.getFileName();
						else
							ret += localLFN.getFileName();

						if (bF && (localLFN.type == 'd'))
							ret += "/";
					}
					if (!silent)
						System.out.println(ret);
				}
			} else {
				if (!silent)
					System.out.println("No such file or directory");
			}
		}
		// timingChallenge = (System.currentTimeMillis() - lStart);
		// System.err.println("jAliEn TIMING CHALLENGE : "+ timingChallenge );
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
	public void printHelp() {
		System.out.println(AlienTime.getStamp()
				+ "Usage: ls [-laFn|b|h] [<directory>]");
		System.out.println("		-l : long format");
		System.out.println("		-a : show hidden .* files");
		System.out.println("		-F : add trailing / to directory names");
		System.out.println("		-b : print in guid format");
		System.out.println("		-h : print the help text");
	}

	/**
	 * ls can run without arguments
	 * 
	 * @return <code>true</code>
	 */
	public boolean canRunWithoutArguments() {
		return true;
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
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandls(final ArrayList<String> alArguments) {
		super(alArguments);
	}

}
