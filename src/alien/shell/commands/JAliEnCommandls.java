package alien.shell.commands;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import lazyj.Log;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.config.ConfigUtils;
import alien.perl.commands.AlienTime;
import alien.ui.api.CatalogueApiUtils;
import alien.user.AliEnPrincipal;

public class JAliEnCommandls extends JAliEnCommand {

	/**
	 * ls command arguments : -help/l/a
	 */
	private static ArrayList<String> lsArguments = new ArrayList<String>();

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils
			.getLogger(GUIDUtils.class.getCanonicalName());

	static {
		lsArguments.add("h");
		lsArguments.add("l");
		lsArguments.add("a");
		lsArguments.add("F");
		lsArguments.add("b");
	}

	/**
	 * marker for -help argument
	 */
	private boolean bHelp = false;

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

	/**
	 * @param p
	 *            AliEn principal received from https request
	 * @param al
	 *            all arguments received from SOAP request, contains user,
	 *            current directory and command
	 * @throws Exception
	 */
	public JAliEnCommandls(AliEnPrincipal p, LFN currentDir, final ArrayList<String> al)
			throws Exception {
		super(p, currentDir, al);
	}

	public void executeCommand() {

		ArrayList<String> alPaths = new ArrayList<String>(alArguments.size());
		
		for (String arg : alArguments) {
			if ("-h".equals(arg) || "-help".equals(arg))
				bHelp = true;
			else if ("-b".equals(arg))
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

		// if (args[1].equals("-h"))
		// bHelp = true;

		if (!bHelp) {

			int iDirs = alPaths.size();

			if (iDirs == 0)
				alPaths.add(currentDirectory.getCanonicalName());

			for (String sPath : alPaths) {
				// listing current directory
				if (!sPath.startsWith("/"))
					sPath = currentDirectory + sPath;

				Log.log(Log.INFO, "Spath = \"" + sPath + "\"");

				final LFN entry = CatalogueApiUtils.getLFN(sPath);

				// what message in case of error?
				if (entry != null) {

					List<LFN> lLFN;

					if (entry.type == 'd') {
						lLFN = entry.list();
					} else
						lLFN = Arrays.asList(entry);

					// if (iDirs != 1) {
					// System.out.println(sPath + "\n");
					// }

					for (LFN localLFN : lLFN) {

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
										+ padLeft(
												String.valueOf(localLFN.size),
												12)
										+ " "
										+ format(localLFN.ctime)
										+ "            "
										+ localLFN.getFileName();
							else
								ret += localLFN.getFileName();

							if (bF && (localLFN.type == 'd'))
								ret += "/";
						}

						System.out.println(ret);
					}
				} else {
					System.out.println("No such file or directory");
				}
			}
		} else {
			System.out.println(AlienTime.getStamp()
					+ "Usage: ls [-laFn|b|h] [<directory>]");
			System.out.println("		-l : long format");
			System.out.println("		-a : show hidden .* files");
			System.out.println("		-F : add trailing / to directory names");
			System.out.println("		-b : print in guid format");
			System.out.println("		-h : print the help text");

		}
	}

	private static final DateFormat formatter = new SimpleDateFormat(
			"MMM dd HH:mm");

	private static synchronized String format(final Date d) {
		return formatter.format(d);
	}


}
