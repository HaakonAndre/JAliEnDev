package alien.shell.commands;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.logging.Logger;

import lazyj.Log;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.io.Transfer;
import alien.io.protocols.Protocol;
import alien.perl.commands.AlienTime;
import alien.se.SEUtils;
import alien.ui.api.CatalogueApiUtils;
import alien.user.AliEnPrincipal;

public class JAliEnCommandwhereis extends JAliEnCommand {

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
		lsArguments.add("s");
		lsArguments.add("g");
		lsArguments.add("o");
	}

	/**
	 * marker for -help argument
	 */
	private boolean bHelp = false;

	private boolean bG = false;
	private boolean bS = false;
	private boolean bR = false;

	/**
	 * marker for -l argument
	 */
	private List<String> ses = null;
	private List<String> exses = null;

	/**
	 * marker for -a argument
	 */
	private String lfnOrGuid = null;

	/**
	 * marker for -F argument
	 */
	private String outputFileName = null;

	private File outputFile = null;

	/**
	 * @param p
	 *            AliEn principal received from https request
	 * @param al
	 *            all arguments received from SOAP request, contains user,
	 *            current directory and command
	 * @throws Exception
	 */
	public JAliEnCommandwhereis(AliEnPrincipal p, LFN currentDir, 
			final ArrayList<String> al) throws Exception {
		super(p, currentDir, al);
	}

	public void executeCommand() {

		final Iterator<String> it = alArguments.iterator();

		while (it.hasNext()) {
			String arg = it.next();
			if ("-h".equals(arg) || "-help".equals(arg))
				bHelp = true;
			else if ("-s".equals(arg))
				bS = true;
			else if ("-s".equals(arg)) {
				ses = new ArrayList<String>();
				exses = new ArrayList<String>();
				final StringTokenizer st = new StringTokenizer(it.next(), ",");
				while (st.hasMoreTokens()) {
					String se = st.nextToken();
					if (se.indexOf('!') == 0)
						exses.add(se.substring(1));
					else
						ses.add(se);
				}
			} else if ("-g".equals(arg))
				bG = true;
			else if ("-o".equals(arg))
				outputFileName = it.next();
			else
				lfnOrGuid = arg;
		}

		// if (args[1].equals("-h"))
		// bHelp = true;

		if (!bHelp) {
			
			String guid = null;
			
			if (bG) {
				guid = lfnOrGuid;
			} else {
				LFN lfn = CatalogueApiUtils.getLFN(FileSystemUtils.getAbsolutePath(principal.getName(), currentDirectory.getCanonicalName(), lfnOrGuid));
				guid = lfn.guid.toString();
			}
			// what message in case of error?
			if (guid != null) {

				Set<PFN> pfns = CatalogueApiUtils.getPFNs(guid);

				if (bR)
					if (pfns.toArray()[0] != null)
						if (((PFN) pfns.toArray()[0]).pfn.toLowerCase()
								.startsWith("guid://"))
							pfns = CatalogueApiUtils
									.getGUID(
											((PFN) pfns
													.toArray()[0]).pfn
													.substring(8, 44))
									.getPFNs();

				if (!bS)
					System.out.println(AlienTime.getStamp()
							+ "The file "
							+ lfnOrGuid.substring(lfnOrGuid.lastIndexOf("/") + 1,
									lfnOrGuid.length()) + " is in\n");
				for (PFN pfn : pfns) {

					String se = CatalogueApiUtils.getSE(pfn.seNumber).seName;
					if (!bS)
						System.out.println("\t\t SE => " + padRight(se, 30)
								+ " pfn =>" + pfn.pfn + "\n");
				}
			} else {
				System.out.println(AlienTime.getStamp()
						+ "No such file or directory\n");
			}

		} else {
			System.out.println("Usage:\n");
			System.out.println("	whereis [-rg] \n");
			System.out.println("\n");
			System.out.println("Options:\n");
			System.out.println("	-g: Use the lfn as guid\n");
			System.out
					.println("	-r: Resolve links (do not give back pointers to zip archives)\n");
			System.out.println("	-s: Silent\n");

		}
	}

	public File getOutputFile() {
		return outputFile;
	}

}
