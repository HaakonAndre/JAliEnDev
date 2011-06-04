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
import java.util.StringTokenizer;
import java.util.logging.Logger;

import lazyj.Log;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.io.Transfer;
import alien.io.protocols.Protocol;
import alien.perl.commands.AlienTime;
import alien.ui.api.CatalogueApiUtils;
import alien.user.AliEnPrincipal;

public class JAliEnCommandget extends JAliEnCommand {

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
	
	private String site = null;

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
	

	public long timingChallenge = 0;
	
	public boolean isATimeChallenge = false;


	/**
	 * @param p
	 *            AliEn principal received from https request
	 * @param al
	 *            all arguments received from SOAP request, contains user,
	 *            current directory and command
	 * @throws Exception
	 */
	public JAliEnCommandget(AliEnPrincipal p, LFN currentDir, String site,
			final ArrayList<String> al) throws Exception {
		super(p, currentDir, al);
		this.site = site;
	}

	public void executeCommand() {

		final Iterator<String> it = alArguments.iterator();

		while (it.hasNext()) {
			String arg = it.next();
			if ("-h".equals(arg) || "-help".equals(arg))
				bHelp = true;
			else if ("-s".equals(arg))
				bS=true;
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

			if (lfnOrGuid != null) {

				List<PFN> pfns = null;

				long lStart = System.currentTimeMillis();

				if (bG) {
					GUID guid = CatalogueApiUtils.getGUID(lfnOrGuid);
					pfns = CatalogueApiUtils.getPFNsToRead(principal, site, guid, ses,
							exses);
				} else {
					LFN lfn = CatalogueApiUtils.getLFN(FileSystemUtils.getAbsolutePath(principal.getName(), currentDirectory.getCanonicalName(), lfnOrGuid));
					pfns = CatalogueApiUtils.getPFNsToRead(principal, site, lfn, ses,
							exses);

				}
				timingChallenge = (System.currentTimeMillis() - lStart);
				System.err.println("jAliEn TIMING CHALLENGE : "+timingChallenge);
				
				if(!isATimeChallenge){

				for (PFN pfn : pfns) {

					List<Protocol> protocols = Transfer.getAccessProtocols(pfn);
					for (final Protocol protocol : protocols) {
						try {

							if(outputFileName!=null)
								outputFile = new File(outputFileName);
							outputFile = protocol.get(pfn, outputFile);
							if(!bS)	
							System.out.println("Downloaded file to " + outputFile.getCanonicalPath());
							
							break;
						} catch (IOException e) {
							// ignore
						}
					}
				}
				if(!outputFile.exists())
					
					System.out.println("Could not get the file.");
				}
			}
		} else {
			System.out.println(AlienTime.getStamp() + "Usage: get  ... ");
			System.out.println("		-g : get by GUID");
			System.out.println("		-s : se,se2,!se3,se4,!se5");
			System.out.println("		-o : outputfilename");

		}
	}
	
	public File getOutputFile(){
		return outputFile;
	}

}
