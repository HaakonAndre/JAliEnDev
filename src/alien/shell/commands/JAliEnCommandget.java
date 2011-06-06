package alien.shell.commands;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import alien.api.catalogue.CatalogueApiUtils;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.io.Transfer;
import alien.io.protocols.Protocol;
import alien.perl.commands.AlienTime;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandget extends JAliEnBaseCommand {

	/**
	 * marker for -g argument
	 */
	private boolean bG = false;

	/**
	 * list of SEs to priorize
	 */
	private List<String> ses = null;

	/**
	 * list of SEs to depriorize
	 */
	private List<String> exses = null;

	/**
	 * The LFN or GUID to get
	 */
	private String lfnOrGuid = null;

	/**
	 * The name of the output file
	 */
	private String outputFileName = null;

	/**
	 * The output file after the execution
	 */
	private File outputFile = null;

	// public long timingChallenge = 0;

	// public boolean isATimeChallenge = false;

	/**
	 * returns the file that contains the output after the execution
	 * 
	 * @return the output file
	 */
	public File getOutputFile() {
		return outputFile;
	}

	/**
	 * execute the get
	 */
	public void execute() {

		final Iterator<String> it = alArguments.iterator();

		while (it.hasNext()) {
			String arg = it.next();
			if ("-S".equals(arg)) {
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

		if (lfnOrGuid != null) {

			List<PFN> pfns = null;

			// long lStart = System.currentTimeMillis();

			if (bG) {
				GUID guid = CatalogueApiUtils.getGUID(lfnOrGuid);
				pfns = CatalogueApiUtils.getPFNsToRead(JAliEnCOMMander.user,
						JAliEnCOMMander.site, guid, ses, exses);
			} else {
				LFN lfn = CatalogueApiUtils.getLFN(FileSystemUtils
						.getAbsolutePath(JAliEnCOMMander.user.getName(),
								JAliEnCOMMander.getCurrentDir().getCanonicalName(),
								lfnOrGuid));
				pfns = CatalogueApiUtils.getPFNsToRead(JAliEnCOMMander.user,
						JAliEnCOMMander.site, lfn, ses, exses);

			}
			// timingChallenge = (System.currentTimeMillis() - lStart);
			// System.err.println("jAliEn TIMING CHALLENGE : "+timingChallenge);
			//
			// if(!isATimeChallenge){

			for (PFN pfn : pfns) {

				List<Protocol> protocols = Transfer.getAccessProtocols(pfn);
				for (final Protocol protocol : protocols) {
					try {

						if (outputFileName != null)
							outputFile = new File(outputFileName);
						outputFile = protocol.get(pfn, outputFile);
						if (!silent)
							System.out.println("Downloaded file to "
									+ outputFile.getCanonicalPath());

						break;
					} catch (IOException e) {
						// ignore
					}
					// }
				}
				if (!outputFile.exists())

					System.out.println("Could not get the file.");
			}
		}
	}

	/**
	 * printout the help info
	 */
	public void printHelp() {

		System.out.println(AlienTime.getStamp() + "Usage: get  ... ");
		System.out.println("		-g : get by GUID");
		System.out.println("		-s : se,se2,!se3,se4,!se5");
		System.out.println("		-o : outputfilename");
	}

	/**
	 * get cannot run without arguments
	 * 
	 * @return <code>false</code>
	 */
	public boolean canRunWithoutArguments() {
		return false;
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
	public JAliEnCommandget(final ArrayList<String> alArguments) {
		super(alArguments);
	}

}
