package alien.shell.commands;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

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

		if (lfnOrGuid != null) {

			List<PFN> pfns = null;

			// long lStart = System.currentTimeMillis();

			if (bG) {
				GUID guid = CatalogueApiUtils.getGUID(lfnOrGuid);
				pfns = CatalogueApiUtils.getPFNsToRead(commander.user,
						commander.site, guid, ses, exses);
			} else {
				LFN lfn = CatalogueApiUtils
						.getLFN(FileSystemUtils.getAbsolutePath(commander.user
								.getName(), commander.getCurrentDir()
								.getCanonicalName(), lfnOrGuid));
				pfns = CatalogueApiUtils.getPFNsToRead(commander.user,
						commander.site, lfn, ses, exses);

			}
			// timingChallenge = (System.currentTimeMillis() - lStart);
			// out.printErrln("jAliEn TIMING CHALLENGE : "+timingChallenge);
			//
			// if(!isATimeChallenge){

			for (PFN pfn : pfns) {

				List<Protocol> protocols = Transfer.getAccessProtocols(pfn);
				for (final Protocol protocol : protocols) {
					try {

						if (outputFileName != null) {
							outputFile = new File(outputFileName);
						}
						outputFile = protocol.get(pfn, outputFile);
						if (!silent)
							out.printOutln("Downloaded file to "
									+ outputFile.getCanonicalPath());

						break;
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				if (outputFile == null || !outputFile.exists())

					out.printErrln("Could not get the file.");
			}
		}
	}

	/**
	 * printout the help info
	 */
	public void printHelp() {

		out.printOutln(AlienTime.getStamp() + "Usage: get  ... ");
		out.printOutln("		-g : get by GUID");
		out.printOutln("		-s : se,se2,!se3,se4,!se5");
		out.printOutln("		-o : outputfilename");
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
	 * Constructor needed for the command factory in commander
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandget(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) {
		super(commander, out,alArguments);

		final OptionParser parser = new OptionParser();
		parser.accepts("g");

		final OptionSet options = parser.parse(alArguments
				.toArray(new String[] {}));
		
		bG = options.has("g");

		if (options.nonOptionArguments().size() != 1)
			printHelp();
		else
			lfnOrGuid = options.nonOptionArguments().get(0);
	}

}
