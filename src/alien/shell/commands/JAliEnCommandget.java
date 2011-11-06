package alien.shell.commands;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
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
	 * marker for -x argument
	 */
	private boolean bX = false;

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
	@Override
	public void run() {
			
		if (lfnOrGuid != null) {

			List<PFN> pfns = null;

			// long lStart = System.currentTimeMillis();
			String md5 = null;
			
			GUID guid = null;
			LFN lfn = null;
			

			if (bG) {
				guid = commander.c_api.getGUID(lfnOrGuid);
				md5 = guid.md5;
				
			} else {
				lfn = commander.c_api
						.getLFN(FileSystemUtils.getAbsolutePath(commander.user
								.getName(), commander.getCurrentDir()
								.getCanonicalName(), lfnOrGuid));
				md5 = lfn.md5;
				if(lfn.type=='d'){
					out.printErrln("["+ lfnOrGuid + "] is a directory!");
					lfn=null;
				}
					
			}
			
			if(guid!=null || lfn!=null)
			{

				if (!bX)
					outputFile = commander.checkLocalFileCache(md5);

				if (outputFile == null || !outputFile.exists()) {

					if (guid != null)
						pfns = commander.c_api.getPFNsToRead(commander.site, guid, ses, exses);
					else
						pfns = commander.c_api.getPFNsToRead(commander.site, lfn, ses, exses);

					try {
						for (PFN pfn : pfns) {
							List<Protocol> protocols = Transfer
									.getAccessProtocols(pfn);
							for (final Protocol protocol : protocols) {

								try {

									if (outputFileName != null) {
										outputFile = new File(outputFileName);
									}

									outputFile = protocol.get(pfn, outputFile);

									commander.cashFile(md5, outputFile);
									break;
								} catch (IOException e) {
									e.printStackTrace();
								}
							}
							if (outputFile == null || !outputFile.exists())

								out.printErrln("Could not get the file.");
						}
					} catch (Exception e) {
						out.printErrln("Problems parsing the PFNs of this file.");
					}
				}

				if (outputFile.isFile() && outputFile.exists() && !isSilent())
					try {
						out.printOutln("Downloaded file to "
								+ outputFile.getCanonicalPath());
					} catch (IOException e) {
						e.printStackTrace();
					}

			}
		}
	}

	/**
	 * printout the help info
	 */
	@Override
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
	@Override
	public boolean canRunWithoutArguments() {
		return false;
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
	public JAliEnCommandget(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
		try {

			final OptionParser parser = new OptionParser();
			parser.accepts("g");
			parser.accepts("x");

			final OptionSet options = parser.parse(alArguments
					.toArray(new String[] {}));

			bG = options.has("g");
			bX = options.has("x");

			if (options.nonOptionArguments().size() != 1)
				printHelp();
			else
				lfnOrGuid = options.nonOptionArguments().get(0);
		} catch (OptionException e) {
			printHelp();
			throw e;
		}
	}

}
