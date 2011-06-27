package alien.shell.commands;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
public class JAliEnCommandcp extends JAliEnBaseCommand {

	private boolean bG = false;

	/**
	 * marker for -l argument
	 */
	private List<String> ses = null;
	private List<String> exses = null;

	/**
	 * marker for -a argument
	 */
	private String source = null;
	private String target = null;

	// public long timingChallenge = 0;

	// public boolean isATimeChallenge = false;

	public void execute() {

		if (!source.startsWith("file://") && target.startsWith("file://")) {
			File outFile = new File(target.replace("file://", ""));
			if (!outFile.exists())
				copyGridToLocal(source, outFile);
			else
				System.err
						.println("A local file already exists with this name.");
		} else if (source.startsWith("file://")
				&& !target.startsWith("file://")) {
			File sourceFile = new File(source.replace("file://", ""));
			if (sourceFile.exists())
				copyLocalToGrid(sourceFile, target);

			else
				System.err
						.println("A local file with this name does not exists.");
		} else
			out.printErrln("You have to specify a grid and a local file");

	}

	/**
	 * Copy a Grid file to a local file
	 * 
	 * @param source
	 *            Grid filename
	 * @param target
	 *            local file
	 */
	public void copyGridToLocal(String source, File target) {

		List<PFN> pfns = null;

		if (bG) {
			GUID guid = CatalogueApiUtils.getGUID(source);
			pfns = CatalogueApiUtils.getPFNsToRead(commander.user,
					commander.site, guid, ses, exses);
		} else {
			LFN lfn = CatalogueApiUtils.getLFN(FileSystemUtils.getAbsolutePath(
					commander.user.getName(), commander.getCurrentDir()
							.getCanonicalName(), source));
			pfns = CatalogueApiUtils.getPFNsToRead(commander.user,
					commander.site, lfn, ses, exses);

		}

		for (PFN pfn : pfns) {

			List<Protocol> protocols = Transfer.getAccessProtocols(pfn);
			for (final Protocol protocol : protocols) {
				try {
					target = protocol.get(pfn, target);
					if (!silent)
						out.printOutln("Downloaded file to "
								+ target.getCanonicalPath());

					break;
				} catch (IOException e) {
					e.getStackTrace();
				}
			}
		}
		if (!target.exists())
			out.printOutln("Could not get the file.");
	}

	/**
	 * Copy a local file to the Grid
	 * 
	 * @param source
	 *            local filename
	 * @param target
	 *            Grid filename
	 */
	public void copyLocalToGrid(File source, String target) {

		if (!source.exists() || !source.isFile() || !source.canRead()) {
			out.printErrln("Could not get the local file: "
					+ source.getAbsolutePath());
			return;
		}

		long size = source.length();
		if (size <= 0) {
			out.printErrln("Local file has size zero: "
					+ source.getAbsolutePath());
			return;
		}
		String md5 = null;
		try {
			md5 = FileSystemUtils.calculateMD5(source);
		} catch (Exception e1) {
		}
		if (md5 == null) {
			System.err
					.println("Could not calculate md5 checksum of the local file: "
							+ source.getAbsolutePath());
			return;
		}

		List<PFN> pfns = null;

		LFN lfn = null;
		GUID guid = null;

		if (bG) {
			guid = CatalogueApiUtils.getGUID(target, true);
			pfns = CatalogueApiUtils.getPFNsToWrite(commander.user,
					commander.site, guid, ses, exses, null, 0);
			guid.size = size;
			guid.md5 = md5;
			out.printErrln("Not working yet...");
			return;
		} else {
			lfn = CatalogueApiUtils.getLFN(FileSystemUtils.getAbsolutePath(
					commander.user.getName(), commander.getCurrentDir()
							.getCanonicalName(), target), true);
			guid = null;
			// lfn.guid=... for user's specification
			lfn.size = size;
			lfn.md5 = md5;

			pfns = CatalogueApiUtils.getPFNsToWrite(commander.user,
					commander.site, lfn, ses, exses, null, 0);

		}
		ArrayList<String> envelopes = new ArrayList<String>(pfns.size());
		ArrayList<String> registerPFNs = new ArrayList<String>(pfns.size());

		for (PFN pfn : pfns) {

			List<Protocol> protocols = Transfer.getAccessProtocols(pfn);
			for (final Protocol protocol : protocols) {
				try {
					target = protocol.put(pfn, source);
					if (!silent)
						out.printOutln("Uploading file "
								+ source.getCanonicalPath() + " to "
								+ pfn.getPFN());
					if (target != null) {
						if (pfn.ticket != null
								&& pfn.ticket.envelope != null
								&& pfn.ticket.envelope.getSignedEnvelope() != null)
							if (pfn.ticket.envelope.getEncryptedEnvelope() == null)
								envelopes.add(target);
							else
								envelopes.add(pfn.ticket.envelope
										.getSignedEnvelope());
					}
					break;
				} catch (IOException e) {
					// ignore
				}
			}
		}

		if (envelopes.size() != 0)
			CatalogueApiUtils.registerEnvelopes(commander.user, envelopes);
		if (registerPFNs.size() != 0)
			CatalogueApiUtils.registerEnvelopes(commander.user, envelopes);

		if (pfns.size() == (envelopes.size() + registerPFNs.size()))
			out.printOutln("File successfully uploaded.");
		else if ((envelopes.size() + registerPFNs.size()) > 0)
			out.printErrln("Only " + (envelopes.size() + registerPFNs.size())
					+ " PFNs could be uploaded");
		else
			out.printOutln("Upload failed, sorry!");

	}

	/**
	 * printout the help info
	 */
	public void printHelp() {
		System.out
				.println(AlienTime.getStamp()
						+ "Usage: cp  <file:///localfile /gridfile> or  </gridfile file:///localfile>");
		out.printOutln("		-g : get by GUID");
		out.printOutln("		-S : [se,se2,!se3,se4,!se5,disk=3,tape=1]");
		out.printOutln("		-s : execute command silent");

	}

	/**
	 * cp cannot run without arguments
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
	 * @param commander 
	 * @param out 
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandcp(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) {
		super(commander, out,alArguments);
		

		final OptionParser parser = new OptionParser();

		parser.accepts("S");
		parser.accepts("g");

		final OptionSet options = parser.parse(alArguments
				.toArray(new String[] {}));

		if (options.nonOptionArguments().size() != 2) {
			printHelp();
			return;
		}

		if (options.has("S") && options.hasArgument("S")) {
			final StringTokenizer st = new StringTokenizer(
					((String) options.valueOf("S")), ",");
			while (st.hasMoreTokens()) {
				String se = st.nextToken();
				if (se.indexOf('!') == 0)
					exses.add(se.substring(1));
				else
					ses.add(se);
			}
		}
		bG = options.has("g");

		source = options.nonOptionArguments().get(0);
		target = options.nonOptionArguments().get(1);
	}

}
