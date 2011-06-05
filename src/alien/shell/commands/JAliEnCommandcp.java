package alien.shell.commands;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.io.Transfer;
import alien.io.protocols.Protocol;
import alien.perl.commands.AlienTime;
import alien.ui.api.CatalogueApiUtils;

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

		final Iterator<String> it = alArguments.iterator();

		while (it.hasNext()) {
			String arg = it.next();
			if ("-S".equals(arg)) {
				ses = new ArrayList<String>();
				exses = new ArrayList<String>();
				final StringTokenizer st = new StringTokenizer(it.next(), ",");
				if (st.hasMoreTokens()) {
					String se = st.nextToken();
					if (se.indexOf('!') == 0)
						exses.add(se.substring(1));
					else
						ses.add(se);
				}
			} else if ("-g".equals(arg))
				bG = true;
			else if (source != null)
				target = arg;
			else
				source = arg;
		}

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
			System.err.println("You have to specify a grid and a local file");

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
			pfns = CatalogueApiUtils.getPFNsToRead(JAliEnCOMMander.user,
					JAliEnCOMMander.site, guid, ses, exses);
		} else {
			LFN lfn = CatalogueApiUtils.getLFN(FileSystemUtils.getAbsolutePath(
					JAliEnCOMMander.user.getName(),
					JAliEnCOMMander.curDir.getCanonicalName(), source));
			pfns = CatalogueApiUtils.getPFNsToRead(JAliEnCOMMander.user,
					JAliEnCOMMander.site, lfn, ses, exses);

		}

		for (PFN pfn : pfns) {

			List<Protocol> protocols = Transfer.getAccessProtocols(pfn);
			for (final Protocol protocol : protocols) {
				try {
					target = protocol.get(pfn, target);
					if (!silent)
						System.out.println("Downloaded file to "
								+ target.getCanonicalPath());

					break;
				} catch (IOException e) {
					// ignore
				}
			}
		}
		if (!target.exists())
			System.out.println("Could not get the file.");
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
			System.err.println("Could not get the local file: " + source.getAbsolutePath());
			return;
		}

		long size = source.length();
		if (size <= 0) {
			System.err.println("Local file has size zero: " + source.getAbsolutePath());
			return;
		}
		String md5 = null;
		try {
			md5 = calculateMD5(source);
		} catch (Exception e1) {
		}
		if (md5 == null) {
			System.err
					.println("Could not calculate md5 checksum of the local file: " + source.getAbsolutePath());
			return;
		}

		List<PFN> pfns = null;

		LFN lfn = null;
		GUID guid = null;

		if (bG) {
			guid = CatalogueApiUtils.getGUID(target, true);
			pfns = CatalogueApiUtils.getPFNsToWrite(JAliEnCOMMander.user,
					JAliEnCOMMander.site, guid, ses, exses,null,0);
			guid.size = size;
			guid.md5 = md5;
			System.err.println("Not working yet...");
			return;
		} else {
			lfn = CatalogueApiUtils.getLFN(FileSystemUtils.getAbsolutePath(
					JAliEnCOMMander.user.getName(),
					JAliEnCOMMander.curDir.getCanonicalName(), target), true);
			guid = null;
			// lfn.guid=... for user's specification
			lfn.size = size;
			lfn.md5 = md5;

			pfns = CatalogueApiUtils.getPFNsToWrite(JAliEnCOMMander.user,JAliEnCOMMander.site, lfn, ses, exses,null,0);

		}

		for (PFN pfn : pfns) {

			List<Protocol> protocols = Transfer.getAccessProtocols(pfn);
			for (final Protocol protocol : protocols) {
				try {
					target = protocol.put(pfn, source);
					if (!silent)
						System.out.println("Downloaded file to "
								+ source.getCanonicalPath());

					break;
				} catch (IOException e) {
					// ignore
				}
			}
		}
		
		ArrayList<String> envelopes = new ArrayList<String>(pfns.size());
		for (PFN pfn : pfns)
			envelopes.add(pfn.ticket.envelope.getSignedEnvelope()); 
		List<PFN> pfnsok = 	CatalogueApiUtils.registerEnvelopes(JAliEnCOMMander.user, envelopes);
		if(pfns.equals(pfnsok))
			System.out.println("File successfully uploaded.");
		else if(pfnsok!=null && pfnsok.size()>0)
			System.err.println("Only " + pfnsok.size()+ " could be uploaded");
		else 
			System.out.println("Upload failed, sorry!");

	}

	/**
	 * @param file
	 * @return MD5 checksum of the file
	 * @throws Exception
	 */
	public static String calculateMD5(File file) throws Exception {
		MessageDigest md = MessageDigest.getInstance("MD5");
		InputStream fis = new FileInputStream(file);
		byte[] buffer = new byte[8192];
		int read = 0;
		while ((read = fis.read(buffer)) > 0)
			md.update(buffer, 0, read);
		BigInteger bi = new BigInteger(1, md.digest());
		return bi.toString(16);
	}

	/**
	 * printout the help info
	 */
	public void printHelp() {
		System.out
				.println(AlienTime.getStamp()
						+ "Usage: cp  [file:///localfile /gridfile] or  [/gridfile file:///localfile]");
		System.out.println("		-g : get by GUID");
		System.out.println("		-s : se,se2,!se3,se4,!se5,disk=3,tape=1");
		System.out.println("		-o : outputfilename");
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
	 * Constructor needed for the command factory in JAliEnCOMMander
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandcp(final ArrayList<String> alArguments) {
		super(alArguments);
	}

}
