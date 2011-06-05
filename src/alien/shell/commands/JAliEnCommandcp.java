package alien.shell.commands;

import java.io.File;
import java.io.IOException;
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

//	public long timingChallenge = 0;

//	public boolean isATimeChallenge = false;

	public void execute() {

		final Iterator<String> it = alArguments.iterator();

		while (it.hasNext()) {
			String arg = it.next();
			 if ("-s".equals(arg)) {
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
				System.err
						.println("You have to specify a grid and a local file");

	}
	

	/**
	 * Copy a Grid file to a local file
	 * @param source Grid filename
	 * @param target local file
	 */
	public void copyGridToLocal(String source, File target) {

		List<PFN> pfns = null;

		if (bG) {
			GUID guid = CatalogueApiUtils.getGUID(source);
			pfns = CatalogueApiUtils.getPFNsToRead(JAliEnCOMMander.user, JAliEnCOMMander.site, guid, ses,
					exses);
		} else {
			LFN lfn = CatalogueApiUtils.getLFN(FileSystemUtils.getAbsolutePath(
					JAliEnCOMMander.user.getName(), JAliEnCOMMander.curDir.getCanonicalName(),
					source));
			pfns = CatalogueApiUtils.getPFNsToRead(JAliEnCOMMander.user, JAliEnCOMMander.site, lfn, ses,
					exses);

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
	 * @param source local filename
	 * @param target Grid filename
	 */
	public void copyLocalToGrid(File source, String target) {
		
//		
//		List<PFN> pfns = null;
//		LFN lfn = null;
//		GUID guid = null;
//
//		if (bG) {
//			guid = CatalogueApiUtils.getGUID(target);
//			pfns = CatalogueApiUtils.getPFNsToRead(principal, site, guid, ses,
//					exses);
//		} else {
//			lfn = CatalogueApiUtils.getLFN(FileSystemUtils.getAbsolutePath(
//					principal.getName(), currentDirectory.getCanonicalName(),
//					target));
//			guid = 
//			pfns = CatalogueApiUtils.getPFNsToRead(principal, site, lfn, ses,
//					exses);
//
//		}
//		
//		
//		
//		
//		// statis list of specified SEs
//		for (SE se : ses) {
//			System.out.println("Trying to book writing on static SE: "
//					+ se.getName());
//
//			if (!se.canWrite(user)) {
//				System.err
//						.println("You are not allowed to write to this SE.");
//				continue;
//			}
//
//			try {
//				pfns.add(BookingTable.bookForWriting(principel, lfn, guid,
//						null, jobid, se));
//			} catch (Exception e) {
//				System.out.println("Error for the request on "
//						+ se.getName() + ", message: " + e);
//			}
//		}
//
//		if (p_qosCount > 0) {
//			ses.addAll(exxSes);
//			List<SE> SEs = SEUtils.getClosestSEs(p_site, ses);
//			final Iterator<SE> it = SEs.iterator();
//
//			int counter = 0;
//			while (counter < p_qosCount && it.hasNext()) {
//				SE se = it.next();
//
//				if (!se.canWrite(user))
//					continue;
//
//				System.out
//						.println("Trying to book writing on discoverd SE: "
//								+ se.getName());
//				try {
//					pfns.add(BookingTable.bookForWriting(user, lfn,
//							guid, null, jobid, se));
//				} catch (Exception e) {
//					System.out.println("Error for the request on "
//							+ se.getName() + ", message: " + e);
//					continue;
//				}
//				counter++;
//			}
//
//		}

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
	 * @return <code>false</code>
	 */
	public boolean canRunWithoutArguments(){
		return false;
	}

	/**
	 * the command's silence trigger
	 */
	private boolean silent = false;
	
	/**
	 * set command's silence trigger
	 */
	public void silent(){
		silent = true;
	}
	
	/**
	 * Constructor needed for the command factory in JAliEnCOMMander
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandcp(final ArrayList<String> alArguments){
		super(alArguments);
	}

}
