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
import alien.catalogue.GUIDUtils;
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

	private boolean bT = false;
	
	/**
	 * marker for -l argument
	 */
	private List<String> ses = new ArrayList<String>();
	private List<String> exses = new ArrayList<String>();
	
	/**
	 * marker for -a argument
	 */
	private String source = null;
	private String target = null;
	
	private File localFile = null;

	// public long timingChallenge = 0;

	// public boolean isATimeChallenge = false;

	public void execute() {
		if (bT) {
			localFile = copyGridToLocal(source, null);
			
		} else if (!source.startsWith("file://") && target.startsWith("file://")) {
			localFile = new File(target.replace("file://", ""));
			if (!localFile.exists())
				copyGridToLocal(source, localFile);
			else
				out.printErrln("A local file already exists with this name.");
			
		} else if (source.startsWith("file://")
				&& !target.startsWith("file://")) {
			File sourceFile = new File(source.replace("file://", ""));
			if (!targetLFNExists(target))
				if (sourceFile.exists())
					copyLocalToGrid(sourceFile, target);
				else
					out.printErrln("A local file with this name does not exists.");
			
		} else if (!targetLFNExists(target)){
				final boolean preSilent=silent;
				silent = true;
				localFile = copyGridToLocal(source, null);
				if (localFile != null && localFile.exists()
						&& localFile.length() > 0)
					if (copyLocalToGrid(localFile, target))
						if(!preSilent)
							out.printOutln("Copy successful.");
					else
						out.printErrln("Could not copy to the target.");
				else
					out.printErrln("Could not get the source.");
		}
	}
	
	/**
	 * @return local File after get/pull
	 */
	protected File getOutputFile(){
		return localFile;
	}

	
	
	/**
	 * @author ron
	 * @since October 5, 2011
	 */
	protected class ProtocolAction extends Thread {
		private final Protocol proto;
		private final File file;
		private final PFN pfn;
		private boolean pull;
		private String back = null;
		private File output = null;
		
		ProtocolAction(final Protocol protocol, final File source, final PFN target){
			proto = protocol;
			file = source;
			pfn = target;
			pull = false;
		}
		
		ProtocolAction(final Protocol protocol, final PFN source, final File target){
			proto = protocol;
			file = target;
			pfn = source;
			pull = true;
		}
		
		public void run() {
			try{
	       if(pull)
	    	  output = proto.get(pfn, file);
	       else
	    	   back =  proto.put(pfn,file);
			} catch (IOException e) {
				e.getStackTrace();
			}
	    }
		/**
		 * @return return string from call
		 */
		public String getReturn(){
			return back;
		}
		/**
		 * @return local output file
		 */
		public File getFile(){
			return output;
		}
	}
	

	private boolean targetLFNExists(String target) {
		LFN tLFN = CatalogueApiUtils.getLFN(FileSystemUtils.getAbsolutePath(
				commander.user.getName(), commander.getCurrentDir()
						.getCanonicalName(), target));
		if (tLFN != null){
			out.printErrln("The target LFN already exists.");
			return true;
		}
		return false;

	}

	/**
	 * Copy a Grid file to a local file
	 * 
	 * @param source
	 *            Grid filename
	 * @param target
	 *            local file
	 * @return local target file
	 */
	public File copyGridToLocal(String source, File target) {

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
				ProtocolAction pA = new ProtocolAction(protocol,pfn, target);
				try {
					pA.start();
					while(pA.isAlive()){
						Thread.sleep(500);
						out.pending();
					}
					target = pA.getFile();
					
					if(!silent)
						out.printOutln("Downloaded file to "
							+ target.getCanonicalPath());

					break;
				} catch (Exception e) {
					e.getStackTrace();
				}
			}
		}
		
		if (target!= null && target.exists() && target.length() > 0)
			return target;
		out.printErrln("Could not get the file.");
		return null;
	}

	/**
	 * Copy a local file to the Grid
	 * 
	 * @param source
	 *            local filename
	 * @param target
	 *            Grid filename
	 * @return status of the upload
	 */
	public boolean copyLocalToGrid(File source, String target) {

		if (!source.exists() || !source.isFile() || !source.canRead()) {
			out.printErrln("Could not get the local file: "
					+ source.getAbsolutePath());
			return false;
		}

		long size = source.length();
		if (size <= 0) {
			out.printErrln("Local file has size zero: "
					+ source.getAbsolutePath());
			return false;
		}
		String md5 = null;
		try {
			md5 = FileSystemUtils.calculateMD5(source);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		if (md5 == null) {
			System.err
					.println("Could not calculate md5 checksum of the local file: "
							+ source.getAbsolutePath());
			return false;
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
			return false;
		} else {
			lfn = CatalogueApiUtils.getLFN(FileSystemUtils.getAbsolutePath(
					commander.user.getName(), commander.getCurrentDir()
							.getCanonicalName(), target), true);
			guid = null;
			// lfn.guid=... for user's specification
			lfn.size = size;
			lfn.md5 = md5;
			
			try {
				guid = GUIDUtils.createGuid(source, commander.user);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			pfns = CatalogueApiUtils.getPFNsToWrite(commander.user,
					commander.site, lfn, guid, ses, exses, null, 0);

		}
		ArrayList<String> envelopes = new ArrayList<String>(pfns.size());
		ArrayList<String> registerPFNs = new ArrayList<String>(pfns.size());
		for (PFN pfn : pfns) {
			List<Protocol> protocols = Transfer.getAccessProtocols(pfn);
			for (final Protocol protocol : protocols) {
				ProtocolAction pA = new ProtocolAction(protocol, source, pfn);
				try {
					pA.start();
					while(pA.isAlive()){
						Thread.sleep(500);
						out.pending();
					}
					
					target = pA.getReturn();
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
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		if (envelopes.size() != 0)
			CatalogueApiUtils.registerEnvelopes(commander.user, envelopes);
		if (registerPFNs.size() != 0)
			CatalogueApiUtils.registerEnvelopes(commander.user, envelopes);

		if (pfns.size() == (envelopes.size() + registerPFNs.size())) {
			if(!silent)
				out.printOutln("File successfully uploaded.");
			return true;
		} else if ((envelopes.size() + registerPFNs.size()) > 0)
			out.printErrln("Only " + (envelopes.size() + registerPFNs.size())
					+ " PFNs could be uploaded");
		else
			out.printOutln("Upload failed, sorry!");
		return false;

	}

	/**
	 * printout the help info
	 */
	public void printHelp() {
		out.printOutln(AlienTime.getStamp()
						+ "Usage: cp  < file:///localfile /gridfile >  |  < /gridfile file:///localfile >  |  < -t /gridfile >");
		out.printOutln("		-g : get by GUID");
		out.printOutln("		-S : [se,se2,!se3,se4,!se5,disk=3,tape=1]");
		out.printOutln("		-s : execute command silent");
		out.printOutln("		-t : create a local temp file.");

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
	 * 
	 * @param commander
	 * @param out
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandcp(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) {
		super(commander, out, alArguments);

		final OptionParser parser = new OptionParser();

		parser.accepts("S");
		parser.accepts("g");
		parser.accepts("t");

		final OptionSet options = parser.parse(alArguments
				.toArray(new String[] {}));

		if ((options.nonOptionArguments().size() != 2) && !(options.nonOptionArguments().size() == 1 && options.has("t"))) {
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
		bT = options.has("t");
		
		source = options.nonOptionArguments().get(0);
		if(!(options.nonOptionArguments().size() == 1 && options.has("t")))
			target = options.nonOptionArguments().get(1);
	}

}
