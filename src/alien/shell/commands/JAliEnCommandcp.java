package alien.shell.commands;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.io.IOUtils;
import alien.io.Transfer;
import alien.io.protocols.Protocol;

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

	@Override
	public void run() {
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
				final boolean preSilent = isSilent();
				silent();
				
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
		if (out.isRootPrinter())
			out.setReturnArgs(deserializeForRoot());
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
	private static final class ProtocolAction extends Thread {
		private final Protocol proto;
		private final File file;
		private final PFN pfn;
		private boolean pull;
		private String back = null;
		private File output = null;
		
		/**
		 * @param protocol
		 * @param source
		 * @param target
		 */
		ProtocolAction(final Protocol protocol, final File source, final PFN target){
			proto = protocol;
			file = source;
			pfn = target;
			pull = false;
		}
		
		/**
		 * @param protocol
		 * @param source
		 * @param target
		 */
		ProtocolAction(final Protocol protocol, final PFN source, final File target){
			proto = protocol;
			file = target;
			pfn = source;
			pull = true;
		}
		
		@Override
		public void run() {
			try {
				if (pull)
					output = proto.get(pfn, file);
				else
					back = proto.put(pfn, file);
			} catch (IOException e) {
				if(pull)
					output=null;
				else
					back=null;
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
	

	private boolean targetLFNExists(String targetLFN) {
		LFN tLFN = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(
				commander.user.getName(), commander.getCurrentDir()
						.getCanonicalName(), targetLFN));
		if (tLFN != null){
			out.printErrln("The target LFN already exists.");
			return true;
		}
		return false;

	}

	/**
	 * Copy a Grid file to a local file
	 * 
	 * @param sourceLFN
	 *            Grid filename
	 * @param targetFile
	 *            local file
	 * @return local target file
	 */
	public File copyGridToLocal(final String sourceLFN, final File targetFile) {

		File targetLocalFile = targetFile;

		List<PFN> pfns = null;

		if (bG) {
			GUID guid = commander.c_api.getGUID(sourceLFN);
			if(guid==null){
				out.printErrln("Could not get the file's GUID: " + sourceLFN);
				return null;
			}
			pfns = commander.c_api.getPFNsToRead(commander.site, guid, ses,
					exses);
		} else {
			LFN lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(
					commander.user.getName(), commander.getCurrentDir()
							.getCanonicalName(), sourceLFN));
			if(lfn==null){
				out.printErrln("Could not get the file's LFN: " + sourceLFN);
				return null;
			}
			pfns = commander.c_api.getPFNsToRead(commander.site, lfn, ses,
					exses);

		}
		if (pfns != null) {
			for (PFN pfn : pfns) {
				List<Protocol> protocols = Transfer.getAccessProtocols(pfn);
				for (final Protocol protocol : protocols) {
					ProtocolAction pA = new ProtocolAction(protocol, pfn,
							targetFile);
					try {
						System.out.println("Trying to get file over: "
								+ commander.c_api.getSE(pfn.seNumber).seName);
						pA.start();
						while (pA.isAlive()) {
							Thread.sleep(500);
							out.pending();
						}

						if (pA.getFile() != null && pA.getFile().exists()
								&& pA.getFile().length() > 0) {
							targetLocalFile = pA.getFile();

							if (!isSilent())
								out.printOutln("Downloaded file to "
									+ pA.getFile().getCanonicalPath());
							System.out.println("Successful.");
							break;
						}
					} catch (Exception e) {
						e.printStackTrace();
					}

				}
				if (targetLocalFile != null && targetLocalFile.exists())
					break;
			}

			if (targetLocalFile != null && targetLocalFile.exists()
					&& targetLocalFile.length() > 0)
				return targetLocalFile;
		}
		out.printErrln("Could not get the file.");
		return null;
	}

	/**
	 * Copy a local file to the Grid
	 * 
	 * @param sourceFile
	 *            local filename
	 * @param targetLFN
	 *            Grid filename
	 * @return status of the upload
	 */
	public boolean copyLocalToGrid(final File sourceFile, final String targetLFN) {

		if (!sourceFile.exists() || !sourceFile.isFile() || !sourceFile.canRead()) {
			out.printErrln("Could not get the local file: "
					+ sourceFile.getAbsolutePath());
			return false;
		}

		long size = sourceFile.length();
		if (size <= 0) {
			out.printErrln("Local file has size zero: "
					+ sourceFile.getAbsolutePath());
			return false;
		}
		String md5 = null;
		
		try {
			md5 = IOUtils.getMD5(sourceFile);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
		if (md5 == null) {
			System.err.println("Could not calculate md5 checksum of the local file: "+ sourceFile.getAbsolutePath());
			return false;
		}

		List<PFN> pfns = null;

		LFN lfn = null;
		GUID guid = null;

		if (bG) {
			guid = commander.c_api.getGUID(targetLFN, true);
			pfns = commander.c_api.getPFNsToWrite(commander.site, guid, ses, exses, null, 0);
			guid.size = size;
			guid.md5 = md5;
			out.printErrln("Not working yet...");
			return false;
		}
		
		lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(
				commander.user.getName(), commander.getCurrentDir()
						.getCanonicalName(), targetLFN), true);
		
		// lfn.guid=... for user's specification
		lfn.size = size;
		lfn.md5 = md5;
		
		try {
			guid = GUIDUtils.createGuid(sourceFile, commander.user);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		pfns = commander.c_api.getPFNsToWrite(
				commander.site, lfn, guid, ses, exses, null, 0);
		ArrayList<String> envelopes = new ArrayList<String>(pfns.size());
		ArrayList<String> registerPFNs = new ArrayList<String>(pfns.size());
		for (PFN pfn : pfns) {
			List<Protocol> protocols = Transfer.getAccessProtocols(pfn);
			for (final Protocol protocol : protocols) {
				ProtocolAction pA = new ProtocolAction(protocol, sourceFile, pfn);
				try {
					pA.start();
					while(pA.isAlive()){
						Thread.sleep(500);
						out.pending();
					}
					
					String targetLFNResult = pA.getReturn();
					
					if (!isSilent())
						out.printOutln("Uploading file "
								+ sourceFile.getCanonicalPath() + " to "
								+ pfn.getPFN());
					if (targetLFNResult != null) {
						if (pfn.ticket != null
								&& pfn.ticket.envelope != null
								&& pfn.ticket.envelope.getSignedEnvelope() != null)
							if (pfn.ticket.envelope.getEncryptedEnvelope() == null)
								envelopes.add(targetLFNResult);
							else
								envelopes.add(pfn.ticket.envelope.getSignedEnvelope());
					}
					break;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		if (envelopes.size() != 0)
			commander.c_api.registerEnvelopes(envelopes);
		if (registerPFNs.size() != 0)
			commander.c_api.registerEnvelopes(envelopes);

		if (pfns.size() == (envelopes.size() + registerPFNs.size())) {
			if(!isSilent())
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
	@Override
	public void printHelp() {
		
		out.printOutln();
		out.printOutln(helpUsage("cp","[-options] < file:///localfile /gridfile >  |  < /gridfile file:///localfile >  |  < -t /gridfile >"));
		out.printOutln(helpStartOptions());
		out.printOutln(helpOption("-g","get by GUID"));
		out.printOutln(helpOption("-S","[se[,se2[,!se3[,se4]]]]"));
		out.printOutln(helpOption("-t","create a local temp file"));
		out.printOutln(helpOption("-silent","execute command silently"));
		out.printOutln();
	}

	/**
	 * cp cannot run without arguments
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
	public JAliEnCommandcp(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
		try {
			final OptionParser parser = new OptionParser();

			parser.accepts("S").withRequiredArg();
			parser.accepts("g");
			parser.accepts("t");

			final OptionSet options = parser.parse(alArguments
					.toArray(new String[] {}));

			if ((options.nonOptionArguments().size() != 2)
					&& !(options.nonOptionArguments().size() == 1 && options
							.has("t"))) {
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
				System.out.println("ses: " + ses);
				System.out.println("exses: " + exses);
				
			}
			bG = options.has("g");
			bT = options.has("t");

			source = options.nonOptionArguments().get(0);
			if (!(options.nonOptionArguments().size() == 1 && options.has("t")))
				target = options.nonOptionArguments().get(1);
			
			System.out.println("source: " + source);
			System.out.println("target: " + target);
			
			
		} catch (OptionException e) {
			printHelp();
			throw e;
		}
	}

}
