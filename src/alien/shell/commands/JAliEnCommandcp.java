package alien.shell.commands;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
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
import alien.io.Transfer;
import alien.io.protocols.Protocol;
import alien.se.SE;
import alien.se.SEUtils;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandcp extends JAliEnBaseCommand {

	private boolean bT = false;
	
	private int referenceCount = 0;

	private List<String> ses = new ArrayList<String>();
	private List<String> exses = new ArrayList<String>();				
	
	private HashMap<String,Integer> qos = new HashMap<String,Integer>();


	private String source = null;
	private String target = null;
	
	private File localFile = null;

	// public long timingChallenge = 0;

	// public boolean isATimeChallenge = false;

	@Override
	public void run(){

		if (bT) 
			localFile = copyGridToLocal(source, null);
			
		else if (!localFileSpec(source) && localFileSpec(target)) {
		
			localFile = new File(getLocalFileSpec(target));
			
			if (!localFile.exists())
				copyGridToLocal(source, localFile);
			else
				if(!isSilent()){
					out.printErrln("A local file already exists with this name.");
				}
				else{
					IOException ex = new IOException("A local file already exists with this name: "+target);
					
					throw new IOError(ex);
				}
			
		} else if (localFileSpec(source) && !localFileSpec(target)) {
			
			File sourceFile = new File(getLocalFileSpec(source));
			if (!targetLFNExists(target))
				if (sourceFile.exists())
					copyLocalToGrid(sourceFile, target);
				else
					if(!isSilent())
						out.printErrln("A local file with this name does not exists.");
					else{
						IOException ex = new IOException("Local file "+target+" doesn't exist");
						
						throw new IOError(ex);
					}
			
		} else if (!targetLFNExists(target)){
			
				localFile = copyGridToLocal(source, null);
				if (localFile != null && localFile.exists()
						&& localFile.length() > 0)
					if (copyLocalToGrid(localFile, target))
						if(!isSilent())
							out.printOutln("Copy successful.");
					else
						if(!isSilent())
							out.printErrln("Could not copy to the target.");
						else{
							IOException ex = new IOException("Could not copy to the target: "+target);
							
							throw new IOError(ex);
						}

				else
					if(!isSilent()){
						out.printErrln("Could not get the source.");
					}
					else{
						IOException ex = new IOException("Could not get the source: "+source);
						
						throw new IOError(ex);
					}

		}
		
		if(out!=null && out.isRootPrinter())
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
		final LFN currentDir = commander.getCurrentDir();
		
		final LFN tLFN = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), currentDir!=null ? currentDir.getCanonicalName() : null, targetLFN));
		
		if (tLFN != null){
			if(!isSilent())
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
	 * @param targetLocalFile
	 *            local file
	 * @return local target file
	 */
	public File copyGridToLocal(final String sourceLFN, File targetLocalFile) {
		final LFN currentDir = commander.getCurrentDir();
		
		LFN lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), currentDir!=null ? currentDir.getCanonicalName() : null, sourceLFN));
		
		if (lfn == null) {
			if (!isSilent())
				out.printErrln("Could not get the file's LFN: " + sourceLFN);
			return null;
		}

		List<PFN> pfns = commander.c_api.getPFNsToRead(lfn,
				ses, exses);

		if (pfns != null) {

			if (referenceCount == 0)
				referenceCount = pfns.size();

			for (PFN pfn : pfns) {
				List<Protocol> protocols = Transfer.getAccessProtocols(pfn);
				for (final Protocol protocol : protocols) {
					ProtocolAction pA = new ProtocolAction(protocol, pfn,
							targetLocalFile);
					try {
						pA.start();
						while (pA.isAlive()) {
							Thread.sleep(500);
							if (!isSilent())
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

			//out.printErrln("pfns not null, but error.");
			//return null;
		}
		
		if (!isSilent())
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

		if (!sourceFile.exists() || !sourceFile.isFile()
				|| !sourceFile.canRead()) {
			if (!isSilent())
				out.printErrln("Could not get the local file: " + sourceFile.getAbsolutePath());
			else{
				IOException ex = new IOException("Could not get the local file: " + sourceFile.getAbsolutePath());
				
				throw new IOError(ex);
			}

			return false;
		}

		List<PFN> pfns = null;

		LFN lfn = null;
		GUID guid = null;

		final LFN currentDir = commander.getCurrentDir();
		
		lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(
				commander.user.getName(), currentDir!=null ? currentDir.getCanonicalName() : null, targetLFN), true);

		try {
			guid = GUIDUtils.createGuid(sourceFile, commander.user);

		} catch (IOException e) {
			if (!isSilent())
				out.printErrln("Couldn't create the GUID.");
			else{
				IOException ex = new IOException("Couldn't create the GUID based on "+sourceFile);
				
				throw new IOError(ex);
			}

			return false;
		}

		lfn.guid = guid.guid;
		lfn.size = guid.size;
		lfn.md5 = guid.md5;
		guid.lfnCache = new LinkedHashSet<LFN>(1);
		guid.lfnCache.add(lfn);

		pfns = commander.c_api.getPFNsToWrite(lfn, guid, ses, exses, qos);
		qos = new HashMap<String, Integer>();
		
		if (pfns == null || pfns.size() == 0) {
			if (!isSilent())
				out.printErrln("Couldn't get any access ticket.");
			else{
				IOException ex = new IOException("Couldn't get any access tickets "+sourceFile);
				
				throw new IOError(ex);
			}
			
			return false;
		}

		if (referenceCount == 0)
			referenceCount = pfns.size();

		ArrayList<String> envelopes = new ArrayList<String>(pfns.size());
		ArrayList<String> registerPFNs = new ArrayList<String>(pfns.size());

		boolean failOver = false;

		do {
			failOver = false;
			
			for (final PFN pfn : pfns) {
				final List<Protocol> protocols = Transfer.getAccessProtocols(pfn);
				
				for (final Protocol protocol : protocols) {
					ProtocolAction pA = new ProtocolAction(protocol, sourceFile, pfn);
					
					String targetLFNResult = null;
					
					try {
						pA.start();
						
						if (!isSilent())
							out.printOutln("Uploading file " + sourceFile.getCanonicalPath() + " to " + pfn.getPFN());
						
						while (pA.isAlive()) {
							Thread.sleep(50);
							if (!isSilent())
								out.pending();
						}
						
						targetLFNResult = pA.getReturn();
					} 
					catch (Exception e) {
						// e.printStackTrace();
					}

					if (targetLFNResult != null) {
						if (pfn.ticket != null && pfn.ticket.envelope != null && pfn.ticket.envelope.getSignedEnvelope() != null){
							if (pfn.ticket.envelope.getEncryptedEnvelope() == null)
								envelopes.add(targetLFNResult);
							else
								envelopes.add(pfn.ticket.envelope.getSignedEnvelope());
						}
						
						ignoreSE(commander.c_api.getSE(pfn.seNumber));
					}
					else {
						failOver = true;
						if (!isSilent())
							out.printErrln("Error uploading file to SE: " + commander.c_api.getSE(pfn.seNumber).getName());

						failOver(commander.c_api.getSE(pfn.seNumber),true);
					}
				}
			}

			if (failOver) {
				pfns = commander.c_api.getPFNsToWrite(lfn, guid, ses, exses, qos);
				qos = new HashMap<String, Integer>();
			}

		} while (failOver && pfns != null && pfns.size() > 0);

		if (envelopes.size() != 0){
			final List<PFN> registeredPFNs = commander.c_api.registerEnvelopes(envelopes);
			
			if (registeredPFNs == null || registeredPFNs.size()!=envelopes.size()){
				if (!isSilent())
					out.printErrln("From the "+envelopes.size()+" replica with tickets only "+(registeredPFNs!=null ? String.valueOf(registeredPFNs.size()) : "null")+" were registered");
			}
		}
		
		if (registerPFNs.size() != 0){
			final List<PFN> registeredPFNs = commander.c_api.registerEnvelopes(registerPFNs);

			if (registeredPFNs == null || registeredPFNs.size()!=registerPFNs.size()){
				if (!isSilent())
					out.printErrln("From the "+registerPFNs.size()+" pfns only "+(registeredPFNs!=null ? String.valueOf(registeredPFNs.size()) : "null")+" were registered");
			}
		}

		if (referenceCount == (envelopes.size() + registerPFNs.size())) {
			if (!isSilent())
				out.printOutln("File successfully uploaded.");
			
			return true;
		} 
		else 
			if ((envelopes.size() + registerPFNs.size()) > 0){
				if (!isSilent())
					out.printErrln("Only " + (envelopes.size() + registerPFNs.size()) + " PFNs could be uploaded");
			}
			else{
				if (!isSilent())
					out.printOutln("Upload failed, sorry!");
				else{
					IOException ex = new IOException("Upload failed");
					
					throw new IOError(ex);
				}

			}
		return false;

	}
	
	private void ignoreSE(final SE se){
		ses.remove(se.getName());
		
		if(!exses.contains(se.getName()))
			exses.add(se.getName());		
	}

	private void failOver(final SE se, final boolean write) {
		if (write) {
			String qosType = "disk";
			
			if (se.qos.iterator().hasNext())
				qosType = se.qos.iterator().next();

			final Integer oldValue = qos.get(qosType); 
			
			qos.put(qosType, Integer.valueOf(1 + (oldValue!=null ? oldValue.intValue() : 0)));
		}
	
		ignoreSE(se);
		
		if (!isSilent()){
			for(String s: ses)
				out.printOutln("putting pos: " + s);
				
			for(String s: exses)
				out.printOutln("putting neg: " + s);
		
			for(String k : qos.keySet())
				out.printOutln("putting qos: [" + k+"] query [" + qos.get(k)+"]");
		}
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		if (!isSilent()){
			out.printOutln();
			out.printOutln(helpUsage("cp","[-options] < file:///localfile /gridfile >  |  < /gridfile file:///localfile >  |  < -t /gridfile >"));
			out.printOutln(helpStartOptions());
			out.printOutln(helpOption("-g","get by GUID"));
			out.printOutln(helpOption("-S","[se[,se2[,!se3[,qos:count]]]]"));
			out.printOutln(helpOption("-t","create a local temp file"));
			out.printOutln(helpOption("-silent","execute command silently"));
			out.printOutln();
		}
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
	
	
	private static boolean localFileSpec(final String file){
		return file.startsWith("file:");
	}
	
	private static String getLocalFileSpec(final String file){
		if (file.startsWith("file://"))
			return file.substring(7);
		
		if (file.startsWith("file:"))
			return file.substring(5);
		
		return file;
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

				if (((String) options.valueOf("S")) != null) {
					final StringTokenizer st = new StringTokenizer(
							(String) options.valueOf("S"), ",");
					while (st.hasMoreElements()) {
						String spec = st.nextToken();
						if (spec.contains("::")) {
							if (spec.indexOf("::") != spec.lastIndexOf("::")) { // any
																				// SE
																				// spec

								if (spec.startsWith("!")) // an exSE spec
									exses.add(spec.toUpperCase());
								else {// an SE spec
									ses.add(spec.toUpperCase());
									referenceCount++;
								}
							}
						} else if (spec.contains(":")) {// a qosTag:count spec
							try {

								int c = Integer.parseInt(spec.substring(spec.indexOf(':') + 1));
								if (c > 0) {
									qos.put(spec.substring(0, spec.indexOf(':')), Integer.valueOf(c));
									referenceCount = referenceCount + c;
								} else
									throw new JAliEnCommandException();

							} catch (Exception e) {
								throw new JAliEnCommandException();
							}
						} else if (!spec.equals(""))
							throw new JAliEnCommandException();
					}
				}
			}

			bT = options.has("t");
			
			if (options.has("t") && options.hasArgument("t"))
				out.printOutln("t has val: " + (String) options.valueOf("t"));
			

			source = options.nonOptionArguments().get(0);
			if (!(options.nonOptionArguments().size() == 1 && options.has("t")))
				target = options.nonOptionArguments().get(1);
			
		} catch (OptionException e) {
			printHelp();
			throw e;
		}
	}

}
