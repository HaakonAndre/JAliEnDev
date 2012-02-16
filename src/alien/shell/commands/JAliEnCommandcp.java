package alien.shell.commands;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandcp extends JAliEnBaseCommand {

	private boolean bT = false;

	private int referenceCount = 0;

	private List<String> ses = new ArrayList<String>();
	private List<String> exses = new ArrayList<String>();

	private HashMap<String, Integer> qos = new HashMap<String, Integer>();

	private String source = null;
	private String target = null;

	private File localFile = null;

	// public long timingChallenge = 0;

	// public boolean isATimeChallenge = false;

	@Override
	public void run() {

		if (bT)
			localFile = copyGridToLocal(source, null);

		else if (!localFileSpec(source) && localFileSpec(target)) {

			localFile = new File(getLocalFileSpec(target));

			if (!localFile.exists())
				copyGridToLocal(source, localFile);
			else if (!isSilent()) {
				out.printErrln("A local file already exists with this name.");
			}
			else {
				IOException ex = new IOException("A local file already exists with this name: " + target);

				throw new IOError(ex);
			}

		}
		else if (localFileSpec(source) && !localFileSpec(target)) {

			File sourceFile = new File(getLocalFileSpec(source));
			if (!targetLFNExists(target))
				if (sourceFile.exists())
					copyLocalToGrid(sourceFile, target);
				else if (!isSilent())
					out.printErrln("A local file with this name does not exists.");
				else {
					IOException ex = new IOException("Local file " + target + " doesn't exist");

					throw new IOError(ex);
				}

		}
		else if (!targetLFNExists(target)) {

			localFile = copyGridToLocal(source, null);
			if (localFile != null && localFile.exists() && localFile.length() > 0)
				if (copyLocalToGrid(localFile, target))
					if (!isSilent())
						out.printOutln("Copy successful.");
					else if (!isSilent())
						out.printErrln("Could not copy to the target.");
					else {
						IOException ex = new IOException("Could not copy to the target: " + target);

						throw new IOError(ex);
					}

				else if (!isSilent()) {
					out.printErrln("Could not get the source.");
				}
				else {
					IOException ex = new IOException("Could not get the source: " + source);

					throw new IOError(ex);
				}

		}

		if (out != null && out.isRootPrinter())
			out.setReturnArgs(deserializeForRoot());
	}

	/**
	 * @return local File after get/pull
	 */
	protected File getOutputFile() {
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
		private File output = null;

		/**
		 * @param protocol
		 * @param source
		 * @param target
		 */
		ProtocolAction(final Protocol protocol, final PFN source, final File target) {
			proto = protocol;
			file = target;
			pfn = source;
		}

		@Override
		public void run() {
			try {
				output = proto.get(pfn, file);
			}
			catch (IOException e) {
				output = null;
			}
		}

		/**
		 * @return local output file
		 */
		public File getFile() {
			return output;
		}
	}

	private boolean targetLFNExists(String targetLFN) {
		final LFN currentDir = commander.getCurrentDir();

		final LFN tLFN = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(),
			currentDir != null ? currentDir.getCanonicalName() : null, targetLFN));

		if (tLFN != null) {
			if (!isSilent())
				out.printErrln("The target LFN already exists.");

			return true;
		}
		return false;

	}

	/**
	 * Copy a Grid file to a local file
	 * 
	 * @param sourceLFN Grid filename
	 * @param toLocalFile local file
	 * @return local target file
	 */
	public File copyGridToLocal(final String sourceLFN, final File toLocalFile) {
		File targetLocalFile = toLocalFile;
		
		final LFN currentDir = commander.getCurrentDir();

		LFN lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(),
			currentDir != null ? currentDir.getCanonicalName() : null, sourceLFN));

		if (lfn == null) {
			if (!isSilent())
				out.printErrln("Could not get the file's LFN: " + sourceLFN);
			return null;
		}

		List<PFN> pfns = commander.c_api.getPFNsToRead(lfn, ses, exses);

		if (pfns != null) {

			if (referenceCount == 0)
				referenceCount = pfns.size();

			for (PFN pfn : pfns) {
				List<Protocol> protocols = Transfer.getAccessProtocols(pfn);
				for (final Protocol protocol : protocols) {
					ProtocolAction pA = new ProtocolAction(protocol, pfn, targetLocalFile);
					try {
						pA.start();
						while (pA.isAlive()) {
							Thread.sleep(500);
							if (!isSilent())
								out.pending();
						}

						if (pA.getFile() != null && pA.getFile().exists() && pA.getFile().length() > 0) {
							targetLocalFile = pA.getFile();

							if (!isSilent())
								out.printOutln("Downloaded file to " + pA.getFile().getCanonicalPath());
							
							break;
						}
					}
					catch (Exception e) {
						e.printStackTrace();
					}

				}
				if (targetLocalFile != null && targetLocalFile.exists())
					break;
			}

			if (targetLocalFile != null && targetLocalFile.exists() && targetLocalFile.length() > 0)
				return targetLocalFile;

			// out.printErrln("pfns not null, but error.");
			// return null;
		}

		if (!isSilent())
			out.printErrln("Could not get the file.");
		return null;
	}

	private static final ExecutorService UPLOAD_THREAD_POOL =  new ThreadPoolExecutor(0, Integer.MAX_VALUE, 2L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());

	/**
	 * Copy a local file to the Grid
	 * 
	 * @param sourceFile local filename
	 * @param targetLFN Grid filename
	 * @return status of the upload
	 */
	public boolean copyLocalToGrid(final File sourceFile, final String targetLFN) {
		if (!sourceFile.exists() || !sourceFile.isFile() || !sourceFile.canRead()) {
			if (!isSilent())
				out.printErrln("Could not get the local file: " + sourceFile.getAbsolutePath());
			else {
				final IOException ex = new IOException("Could not get the local file: " + sourceFile.getAbsolutePath());

				throw new IOError(ex);
			}

			return false;
		}

		List<PFN> pfns = null;

		final LFN currentDir = commander.getCurrentDir();

		final LFN lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), currentDir != null ? currentDir.getCanonicalName() : null, targetLFN), true);

		final GUID guid;

		try {
			guid = GUIDUtils.createGuid(sourceFile, commander.user);
		}
		catch (IOException e) {
			if (!isSilent())
				out.printErrln("Couldn't create the GUID.");
			else {
				IOException ex = new IOException("Couldn't create the GUID based on " + sourceFile);

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
		exses.addAll(ses);
		qos.clear();

		if (pfns == null || pfns.size() == 0) {
			if (!isSilent())
				out.printErrln("Couldn't get any access ticket.");
			else {
				IOException ex = new IOException("Couldn't get any access tickets " + sourceFile);

				throw new IOError(ex);
			}

			return false;
		}

		if (referenceCount == 0)
			referenceCount = pfns.size();

		final Vector<String> envelopes = new Vector<String>(pfns.size());
		final Vector<String> registerPFNs = new Vector<String>(pfns.size());

		final ArrayList<Future<?>> futures = new ArrayList<Future<?>>(pfns.size());

		for (final PFN pfn : pfns) {
			final Future<?> f = UPLOAD_THREAD_POOL.submit(new Runnable() {
				@Override
				public void run() {
					uploadPFN(lfn, guid, sourceFile, envelopes, pfn);
				}
			});

			futures.add(f);
		}

		long lastReport = System.currentTimeMillis();

		boolean allDone;

		do {
			allDone = true;

			for (final Future<?> f : futures) {
				if (!f.isDone()) {
					allDone = false;
					break;
				}
			}

			if (!allDone) {
				if (System.currentTimeMillis() - lastReport > 500 && !isSilent()) {
					out.pending();

					lastReport = System.currentTimeMillis();
				}

				try {
					Thread.sleep(50);
				}
				catch (InterruptedException ie) {
					// ignore
				}
			}
		}
		while (!allDone);

		if (envelopes.size() != 0) {
			final List<PFN> registeredPFNs = commander.c_api.registerEnvelopes(envelopes);

			if (!isSilent() && (registeredPFNs == null || registeredPFNs.size() != envelopes.size())) {
				out.printErrln("From the " + envelopes.size() + " replica with tickets only "
					+ (registeredPFNs != null ? String.valueOf(registeredPFNs.size()) : "null") + " were registered");
			}
		}

		if (registerPFNs.size() != 0) {
			final List<PFN> registeredPFNs = commander.c_api.registerEnvelopes(registerPFNs);

			if (!isSilent() && (registeredPFNs == null || registeredPFNs.size() != registerPFNs.size())) {
				out.printErrln("From the " + registerPFNs.size() + " pfns only "
					+ (registeredPFNs != null ? String.valueOf(registeredPFNs.size()) : "null") + " were registered");
			}
		}

		if (referenceCount == (envelopes.size() + registerPFNs.size())) {
			if (!isSilent())
				out.printOutln("File successfully uploaded.");

			return true;
		}
		else if ((envelopes.size() + registerPFNs.size()) > 0) {
			if (!isSilent())
				out.printErrln("Only " + (envelopes.size() + registerPFNs.size()) + " PFNs could be uploaded");
		}
		else {
			if (!isSilent())
				out.printOutln("Upload failed, sorry!");
			else {
				IOException ex = new IOException("Upload failed");

				throw new IOError(ex);
			}

		}
		return false;

	}

	/**
	 * @param lfn
	 * @param guid
	 * @param sourceFile
	 * @param envelopes
	 * @param initialPFN
	 */
	void uploadPFN(final LFN lfn, final GUID guid, final File sourceFile, final Vector<String> envelopes,
		final PFN initialPFN) {
		boolean failOver;

		PFN pfn = initialPFN;

		do {
			failOver = false;

			final List<Protocol> protocols = Transfer.getAccessProtocols(pfn);

			String targetLFNResult = null;

			for (final Protocol protocol : protocols) {
				try {
					if (!isSilent())
						out.printOutln("Uploading file " + sourceFile.getCanonicalPath() + " to " + pfn.getPFN());

					try {
						targetLFNResult = protocol.put(pfn, sourceFile);
					}
					catch (IOException ioe) {
						// ignore, will try next protocol or fetch another
						// replica to replace this one
					}
				}
				catch (Exception e) {
					// e.printStackTrace();
				}

				if (targetLFNResult != null)
					break;
			}

			if (targetLFNResult != null) {
				if (!isSilent()){
					out.printOutln("Successfully uploaded " + sourceFile.getAbsolutePath() + " to " + pfn.getPFN()+"\n"+targetLFNResult);
				}
				
				if (pfn.ticket != null && pfn.ticket.envelope != null
					&& pfn.ticket.envelope.getSignedEnvelope() != null) {
					if (pfn.ticket.envelope.getEncryptedEnvelope() == null)
						envelopes.add(targetLFNResult);
					else
						envelopes.add(pfn.ticket.envelope.getSignedEnvelope());
				}
			}
			else {
				failOver = true;

				if (!isSilent())
					out.printErrln("Error uploading file to SE: " + commander.c_api.getSE(pfn.seNumber).getName());

				synchronized (exses) {
					SE se = commander.c_api.getSE(pfn.seNumber);

					final HashMap<String, Integer> replacementQoS = new HashMap<String, Integer>();

					String qosType = "disk";

					if (se.qos.size() > 0)
						qosType = se.qos.iterator().next();

					replacementQoS.put(qosType, Integer.valueOf(1));

					final List<PFN> newPFNtoTry = commander.c_api.getPFNsToWrite(lfn, guid, ses, exses, qos);

					if (newPFNtoTry != null && newPFNtoTry.size() > 0) {
						pfn = newPFNtoTry.get(0);

						se = commander.c_api.getSE(pfn.seNumber);

						exses.add(se.getName());
					}
					else
						pfn = null;
				}
			}
		}
		while (failOver && pfn != null);
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		if (!isSilent()) {
			out.printOutln();
			out.printOutln(helpUsage("cp",
				"[-options] < file:///localfile /gridfile >  |  < /gridfile file:///localfile >  |  < -t /gridfile >"));
			out.printOutln(helpStartOptions());
			out.printOutln(helpOption("-g", "get by GUID"));
			out.printOutln(helpOption("-S", "[se[,se2[,!se3[,qos:count]]]]"));
			out.printOutln(helpOption("-t", "create a local temp file"));
			out.printOutln(helpOption("-silent", "execute command silently"));
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

	private static boolean localFileSpec(final String file) {
		return file.startsWith("file:");
	}

	private static String getLocalFileSpec(final String file) {
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
	 * @param alArguments the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandcp(JAliEnCOMMander commander, UIPrintWriter out, final ArrayList<String> alArguments)
		throws OptionException {
		super(commander, out, alArguments);
		try {
			final OptionParser parser = new OptionParser();

			parser.accepts("S").withRequiredArg();
			parser.accepts("g");
			parser.accepts("t");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			if ((options.nonOptionArguments().size() != 2)
				&& !(options.nonOptionArguments().size() == 1 && options.has("t"))) {
				printHelp();
				return;
			}

			if (options.has("S") && options.hasArgument("S")) {

				if (((String) options.valueOf("S")) != null) {
					final StringTokenizer st = new StringTokenizer((String) options.valueOf("S"), ",");
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
						}
						else if (spec.contains(":")) {// a qosTag:count spec
							try {

								int c = Integer.parseInt(spec.substring(spec.indexOf(':') + 1));
								if (c > 0) {
									qos.put(spec.substring(0, spec.indexOf(':')), Integer.valueOf(c));
									referenceCount = referenceCount + c;
								}
								else
									throw new JAliEnCommandException();

							}
							catch (Exception e) {
								throw new JAliEnCommandException();
							}
						}
						else if (!spec.equals(""))
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

		}
		catch (OptionException e) {
			printHelp();
			throw e;
		}
	}

}
