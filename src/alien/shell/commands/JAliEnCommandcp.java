package alien.shell.commands;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.PFNforWrite;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.io.Transfer;
import alien.io.protocols.Protocol;
import alien.io.protocols.TempFileManager;
import alien.se.SE;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandcp extends JAliEnBaseCommand {

	/**
	 * If <code>true</code> then force a download operation
	 */
	private boolean bT = false;

	/**
	 * If <code>true</code> then wait for all uploads to finish before returning. Otherwise return after the first successful one, waiting for the remaining copies to be uploaded in background and
	 * committed asynchronously to the catalogue.
	 */
	private boolean bW = ConfigUtils.getConfig().getb("alien.shell.commands.cp.wait_for_all_uploads.default", false);

	/**
	 * If <code>true</code> then the source file is to be deleted after a successful upload (for temporary files)
	 */
	private boolean bD = false;
	
	private int referenceCount = 0;

	private final List<String> ses = new ArrayList<String>();
	private final List<String> exses = new ArrayList<String>();

	private final HashMap<String, Integer> qos = new HashMap<String, Integer>();

	private String source = null;
	private String target = null;

	private File localFile = null;

	// public long timingChallenge = 0;

	// public boolean isATimeChallenge = false;

	@Override
	public void run() {

		if (bT)
			localFile = copyGridToLocal(source, null);

		else
			if (!localFileSpec(source) && localFileSpec(target)) {

				localFile = new File(getLocalFileSpec(target));

				if (!localFile.exists())
					copyGridToLocal(source, localFile);
				else
					if (!isSilent()) {
						out.printErrln("A local file already exists with this name.");
					}
					else {
						final IOException ex = new IOException("A local file already exists with this name: " + target);

						throw new IOError(ex);
					}

			}
			else
				if (localFileSpec(source) && !localFileSpec(target)) {

					final File sourceFile = new File(getLocalFileSpec(source));
					if (!targetLFNExists(target))
						if (sourceFile.exists())
							copyLocalToGrid(sourceFile, target);
						else
							if (!isSilent())
								out.printErrln("A local file with this name does not exists.");
							else {
								final IOException ex = new IOException("Local file " + target + " doesn't exist");

								throw new IOError(ex);
							}

				}
				else
					if (!targetLFNExists(target)) {

						localFile = copyGridToLocal(source, null);
						if (localFile != null && localFile.exists() && localFile.length() > 0)
							if (copyLocalToGrid(localFile, target))
								if (!isSilent())
									out.printOutln("Copy successful.");
								else
									if (!isSilent())
										out.printErrln("Could not copy to the target.");
									else {
										final IOException ex = new IOException("Could not copy to the target: " + target);

										throw new IOError(ex);
									}

							else
								if (!isSilent()) {
									out.printErrln("Could not get the source.");
								}
								else {
									final IOException ex = new IOException("Could not get the source: " + source);

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
			catch (final IOException e) {
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

	private boolean targetLFNExists(final String targetLFN) {
		final LFN currentDir = commander.getCurrentDir();

		final LFN tLFN = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), currentDir != null ? currentDir.getCanonicalName() : null, targetLFN));

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
	 * @param sourceLFN
	 *            Grid filename
	 * @param toLocalFile
	 *            local file
	 * @return local target file
	 */
	public File copyGridToLocal(final String sourceLFN, final File toLocalFile) {
		File targetLocalFile = toLocalFile;

		final LFN currentDir = commander.getCurrentDir();

		final LFN lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), currentDir != null ? currentDir.getCanonicalName() : null, sourceLFN));

		if (lfn == null) {
			if (!isSilent())
				out.printErrln("Could not get the file's LFN: " + sourceLFN);
			return null;
		}

		final List<PFN> pfns = commander.c_api.getPFNsToRead(lfn, ses, exses);

		if (pfns != null) {

			if (referenceCount == 0)
				referenceCount = pfns.size();

			for (final PFN pfn : pfns) {
				final List<Protocol> protocols = Transfer.getAccessProtocols(pfn);
				for (final Protocol protocol : protocols) {
					final ProtocolAction pA = new ProtocolAction(protocol, pfn, targetLocalFile);
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
					catch (final Exception e) {
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

	private static final ExecutorService UPLOAD_THREAD_POOL = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 2L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());

	/**
	 * Upload one file in a separate thread
	 * 
	 * @author costing
	 */
	private final class UploadWork implements Runnable {
		private final LFN lfn;
		private final GUID guid;
		private final File sourceFile;
		private final PFN pfn;
		private final Object lock;

		private String envelope;

		/**
		 * @param lfn
		 * @param guid
		 * @param sourceFile
		 * @param pfn
		 * @param lock
		 */
		public UploadWork(final LFN lfn, final GUID guid, final File sourceFile, final PFN pfn, final Object lock) {
			this.lfn = lfn;
			this.guid = guid;
			this.sourceFile = sourceFile;
			this.pfn = pfn;
			this.lock = lock;
		}

		@Override
		public void run() {
			envelope = uploadPFN(lfn, guid, sourceFile, pfn);

			synchronized (lock) {
				lock.notifyAll();
			}
		}

		public String getEnvelope() {
			return envelope;
		}
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
		catch (final IOException e) {
			if (!isSilent())
				out.printErrln("Couldn't create the GUID.");
			else {
				final IOException ex = new IOException("Couldn't create the GUID based on " + sourceFile);

				throw new IOError(ex);
			}

			return false;
		}

		lfn.guid = guid.guid;
		lfn.size = guid.size;
		lfn.md5 = guid.md5;
		guid.lfnCache = new LinkedHashSet<LFN>(1);
		guid.lfnCache.add(lfn);

		try {
			final PFNforWrite pfw = Dispatcher.execute(new PFNforWrite(commander.getUser(), commander.getRole(), commander.getSite(), lfn, guid, ses, exses, qos));

			pfns = pfw.getPFNs();

			if (pfns == null || pfns.size() == 0) {
				final String err = pfw.getErrorMessage();

				if (isSilent()) {
					throw new IOError(new IOException(err != null ? err : "No write access tickets were returned for " + lfn.getCanonicalName()));
				}

				out.printErr(err != null ? err : "Could not get any write access tickets for " + lfn.getCanonicalName());

				return false;
			}
		}
		catch (final ServerException e) {
			if (!isSilent()) {
				out.printErrln("Couldn't get any access ticket.");

				return false;
			}

			throw new IOError(new IOException("Call for write PFNs for " + lfn.getCanonicalName() + " failed", e.getCause()));
		}

		qos.clear();

		for (final PFN p : pfns) {
			final SE se = commander.c_api.getSE(p.seNumber);

			if (se != null)
				exses.add(se.getName());
		}

		if (referenceCount == 0)
			referenceCount = pfns.size();

		final Vector<String> envelopes = new Vector<String>(pfns.size());
		final Vector<String> registerPFNs = new Vector<String>(pfns.size());

		final ArrayList<Future<UploadWork>> futures = new ArrayList<Future<UploadWork>>(pfns.size());

		final Object lock = new Object();

		for (final PFN pfn : pfns) {
			final UploadWork work = new UploadWork(lfn, guid, sourceFile, pfn, lock);

			final Future<UploadWork> f = UPLOAD_THREAD_POOL.submit(work, work);

			futures.add(f);
		}

		long lastReport = System.currentTimeMillis();

		do {
			final Iterator<Future<UploadWork>> it = futures.iterator();

			while (it.hasNext()) {
				final Future<UploadWork> f = it.next();

				if (f.isDone()) {
					try {
						final UploadWork uw = f.get();

						final String envelope = uw.getEnvelope();

						if (envelope != null) {
							envelopes.add(envelope);

							if (!bW) {
								break;
							}
						}
					}
					catch (final InterruptedException e) {
						logger.log(Level.WARNING, "Interrupted operation", e);
					}
					catch (final ExecutionException e) {
						logger.log(Level.WARNING, "Execution exception", e);
					}
					finally {
						it.remove();
					}
				}
			}

			if (!bW && pfns.size() > 1 && envelopes.size() > 0) {
				if (commit(envelopes, registerPFNs, guid, bD ? null : sourceFile, 1 + registerPFNs.size(), true)) {
					break;
				}

				envelopes.clear();
				registerPFNs.clear();
			}

			if (futures.size() > 0) {
				if (System.currentTimeMillis() - lastReport > 500 && !isSilent()) {
					out.pending();

					lastReport = System.currentTimeMillis();
				}

				synchronized (lock) {
					try {
						lock.wait(100);
					}
					catch (final InterruptedException e) {
						return false;
					}
				}
			}
		} while (futures.size() > 0);

		if (futures.size() > 0) {
			// there was a successfully registered upload so far, we can return true

			new BackgroundUpload(guid, futures, bD ? sourceFile : null).start();

			return true;
		}
		
		if (bD)
			sourceFile.delete();
		
		return commit(envelopes, registerPFNs, guid, bD ? null : sourceFile, referenceCount, true);
	}

	private final class BackgroundUpload extends Thread {
		private final GUID guid;
		private final List<Future<UploadWork>> futures;
		private final File fileToDeleteOnComplete;

		public BackgroundUpload(final GUID guid, final List<Future<UploadWork>> futures, final File fileToDeleteOnComplete) {
			super("alien.shell.commands.JAliEnCommandcp.BackgroundUpload (" + guid.guidId + ")");

			this.guid = guid;
			this.futures = futures;
			this.fileToDeleteOnComplete = fileToDeleteOnComplete;
		}

		@Override
		public void run() {
			final Vector<String> envelopes = new Vector<String>(futures.size());

			while (futures.size() > 0) {
				final Iterator<Future<UploadWork>> it = futures.iterator();

				while (it.hasNext()) {
					final Future<UploadWork> f = it.next();

					if (f.isDone()) {
						logger.log(Level.FINER, "Got back one more copy of " + guid.guid);

						try {
							final UploadWork uw = f.get();

							final String envelope = uw.getEnvelope();

							if (envelope != null) {
								envelopes.add(envelope);
							}
						}
						catch (final InterruptedException e) {
							// Interrupted upload
							logger.log(Level.FINE, "Interrupted upload of " + guid.guid, e);
						}
						catch (final ExecutionException e) {
							// Error executing
							logger.log(Level.FINE, "Error getting the upload result of " + guid.guid, e);
						}
						finally {
							it.remove();
						}
					}
				}
			}

			if (envelopes.size() > 0)
				commit(envelopes, null, guid, null, futures.size(), false);
			
			if (fileToDeleteOnComplete!=null)
				fileToDeleteOnComplete.delete();
		}
	}

	boolean commit(final Vector<String> envelopes, final Vector<String> registerPFNs, final GUID guid, final File sourceFile, final int desiredCount, final boolean report) {
		if (envelopes.size() != 0) {
			final List<PFN> registeredPFNs = commander.c_api.registerEnvelopes(envelopes);

			if (report && !isSilent() && (registeredPFNs == null || registeredPFNs.size() != envelopes.size())) {
				out.printErrln("From the " + envelopes.size() + " replica with tickets only " + (registeredPFNs != null ? String.valueOf(registeredPFNs.size()) : "null") + " were registered");
			}
		}

		int registeredPFNsCount = 0;

		if (registerPFNs != null && registerPFNs.size() > 0) {
			final List<PFN> registeredPFNs = commander.c_api.registerEnvelopes(registerPFNs);

			registeredPFNsCount = registeredPFNs != null ? registeredPFNs.size() : 0;

			if (report && !isSilent() && registeredPFNsCount != registerPFNs.size()) {
				out.printErrln("From the " + registerPFNs.size() + " pfns only " + registeredPFNsCount + " were registered");
			}
		}

		if (sourceFile != null && envelopes.size() + registeredPFNsCount > 0) {
			TempFileManager.putPersistent(guid, sourceFile);
		}

		if (desiredCount == envelopes.size() + registeredPFNsCount) {
			if (report && !isSilent())
				out.printOutln("File successfully uploaded to " + desiredCount + " SEs");

			return true;
		}
		else
			if (envelopes.size() + registeredPFNsCount > 0) {
				if (report && !isSilent())
					out.printErrln("Only " + (envelopes.size() + registeredPFNsCount) + " out of " + desiredCount + " requested replicas could be uploaded");

				return true;
			}
			else {
				if (report) {
					if (!isSilent())
						out.printOutln("Upload failed, sorry!");
					else {
						final IOException ex = new IOException("Upload failed");

						throw new IOError(ex);
					}
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
	String uploadPFN(final LFN lfn, final GUID guid, final File sourceFile, final PFN initialPFN) {
		boolean failOver;

		PFN pfn = initialPFN;

		String returnEnvelope = null;

		do {
			failOver = false;

			final List<Protocol> protocols = Transfer.getAccessProtocols(pfn);

			String targetPFNResult = null;

			for (final Protocol protocol : protocols) {
				try {
					if (!isSilent())
						out.printOutln("Uploading file " + sourceFile.getCanonicalPath() + " to " + pfn.getPFN());

					try {
						targetPFNResult = protocol.put(pfn, sourceFile);
					}
					catch (final IOException ioe) {
						// ignore, will try next protocol or fetch another
						// replica to replace this one
					}
				}
				catch (final Exception e) {
					// e.printStackTrace();
				}

				if (targetPFNResult != null)
					break;
			}

			if (targetPFNResult != null) {
				// if (!isSilent()){
				// out.printOutln("Successfully uploaded " + sourceFile.getAbsolutePath() + " to " + pfn.getPFN()+"\n"+targetLFNResult);
				// }

				if (pfn.ticket != null && pfn.ticket.envelope != null && pfn.ticket.envelope.getSignedEnvelope() != null) {
					if (pfn.ticket.envelope.getEncryptedEnvelope() == null)
						returnEnvelope = targetPFNResult;
					else
						returnEnvelope = pfn.ticket.envelope.getSignedEnvelope();
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

					final List<PFN> newPFNtoTry = commander.c_api.getPFNsToWrite(lfn, guid, ses, exses, replacementQoS);

					if (newPFNtoTry != null && newPFNtoTry.size() > 0) {
						pfn = newPFNtoTry.get(0);

						se = commander.c_api.getSE(pfn.seNumber);

						exses.add(se.getName());
					}
					else
						pfn = null;
				}
			}
		} while (failOver && pfn != null);

		return returnEnvelope;
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		if (!isSilent()) {
			out.printOutln();
			out.printOutln(helpUsage("cp", "[-options] < file:///localfile /gridfile >  |  < /gridfile file:///localfile >  |  < -t /gridfile >"));
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
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandcp(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
		try {
			final OptionParser parser = new OptionParser();

			parser.accepts("S").withRequiredArg();
			parser.accepts("g");
			parser.accepts("t");
			parser.accepts("w");
			parser.accepts("W");
			parser.accepts("d");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			if (options.nonOptionArguments().size()==0)
				return;
			
			if (options.nonOptionArguments().size() != 2 && !(options.nonOptionArguments().size() == 1 && options.has("t"))) {
				printHelp();
				return;
			}

			if (options.has("w"))
				bW = true;

			if (options.has("W"))
				bW = false;
			
			if (options.has("d"))
				bD = true;

			if (options.has("S") && options.hasArgument("S")) {

				if ((String) options.valueOf("S") != null) {
					final StringTokenizer st = new StringTokenizer((String) options.valueOf("S"), ",");
					while (st.hasMoreElements()) {
						final String spec = st.nextToken();
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
						else
							if (spec.contains(":")) {// a qosTag:count spec
								try {

									final int c = Integer.parseInt(spec.substring(spec.indexOf(':') + 1));
									if (c > 0) {
										qos.put(spec.substring(0, spec.indexOf(':')), Integer.valueOf(c));
										referenceCount = referenceCount + c;
									}
									else
										throw new JAliEnCommandException();

								}
								catch (final Exception e) {
									throw new JAliEnCommandException();
								}
							}
							else
								if (!spec.equals(""))
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
		catch (final OptionException e) {
			printHelp();
			throw e;
		}
	}

}
