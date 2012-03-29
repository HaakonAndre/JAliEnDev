/**
 * 
 */
package alien.io.protocols;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;

import utils.ExternalCalls;

import lia.util.process.ExternalProcess;
import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcessBuilder;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.XrootDEnvelope;
import alien.config.ConfigUtils;
import alien.io.IOUtils;

/**
 * @author costing
 * @since Dec 8, 2010
 */
public class Xrootd extends Protocol {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7860814883144320429L;
	private static String xrdcpdebug = "-d";
	private int xrdcpdebuglevel = 0;

	private final static String xrdcpCommand = "xrdcpapmon";
	
	private static String xrootd_default_path = null;

	private static String xrdcpPath = null;
	
	static {
		if (ConfigUtils.getConfig() != null) {
			xrootd_default_path = ConfigUtils.getConfig().gets("xrootd.location", null);

			if (xrootd_default_path != null){
				xrdcpPath = xrootd_default_path + "/bin/" + xrdcpCommand;
				
				File test = new File(xrdcpPath);
				
				if (!test.exists() || !test.isFile() || !test.canExecute())
					xrdcpPath = null;
			}
		}
		
		if (xrdcpPath == null){
			xrdcpPath = ExternalCalls.programExistsInPath(xrdcpCommand);
			
			if (xrdcpPath!=null){
				int idx = xrdcpPath.lastIndexOf('/');
				
				if (idx>0){
					idx = xrdcpPath.lastIndexOf('/', idx);
					
					if (idx>=0)
						xrootd_default_path = xrdcpPath.substring(0, idx);
				}
			}
		}
	}
	
	private static String DIFirstConnectMaxCnt = "2";

	private int timeout = 60;

	// last value must be 0 for a clean exit
	//private static final int statRetryTimes[] = { 1, 2, 4, 8, 16, 0 };
	private static final int statRetryTimes[] = { 5, 0 };

	
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(Xrootd.class.getCanonicalName());

	/**
	 * package protected
	 */
	public Xrootd() {
		// package protected
	}

	private static void checkLibraryPath(final ExternalProcessBuilder p) {
		if (xrootd_default_path != null) {
			String ldpath = "";
			if (p.environment().containsKey("LD_LIBRARY_PATH"))
				ldpath = p.environment().get("LD_LIBRARY_PATH");

			p.environment().put("LD_LIBRARY_PATH", ldpath + ":" + xrootd_default_path + "/lib");
		}
	}
	
	/**
	 * @param level
	 *            xrdcp debug level
	 */
	public void setDebugLevel(final int level) {
		xrdcpdebuglevel = level;
	}

	/**
	 * Set the xrdcp timeout
	 * 
	 * @param seconds
	 */
	public void setTimeout(final int seconds) {
		timeout = seconds;
	}

	/**
	 * Extract the most relevant failure reason from an xrdcp / xrd3cp output
	 * 
	 * @param message
	 * @return relevant portion of the output
	 */
	public static final String parseXrootdError(final String message) {
		if (message == null || message.length() == 0)
			return null;

		int idx = message.indexOf("Last server error");

		if (idx >= 0) {
			idx = message.indexOf("('", idx);

			if (idx > 0) {
				idx += 2;

				int idx2 = message.indexOf("')", idx);

				if (idx2 > idx) {
					return message.substring(idx, idx2);
				}
			}
		}

		idx = message.lastIndexOf("\tretc=");

		if (idx >= 0) {
			int idx2 = message.indexOf('\n', idx);

			if (idx2 < 0)
				idx2 = message.length();

			return message.substring(idx + 1, idx2);
		}

		return null;
	}

	private boolean usexrdrm = true;

	/**
	 * Whether to use "xrdrm" (<code>true</code>) or "xrd rm" ( <code>false</code>)
	 * 
	 * @param newValue
	 * @return the previous setting
	 */
	public boolean setUseXrdRm(final boolean newValue) {
		final boolean prev = usexrdrm;

		usexrdrm = newValue;

		return prev;
	}

	@Override
	public boolean delete(final PFN pfn) throws IOException {
		if (pfn == null || pfn.ticket == null || pfn.ticket.type != AccessType.DELETE) {
			throw new IOException("You didn't get the rights to delete this PFN");
		}

		try {
			final List<String> command = new LinkedList<String>();

			// command.addAll(getCommonArguments());

			String envelope = null;

			if (pfn.ticket.envelope != null) {
				envelope = pfn.ticket.envelope.getEncryptedEnvelope();

				if (envelope == null)
					envelope = pfn.ticket.envelope.getSignedEnvelope();
			}

			File fAuthz = null;

			if (usexrdrm) {
				command.add("xrdrm");
				command.add("-v");

				String transactionURL = pfn.pfn;

				if (pfn.ticket.envelope != null) {
					transactionURL = pfn.ticket.envelope.getTransactionURL();
				}

				if (envelope != null) {
					fAuthz = File.createTempFile("xrdrm-", ".authz", IOUtils.getTemporaryDirectory());

					final FileWriter fw = new FileWriter(fAuthz);

					fw.write(envelope);

					fw.flush();
					fw.close();

					command.add("-authz");
					command.add(fAuthz.getCanonicalPath());
				}

				command.add(transactionURL);
			}
			else {
				final Matcher m = XrootDEnvelope.PFN_EXTRACT.matcher(pfn.pfn);

				if (!m.matches()) {
					System.err.println("Pattern doesn't match " + pfn.pfn);
					return false;
				}

				final String server = m.group(1);
				final String spfn = m.group(4);

				command.add("xrd");
				command.add(server);
				command.add("rm " + spfn + "?authz=" + envelope);
			}

			// System.err.println(command);

			final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(command);
			
			checkLibraryPath(pBuilder);

			pBuilder.returnOutputOnExit(true);

			pBuilder.timeout(1, TimeUnit.MINUTES);

			pBuilder.redirectErrorStream(true);

			final ExitStatus exitStatus;

			try {
				exitStatus = pBuilder.start().waitFor();
			}
			catch (final InterruptedException ie) {
				throw new IOException("Interrupted while waiting for the following command to finish : " + command.toString());
			}
			finally {
				if (fAuthz != null) {
					if (!fAuthz.delete())
						logger.log(Level.WARNING, "Could not delete temporary auth token file: " + fAuthz.getAbsolutePath());
				}
			}

			if (exitStatus.getExtProcExitStatus() != 0) {
				if (logger.isLoggable(Level.FINE))
					logger.log(Level.FINE, exitStatus.getStdOut());

				throw new IOException("Exit code " + exitStatus.getExtProcExitStatus());
			}

			// System.err.println(exitStatus.getStdOut());

			return true;
		}
		catch (final IOException ioe) {
			throw ioe;
		}
		catch (final Throwable t) {
			logger.log(Level.WARNING, "Caught exception", t);

			throw new IOException("delete aborted because " + t);
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see alien.io.protocols.Protocol#get(alien.catalogue.PFN, alien.catalogue.access.CatalogueReadAccess,
	 * java.lang.String)
	 */
	@Override
	public File get(final PFN pfn, final File localFile) throws IOException {
		File target = null;

		if (localFile != null) {
			if (localFile.exists())
				throw new SourceException("Local file " + localFile.getCanonicalPath() + " exists already. Xrdcp would fail.");
			target = localFile;
		}

		if (target == null) {
			// we are free to use any cached value
			target = TempFileManager.getAny(pfn.getGuid());
			
			if (target!=null){
				logger.log(Level.FINE, "Reusing cached file: "+target.getCanonicalPath());
				
				return target;
			}
			
			target = File.createTempFile("xrootd-get", null, IOUtils.getTemporaryDirectory());

			if (!target.delete()){
				logger.log(Level.WARNING, "Could not delete the just created temporary file: " + target);
				return null;
			}
		}

		if (pfn.ticket == null || pfn.ticket.type != AccessType.READ) {
			throw new SourceException("The envelope for PFN " + pfn.toString() + (pfn.ticket==null ? " could not be found" : " is not a READ one"));
		}

		try {
			final List<String> command = new LinkedList<String>();

			if (xrdcpPath == null) {
				logger.log(Level.SEVERE, "Could not find [" + xrdcpCommand + "] in path.");
				throw new SourceException("Could not find [" + xrdcpCommand + "] in path.");
			}

			command.add(xrdcpPath);

			command.addAll(getCommonArguments());

			String transactionURL = pfn.pfn;

			if (pfn.ticket.envelope != null) {
				transactionURL = pfn.ticket.envelope.getTransactionURL();
			}

			command.add(transactionURL);
			command.add(target.getCanonicalPath());

			if (pfn.ticket.envelope != null) {
				if (pfn.ticket.envelope.getEncryptedEnvelope() != null)
					command.add("-OS&authz=" + pfn.ticket.envelope.getEncryptedEnvelope());
				else
					if (pfn.ticket.envelope.getSignedEnvelope() != null)
						command.add("-OS" + pfn.ticket.envelope.getSignedEnvelope());
			}

			final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(command);
			
			checkLibraryPath(pBuilder);

			pBuilder.returnOutputOnExit(true);

			long maxTime = pfn.getGuid().size / 20000; // 20KB/s should be available to anybody

			maxTime += timeout;

			pBuilder.timeout(maxTime, TimeUnit.SECONDS);

			pBuilder.redirectErrorStream(true);

			final ExitStatus exitStatus;

			try {
				final ExternalProcess p = pBuilder.start();

				if (p != null)
					exitStatus = p.waitFor();
				else
					throw new SourceException("Cannot start the process");
			}
			catch (final InterruptedException ie) {
				throw new SourceException("Interrupted while waiting for the following command to finish : " + command.toString());
			}

			if (exitStatus.getExtProcExitStatus() != 0) {
				String sMessage = parseXrootdError(exitStatus.getStdOut());

				logger.log(Level.WARNING, "GET failed with " + exitStatus.getStdOut());

				if (sMessage != null) {
					sMessage = xrdcpCommand+" exited with " + exitStatus.getExtProcExitStatus() + ": " + sMessage;
				}
				else {
					sMessage = "Exit code was " + exitStatus.getExtProcExitStatus() + " for command : " + command.toString();
				}

				throw new SourceException(sMessage);
			}

			if (!checkDownloadedFile(target, pfn))
				throw new SourceException("Local file doesn't match catalogue details");
		}
		catch (final SourceException ioe) {
			if (target.exists() && !target.delete())
				logger.log(Level.WARNING, "Could not delete temporary file on IO exception: " + target);

			throw ioe;
		}
		catch (final Throwable t) {
			if (target.exists() && !target.delete())
				logger.log(Level.WARNING, "Could not delete temporary file on throwable: " + target);

			logger.log(Level.WARNING, "Caught exception", t);

			throw new SourceException("Get aborted because " + t);
		}

		if (localFile==null){
			TempFileManager.putTemp(pfn.getGuid(), target);
		}
		else{
			TempFileManager.putPersistent(pfn.getGuid(), target);
		}
		
		return target;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see alien.io.protocols.Protocol#put(alien.catalogue.PFN, alien.catalogue.access.CatalogueWriteAccess,
	 * java.lang.String)
	 */
	@Override
	public String put(final PFN pfn, final File localFile) throws IOException {
		if (localFile == null || !localFile.exists() || !localFile.isFile() || !localFile.canRead())
			throw new TargetException("Local file " + localFile + " cannot be read");

		if (pfn.ticket == null || pfn.ticket.type != AccessType.WRITE) {
			throw new TargetException("No access to this PFN");
		}

		if (localFile.length() != pfn.getGuid().size) {
			throw new TargetException("Difference in sizes: local=" + localFile.length() + " / pfn=" + pfn.getGuid().size);
		}

		try {
			final List<String> command = new LinkedList<String>();

			if (xrdcpPath == null) {
				logger.log(Level.SEVERE, "Could not fine [" + xrdcpCommand + "] in path.");
				throw new TargetException("Could not fine [" + xrdcpCommand + "] in path.");
			}

			command.add(xrdcpPath);

			command.addAll(getCommonArguments());

			command.add("-np");
			command.add("-v");
			command.add("-f");
			command.add(localFile.getCanonicalPath());

			String transactionURL = pfn.pfn;

			if (pfn.ticket.envelope != null) {
				transactionURL = pfn.ticket.envelope.getTransactionURL();
			}

			command.add(transactionURL);

			if (pfn.ticket.envelope != null) {
				if (pfn.ticket.envelope.getEncryptedEnvelope() != null)
					command.add("-OD&authz=" + pfn.ticket.envelope.getEncryptedEnvelope());
				else
					if (pfn.ticket.envelope.getSignedEnvelope() != null) {
						command.add("-OD" + pfn.ticket.envelope.getSignedEnvelope());

					}
			}

			final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(command);
			
			checkLibraryPath(pBuilder);

			pBuilder.returnOutputOnExit(true);

			long maxTime = pfn.getGuid().size / 20000; // 20KB/s should be available to anybody

			maxTime += timeout;

			pBuilder.timeout(maxTime, TimeUnit.SECONDS);

			pBuilder.redirectErrorStream(true);

			final ExitStatus exitStatus;

			try {
				exitStatus = pBuilder.start().waitFor();
			}
			catch (final InterruptedException ie) {
				throw new TargetException("Interrupted while waiting for the following command to finish : " + command.toString());
			}

			if (exitStatus.getExtProcExitStatus() != 0) {
				String sMessage = parseXrootdError(exitStatus.getStdOut());

				logger.log(Level.WARNING, "PUT failed with " + exitStatus.getStdOut());

				if (sMessage != null) {
					sMessage = xrdcpCommand+" exited with " + exitStatus.getExtProcExitStatus() + ": " + sMessage;
				}
				else {
					sMessage = "Exit code was " + exitStatus.getExtProcExitStatus() + " for command : " + command.toString();
				}

				throw new TargetException(sMessage);
			}

			if (pfn.ticket.envelope.getEncryptedEnvelope() != null)
				return xrdstat(pfn, false);

			return xrdstat(pfn, true);
		}
		catch (final TargetException ioe) {
			throw ioe;
		}
		catch (final IOException ioe) {
			throw new TargetException(ioe.getMessage());
		}
		catch (final Throwable t) {
			logger.log(Level.WARNING, "Caught exception", t);

			throw new TargetException("Put aborted because " + t);
		}
	}

	private final List<String> getCommonArguments() {
		final List<String> ret = new ArrayList<String>();

		ret.add("-DIFirstConnectMaxCnt");
		ret.add(DIFirstConnectMaxCnt);

		if (xrdcpdebuglevel > 0) {
			ret.add(xrdcpdebug);
			ret.add(String.valueOf(xrdcpdebuglevel));
		}

		if (timeout > 0) {
			ret.add("-DITransactionTimeout");
			ret.add(String.valueOf(timeout));

			ret.add("-DIRequestTimeout");
			ret.add(String.valueOf(timeout));
		}
		
		ret.add("-DIReadCacheSize");
		ret.add("0");

		return ret;
	}

	/**
	 * Check if the PFN has the correct properties, such as described in the access envelope
	 * 
	 * @param pfn
	 * @param returnEnvelope
	 * @return the signed envelope from the storage, if it knows how to generate one
	 * @throws IOException
	 *             if the remote file properties are not what is expected
	 */
	public String xrdstat(final PFN pfn, final boolean returnEnvelope) throws IOException {
		return xrdstat(pfn, returnEnvelope, true, false);
	}

	/**
	 * Check if the PFN has the correct properties, such as described in the access envelope
	 * 
	 * @param pfn
	 * @param returnEnvelope
	 * @param retryWithDelay
	 * @param forceRecalcMd5
	 * @return the signed envelope from the storage, if it knows how to generate one
	 * @throws IOException
	 *             if the remote file properties are not what is expected
	 */
	public String xrdstat(final PFN pfn, final boolean returnEnvelope, final boolean retryWithDelay, final boolean forceRecalcMd5) throws IOException {

		for (int statRetryCounter = 0; statRetryCounter < statRetryTimes.length; statRetryCounter++) {
			try {
				final List<String> command = new LinkedList<String>();

				if (returnEnvelope) {
					// e.g. xrd pcaliense01:1095 query 32
					// /15/63447/e3f01fd2-23e3-11e0-9a96-001f29eb8b98?getrespenv=1\&recomputemd5=1
					// TODO:
					// clean the following up, it's working but not very good
					// looking
					command.add("xrd");
					String qProt = pfn.getPFN().substring(7);
					String host = qProt.substring(0, qProt.indexOf(':'));
					String port = qProt.substring(qProt.indexOf(':') + 1, qProt.indexOf('/'));

					try {
						int pno = Integer.parseInt(port);
						pno++;
						port = "" + pno;
					}
					catch (NumberFormatException n) {
						// port was not a number, keeping the default port
					}

					command.add(host + ":" + port);
					command.add("query");
					command.add("32");
					String qpfn = qProt.substring(qProt.indexOf('/') + 1) + "?getrespenv=1";

					if (forceRecalcMd5)
						qpfn += "\\&recomputemd5=1";
					command.add(qpfn);

				}
				else {
					command.add("xrdstat");
					command.addAll(getCommonArguments());
					command.add(pfn.getPFN());

				}

				final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(command);

				checkLibraryPath(pBuilder);

				pBuilder.returnOutputOnExit(true);

				pBuilder.timeout(15, TimeUnit.SECONDS);

				pBuilder.redirectErrorStream(true);

				ExitStatus exitStatus;

				try {
					exitStatus = pBuilder.start().waitFor();
				}
				catch (final InterruptedException ie) {
					throw new IOException("Interrupted while waiting for the following command to finish : " + command.toString());
				}

				final int sleep = statRetryTimes[statRetryCounter];

				if (exitStatus.getExtProcExitStatus() != 0) {
					if (sleep == 0 || !retryWithDelay) {
						throw new IOException("Exit code was " + exitStatus.getExtProcExitStatus() + ", output was " + exitStatus.getStdOut() + ", " + "for command : " + command.toString());
					}

					Thread.sleep(sleep * 1000);
					continue;
				}

				if (returnEnvelope)
					return exitStatus.getStdOut();

				final long filesize = checkOldOutputOnSize(exitStatus.getStdOut());

				if (pfn.getGuid().size == filesize)
					return exitStatus.getStdOut();

				if (sleep == 0 || !retryWithDelay) {
					throw new IOException(command.toString() + ": could not confirm the upload: " + exitStatus.getStdOut());
				}

				Thread.sleep(sleep * 1000);
				continue;

			}
			catch (final IOException ioe) {
				throw ioe;
			}
			catch (final Throwable t) {
				logger.log(Level.WARNING, "Caught exception", t);

				final IOException ioe = new IOException("xrdstat internal failure " + t);

				ioe.setStackTrace(t.getStackTrace());

				throw ioe;
			}
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see alien.io.protocols.Protocol#transfer(alien.catalogue.PFN, alien.catalogue.access.CatalogueReadAccess,
	 * alien.catalogue.PFN, alien.catalogue.access.CatalogueWriteAccess)
	 */
	@Override
	public String transfer(final PFN source, final PFN target) throws IOException {
		final File temp = get(source, null);

		try {
			return put(target, temp);
		}
		finally {
			TempFileManager.release(temp);
		}
	}

	private static long checkOldOutputOnSize(String stdout) {

		long size = 0;
		String line = null;
		BufferedReader reader = new BufferedReader(new StringReader(stdout));

		try {
			while ((line = reader.readLine()) != null) {
				if (line.startsWith("xstat:")) {
					int idx = line.indexOf("size=");

					if (idx > 0) {
						int idx2 = line.indexOf(" ", idx);

						size = Long.parseLong(line.substring(idx + 5, idx2));
					}
				}
			}

		}
		catch (IOException e) {
			e.printStackTrace();
		}
		return size;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "xrootd";
	}

	@Override
	int getPreference() {
		return 2;
	}
	
}
