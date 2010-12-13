/**
 * 
 */
package alien.io.protocols;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import lia.util.process.ExternalProcessBuilder;
import lia.util.process.ExternalProcess.ExecutorFinishStatus;
import lia.util.process.ExternalProcess.ExitStatus;
import alien.catalogue.PFN;
import alien.catalogue.access.XrootDEnvelope;
import alien.config.ConfigUtils;

/**
 * @author costing
 * @since Dec 8, 2010
 */
public class Xrootd extends Protocol {

	private static String xrdcpenvKey = "LD_LIBRARY_PATH";
	private static String xrdcpenvVal = "/lib:/lib:/opt/alien/api/lib/";
	private static String xrdcplocation = "/opt/alien/api/bin/xrdcp";
	// private static String xrdcplocation = "/opt/alien/api/bin/xrdcpapmon";
	private static String xrdstatlocation = "/opt/alien/api/bin/xrdstat";
	private static String xrdcpdebug = "-d";
	private static int xrdcpdebuglevel = 0;
	private static String DIFirstConnectMaxCnt = "6";

	private static int statRetries = 3;
	private static int statRetryTimes[] = { 6, 12, 30 };
	private int statRetryCounter = 0;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(Xrootd.class
			.getCanonicalName());

	/**
	 * package protected
	 */
	public Xrootd() {
		// package protected
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -7442913364052285097L;

	public void setDebugLevel(int level) {
		xrdcpdebuglevel = level;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see alien.io.protocols.Protocol#get(alien.catalogue.PFN,
	 * alien.catalogue.access.CatalogueReadAccess, java.lang.String)
	 */
	@Override
	public File get(final PFN pfn, final File localFile) throws IOException {
		File target = null;

		if (localFile != null) {
			if (localFile.exists())
				throw new IOException("Local file " + localFile.getCanonicalPath()
						+ " exists already. Xrdcp would fail.");
			target = localFile;
		}

		if (target == null) {
			// TODO: we need a temporary file name, the file may NOT exist, otherwise xrdcp fails
		}

		if (pfn.envelope == null) {
			throw new IOException("The envelope for PFN " + pfn.toString()
					+ " could not be found.");
		}

		try {
			final List<String> command = new LinkedList<String>();
			command.add(xrdcplocation);
			command.add("-DIFirstConnectMaxCnt");
			command.add(DIFirstConnectMaxCnt);
			if (xrdcpdebuglevel > 0) {
				command.add(xrdcpdebug);
				command.add(String.valueOf(xrdcpdebuglevel));
			}
			command.add(pfn.pfn);
			command.add(target.getCanonicalPath());

			if (pfn.envelope.getEncryptedEnvelope() != null)
				command.add("-OD&authz=\""
						+ pfn.envelope.getEncryptedEnvelope() + "\"");
			else if (pfn.envelope.getSignedEnvelope() != null)
				command.add("-OD" + pfn.envelope.getSignedEnvelope());

			final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(
					command);

			pBuilder.environment().put(xrdcpenvKey, xrdcpenvVal);

			pBuilder.returnOutputOnExit(true);

			pBuilder.timeout(24, TimeUnit.HOURS);

			pBuilder.redirectErrorStream(true);

			ExitStatus exitStatus;

			try {
				exitStatus = pBuilder.start().waitFor();
			} catch (final InterruptedException ie) {
				throw new IOException(
						"Interrupted while waiting for the following command to finish : "
								+ command.toString());
			}

			if (exitStatus.getExecutorFinishStatus() != ExecutorFinishStatus.NORMAL) {
				System.out.println(exitStatus.getStdOut());
				throw new IOException("Executor finish status: "
						+ exitStatus.getExecutorFinishStatus()
						+ " for command: " + command.toString());
			}

			if (exitStatus.getExtProcExitStatus() != 0) {
				throw new IOException("Exit code was not zero but "
						+ exitStatus.getExtProcExitStatus() + " for command : "
						+ command.toString());
			}

			if (!checkDownloadedFile(target, pfn))
				throw new IOException(
						"Local file doesn't match catalogue details");
		} catch (final IOException ioe) {
			target.delete();

			throw ioe;
		} catch (final Throwable t) {
			target.delete();

			logger.log(Level.WARNING, "Caught exception", t);

			throw new IOException("Get aborted because " + t);
		}

		return target;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see alien.io.protocols.Protocol#put(alien.catalogue.PFN,
	 * alien.catalogue.access.CatalogueWriteAccess, java.lang.String)
	 */
	@Override
	public String put(final PFN pfn, final File localFile) throws IOException {
		if (localFile == null || !localFile.exists() || !localFile.isFile()
				|| !localFile.canRead())
			throw new IOException("Local file " + localFile + " cannot be read");

		if (pfn.envelope == null) {
			throw new IOException("The envelope for PFN " + pfn.toString()
					+ " could not be found.");
		}
		if (localFile.length() != pfn.envelope.ticket.getLFN().size) {
			throw new IOException("The ticket for PFN " + pfn.toString()
					+ " does not match file size of " + localFile.getName());
		}

		try {
			final List<String> command = new LinkedList<String>();
			command.add(xrdcplocation);
			command.add("-DIFirstConnectMaxCnt");
			command.add(DIFirstConnectMaxCnt);
			if (xrdcpdebuglevel > 0) {
				command.add(xrdcpdebug);
				command.add(String.valueOf(xrdcpdebuglevel));
			}
			command.add("-np");
			command.add("-v");
			command.add("-f");
			command.add(localFile.getCanonicalPath());
			command.add(pfn.pfn);

			if (pfn.envelope.getEncryptedEnvelope() != null)
				command.add("-OD&authz=\""
						+ pfn.envelope.getEncryptedEnvelope() + "\"");
			else if (pfn.envelope.getSignedEnvelope() != null)
				command.add("-OD" + pfn.envelope.getSignedEnvelope());

			final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(
					command);

			pBuilder.environment().put(xrdcpenvKey, xrdcpenvVal);

			pBuilder.returnOutputOnExit(true);

			pBuilder.timeout(24, TimeUnit.HOURS);

			pBuilder.redirectErrorStream(true);

			ExitStatus exitStatus;

			try {
				exitStatus = pBuilder.start().waitFor();
			} catch (final InterruptedException ie) {
				throw new IOException(
						"Interrupted while waiting for the following command to finish : "
								+ command.toString());
			}

			if (exitStatus.getExecutorFinishStatus() != ExecutorFinishStatus.NORMAL) {
				throw new IOException("Executor finish status: "
						+ exitStatus.getExecutorFinishStatus()
						+ " for command: " + command.toString());
			}

			if (exitStatus.getExtProcExitStatus() != 0) {
				throw new IOException("Exit code was not zero but "
						+ exitStatus.getExtProcExitStatus() + " for command : "
						+ command.toString());
			}
			if (pfn.envelope.getEncryptedEnvelope() != null)
				return xrdstat(pfn, false);
			return xrdstat(pfn, true);
		} catch (final IOException ioe) {
			throw ioe;
		} catch (final Throwable t) {
			logger.log(Level.WARNING, "Caught exception", t);

			throw new IOException("Get aborted because " + t);
		}
	}

	/**
	 * Check if the PFN has the correct properties, such as described in the
	 * access envelope
	 * 
	 * @param pfn
	 * @return the signed envelope from the storage, if it knows how to generate
	 *         one
	 * @throws IOException
	 *             if the remote file properties are not what is expected
	 */
	public String xrdstat(final PFN pfn, boolean returnEnvelope)
			throws IOException {

		try {
			final List<String> command = new LinkedList<String>();
			command.add(xrdstatlocation);
			if (xrdcpdebuglevel > 0) {
				command.add(xrdcpdebug);
				command.add(String.valueOf(xrdcpdebuglevel));
			}
			if (returnEnvelope)
				command.add("-returnEnvelope");
			command.add(pfn.toString());

			if ((pfn.envelope != null)
					&& (pfn.envelope.getSignedEnvelope() != null))
				command.add("-OD" + pfn.envelope.getSignedEnvelope());

			final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(
					command);

			pBuilder.environment().put(xrdcpenvKey, xrdcpenvVal);

			pBuilder.returnOutputOnExit(true);

			pBuilder.timeout(24, TimeUnit.HOURS);

			pBuilder.redirectErrorStream(true);

			ExitStatus exitStatus;

			try {
				exitStatus = pBuilder.start().waitFor();
			} catch (final InterruptedException ie) {
				throw new IOException(
						"Interrupted while waiting for the following command to finish : "
								+ command.toString());
			}

			if (exitStatus.getExecutorFinishStatus() != ExecutorFinishStatus.NORMAL) {
				throw new IOException("Executor finish status: "
						+ exitStatus.getExecutorFinishStatus()
						+ " for command: " + command.toString());
			}

			if (exitStatus.getExtProcExitStatus() != 0) {

				if (waitAndTryAgain()) {
					return xrdstat(pfn, returnEnvelope);
				} else {
					throw new IOException("Exit code was not zero but "
							+ exitStatus.getExtProcExitStatus()
							+ " for command : " + command.toString());
				}
			} else {
				if (returnEnvelope) {
					return exitStatus.getStdOut(); // TODO this is not the
													// return envelope, we have
													// to get first the xrdcp
													// implementation, than
													// finish this
				} else {
					long filesize = checkOldOutputOnSize(exitStatus.getStdOut());
					if (pfn.envelope.ticket.getLFN().size == filesize) {
						return pfn.envelope.getSignedEnvelope();
					} else {
						if (waitAndTryAgain()) {
							return xrdstat(pfn, false);
						} else {
							throw new IOException(
									"The xrdstat could not confirm the file to be uploaded with size, reported size: "
											+ filesize
											+ ", expected size: "
											+ pfn.envelope.ticket.getLFN().size);
						}
					}
				}
			}
		} catch (final IOException ioe) {
			throw ioe;
		} catch (final Throwable t) {
			logger.log(Level.WARNING, "Caught exception", t);

			throw new IOException("Get aborted because " + t);
		}
	}

	private boolean waitAndTryAgain() {
		if (statRetryCounter < statRetries) {
			try {
				Thread.sleep(statRetryTimes[statRetryCounter]);
			} catch (InterruptedException e) {
				// the VM doesn't want us to sleep anymore,
				// so get back to work
			}
			statRetryCounter++;
			return true;
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see alien.io.protocols.Protocol#transfer(alien.catalogue.PFN,
	 * alien.catalogue.access.CatalogueReadAccess, alien.catalogue.PFN,
	 * alien.catalogue.access.CatalogueWriteAccess)
	 */
	@Override
	public String transfer(final PFN source, final PFN target)
			throws IOException {
		final File temp = get(source, null);

		try {
			return put(target, temp);
		} finally {
			temp.delete();
		}
	}

	private static long checkOldOutputOnSize(String stdout) {

		long size = 0;
		String line = null;
		BufferedReader reader = new BufferedReader(new StringReader(stdout));

		try {
			while ((line = reader.readLine()) != null) {
				// $doxrcheck =~ /Size:\ (\d+)/;
				if (line.length() > 0)
					System.out.println(line.charAt(0));
				if (line.startsWith("Size: ")) {
					String[] elements = line.split(" ");
					size = Long.parseLong(elements[1].trim());
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		return size;
	}

}
