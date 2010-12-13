/**
 * 
 */
package alien.io.protocols;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import lia.util.process.ExternalProcess.ExecutorFinishStatus;
import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcessBuilder;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.config.ConfigUtils;

/**
 * @author costing
 * @since Dec 8, 2010
 */
public class Xrootd extends Protocol {
	private static String xrdcpdebug = "-d";
	private static int xrdcpdebuglevel = 0;
	
	private static String DIFirstConnectMaxCnt = "6";

	// last value must be 0 for a clean exit
	private static final int statRetryTimes[] = { 6, 12, 30, 0 };

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

	/**
	 * @param level xrdcp debug level
	 */
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
			target = File.createTempFile("xrootd-get", null);
			target.delete();
		}

		if (pfn.ticket==null || pfn.ticket.type!=AccessType.READ) {
			throw new IOException("The envelope for PFN " + pfn.toString()
					+ " could not be found or is not a READ one.");
		}

		try {
			final List<String> command = new LinkedList<String>();
			command.add("xrdcpapmon");
			command.add("-DIFirstConnectMaxCnt");
			command.add(DIFirstConnectMaxCnt);
			if (xrdcpdebuglevel > 0) {
				command.add(xrdcpdebug);
				command.add(String.valueOf(xrdcpdebuglevel));
			}
			command.add(pfn.pfn);
			command.add(target.getCanonicalPath());

			if (pfn.ticket.envelope!=null){
				if (pfn.ticket.envelope.getEncryptedEnvelope() != null)
					command.add("-OD&authz=\""
						+ pfn.ticket.envelope.getEncryptedEnvelope() + "\"");
				else if (pfn.ticket.envelope.getSignedEnvelope() != null)
					command.add("-OD" + pfn.ticket.envelope.getSignedEnvelope());
			}

			final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(command);

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

		if (pfn.ticket == null || pfn.ticket.type!=AccessType.WRITE) {
			throw new IOException("No access to this PFN");
		}
		
		if (localFile.length() != pfn.getGuid().size) {
			throw new IOException("Difference in sizes: local="+localFile.length()+" / pfn="+pfn.getGuid().size);
		}

		try {
			final List<String> command = new LinkedList<String>();
			command.add("xrdcpapmon");
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

			if (pfn.ticket.envelope != null){
				if (pfn.ticket.envelope.getEncryptedEnvelope() != null)
					command.add("-OD&authz=\""
						+ pfn.ticket.envelope.getEncryptedEnvelope() + "\"");
				else if (pfn.ticket.envelope.getSignedEnvelope() != null)
					command.add("-OD" + pfn.ticket.envelope.getSignedEnvelope());
			}

			final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(command);

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
			if (pfn.ticket.envelope.getEncryptedEnvelope() != null)
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
	 * @param returnEnvelope 
	 * @return the signed envelope from the storage, if it knows how to generate
	 *         one
	 * @throws IOException
	 *             if the remote file properties are not what is expected
	 */
	public String xrdstat(final PFN pfn, final boolean returnEnvelope) throws IOException {
		for (int statRetryCounter=0; statRetryCounter<statRetryTimes.length; statRetryCounter++){
			try {
				final List<String> command = new LinkedList<String>();
				command.add("xrdstat");
				if (xrdcpdebuglevel > 0) {
					command.add(xrdcpdebug);
					command.add(String.valueOf(xrdcpdebuglevel));
				}
				if (returnEnvelope)
					command.add("-returnEnvelope");
				command.add(pfn.toString());
	
				if ((pfn.ticket.envelope != null)
						&& (pfn.ticket.envelope.getSignedEnvelope() != null))
					command.add("-OD" + pfn.ticket.envelope.getSignedEnvelope());
	
				final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(
						command);
	
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
				
				final int sleep = statRetryTimes[statRetryCounter];
	
				if (exitStatus.getExtProcExitStatus() != 0) {
					if (sleep==0){
						throw new IOException("Exit code was not zero but "
							+ exitStatus.getExtProcExitStatus()
							+ " for command : " + command.toString());
					}
					
					Thread.sleep(sleep * 1000);
					continue;					
				}
				
				if (returnEnvelope) {
					return exitStatus.getStdOut(); // TODO this is not the
													// return envelope, we have
													// to get first the xrdcp
													// implementation, than
													// finish this
				}
				
				long filesize = checkOldOutputOnSize(exitStatus.getStdOut());
				
				if (pfn.getGuid().size == filesize) {
					return pfn.ticket.envelope.getSignedEnvelope();
				}

				if (sleep==0){
					throw new IOException(
						"The xrdstat could not confirm the file to be uploaded with size, reported size: "
								+ filesize
								+ ", expected size: "
								+ pfn.getGuid().size);
				}
				
				Thread.sleep(sleep * 1000);
				continue;
				
			} 
			catch (final IOException ioe) {
				throw ioe;
			}
			catch (final Throwable t) {
				logger.log(Level.WARNING, "Caught exception", t);

				throw new IOException("Get aborted because " + t);
			}
		}
		
		return null;
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
