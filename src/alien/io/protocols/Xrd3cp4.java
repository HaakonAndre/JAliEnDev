/**
 * 
 */
package alien.io.protocols;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcessBuilder;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;

/**
 * 3rd party Xrootd transfers using the default client in Xrootd 4+
 * 
 * @author costing
 * @since Jun 16 2015
 */
public class Xrd3cp4 extends Xrootd {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9084272684664087714L;

	/**
	 * package protected
	 */
	Xrd3cp4() {
		// package protected
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see alien.io.protocols.Protocol#transfer(alien.catalogue.PFN,
	 * alien.catalogue.access.CatalogueReadAccess, alien.catalogue.PFN,
	 * alien.catalogue.access.CatalogueWriteAccess)
	 */
	@Override
	public String transfer(final PFN source, final PFN target) throws IOException {
		// direct copying between two storages

		if (!xrootdNewerThan4)
			throw new IOException("Xrootd client v4+ is required for Xrd3cp4");
		
		try {
			if (source.ticket == null || source.ticket.type != AccessType.READ)
				throw new IOException("The ticket for source PFN " + source.toString() + " could not be found or is not a READ one.");

			if (target.ticket == null || target.ticket.type != AccessType.WRITE)
				throw new IOException("The ticket for target PFN " + target.toString() + " could not be found or is not a WRITE one.");

			final List<String> command = new LinkedList<>();
			command.add("xrdcp");
			command.add("--tpc");
			command.add("only");
			command.add("--force");
			command.add("--path");
			command.add("--posc");

			final boolean sourceEnvelope = source.ticket != null && source.ticket.envelope != null;

			final boolean targetEnvelope = target.ticket != null && target.ticket.envelope != null;

			String sourcePath;
			
			String targetPath;
			
			if (sourceEnvelope)
				sourcePath = source.ticket.envelope.getTransactionURL();
			else
				sourcePath = source.pfn;

			if (targetEnvelope)
				targetPath = target.ticket.envelope.getTransactionURL();
			else
				targetPath = target.pfn;

			if (sourceEnvelope)
				if (source.ticket.envelope.getEncryptedEnvelope() != null)
					sourcePath += "?authz=" + source.ticket.envelope.getEncryptedEnvelope();
				else if (source.ticket.envelope.getSignedEnvelope() != null)
					sourcePath += "?"+source.ticket.envelope.getSignedEnvelope();

			if (targetEnvelope)
				if (target.ticket.envelope.getEncryptedEnvelope() != null)
					targetPath += "?authz=" + target.ticket.envelope.getEncryptedEnvelope();
				else if (target.ticket.envelope.getSignedEnvelope() != null)
					targetPath += "?"+target.ticket.envelope.getSignedEnvelope();

			command.add(sourcePath);
			command.add(targetPath);
			
			final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(command);

			pBuilder.returnOutputOnExit(true);

			long seconds = source.getGuid().size / 200000; // average target
															// speed: 200KB/s

			seconds += 5 * 60; // 5 minutes extra time, handshakes and such

			pBuilder.timeout(seconds, TimeUnit.SECONDS);

			pBuilder.redirectErrorStream(true);

			final ExitStatus exitStatus;

			try {
				exitStatus = pBuilder.start().waitFor();
			} catch (final InterruptedException ie) {
				throw new IOException("Interrupted while waiting for the following command to finish : " + command.toString());
			}

			if (exitStatus.getExtProcExitStatus() != 0) {
				String sMessage = parseXrootdError(exitStatus.getStdOut());

				logger.log(Level.WARNING, "TRANSFER failed with " + exitStatus.getStdOut());

				if (sMessage != null)
					sMessage = "xrdcp --tpc only exited with " + exitStatus.getExtProcExitStatus() + ": " + sMessage;
				else
					sMessage = "Exit code was " + exitStatus.getExtProcExitStatus() + " for command : " + command.toString();

				if (exitStatus.getExtProcExitStatus() == 5 && exitStatus.getStdOut().indexOf("source or destination has 0 size") >= 0) {
					logger.log(Level.WARNING, "Retrying xrdstat, maybe the file shows up with the correct size in a few seconds");

					try {
						final String ret = xrdstat(target, (target.ticket.envelope.getSignedEnvelope() == null));

						if (ret != null) {
							logger.log(Level.WARNING, "xrdstat is ok, assuming transfer was successful");

							return ret;
						}
					} catch (final IOException ioe) {
						logger.log(Level.WARNING, "xrdstat throwed exception", ioe);
					}
				}

				if (sMessage.indexOf("unable to connect to destination") >= 0 || sMessage.indexOf("No servers are available to write the file.") >= 0 || sMessage.indexOf("Unable to create") >= 0
						|| sMessage.indexOf("dest-size=0 (source or destination has 0 size!)") >= 0)
					throw new TargetException(sMessage);

				if (sMessage.indexOf("No servers have the file") >= 0 || sMessage.indexOf("No such file or directory") >= 0)
					throw new SourceException(sMessage);

				throw new IOException(sMessage);
			}

			return xrdstat(target, (target.ticket.envelope.getSignedEnvelope() == null));
		} catch (final IOException ioe) {
			throw ioe;
		} catch (final Throwable t) {
			logger.log(Level.WARNING, "Caught exception", t);

			throw new IOException("Transfer aborted because " + t);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "xrd3cp4";
	}

	@Override
	int getPreference() {
		return 2;
	}
}
