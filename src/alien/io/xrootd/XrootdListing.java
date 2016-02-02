package alien.io.xrootd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.XrootDEnvelope;
import alien.config.ConfigUtils;
import alien.io.protocols.Xrootd;
import alien.io.xrootd.envelopes.XrootDEnvelopeSigner;
import alien.se.SE;
import lazyj.Format;
import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcessBuilder;

/**
 * @author costing
 *
 */
public class XrootdListing {

	static transient final Logger logger = ConfigUtils.getLogger(XrootdListing.class.getCanonicalName());

	/**
	 * Server host and port
	 */
	public final String server;

	/**
	 * Starting path
	 */
	public final String path;

	private Set<XrootdFile> entries;

	private SE se = null;

	/**
	 * @param server
	 *            server host and port
	 * @throws IOException
	 */
	public XrootdListing(final String server) throws IOException {
		this(server, "/");
	}

	/**
	 * @param server
	 *            server host and port
	 * @param path
	 *            starting path
	 * @throws IOException
	 */
	public XrootdListing(final String server, final String path) throws IOException {
		this(server, path, null);
	}

	/**
	 * @param server
	 *            server host and port
	 * @param path
	 *            starting path
	 * @param se
	 * @throws IOException
	 */
	public XrootdListing(final String server, final String path, final SE se) throws IOException {
		this.server = server;
		this.path = path;
		this.se = se;

		init();
	}

	private void init() throws IOException {
		entries = new TreeSet<>();

		String xrdcommand = path;

		if (se != null) {
			// dcache requires envelopes for listing

			final GUID guid = GUIDUtils.createGuid();

			guid.addKnownLFN(LFNUtils.getLFN(path, true));

			final PFN pfn = new PFN(guid, se);

			pfn.pfn = SE.generateProtocol(se.seioDaemons, path);

			final XrootDEnvelope env = new XrootDEnvelope(AccessType.READ, pfn);

			try {
				if (se.needsEncryptedEnvelope)
					XrootDEnvelopeSigner.encryptEnvelope(env);
				else
					// new xrootd implementations accept signed-only envelopes
					XrootDEnvelopeSigner.signEnvelope(env);
			} catch (final GeneralSecurityException e) {
				e.printStackTrace();
				return;
			}

			final String envelope = env.getEncryptedEnvelope();

			xrdcommand += "?authz=" + envelope;
		}

		final List<String> command = Arrays.asList(Xrootd.getXrootdDefaultPath() + "/bin/xrdfs", server, "ls", "-l", xrdcommand);

		logger.log(Level.INFO, "Executing:\n" + command);

		final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(command);

		Xrootd.checkLibraryPath(pBuilder);

		pBuilder.returnOutputOnExit(true);

		pBuilder.timeout(1, TimeUnit.HOURS);

		pBuilder.redirectErrorStream(true);

		final ExitStatus exitStatus;

		try {
			exitStatus = pBuilder.start().waitFor();
		} catch (final InterruptedException ie) {
			throw new IOException("Interrupted while waiting for the following command to finish : " + command.toString());
		}

		final int exitCode = exitStatus.getExtProcExitStatus();

		if (exitCode != 0)
			logger.log(Level.WARNING, "Exit code was " + exitCode + " for " + command + ":\n" + exitStatus.getStdOut());

		String listing = exitStatus.getStdOut();

		final int idxAuthz = listing.indexOf("?authz=");

		if (idxAuthz > 0) {
			final char c = listing.charAt(idxAuthz - 1);

			int idxAuthzEnd = listing.indexOf("-----END SEALED ENVELOPE-----", idxAuthz);

			if (idxAuthzEnd >= 0) {
				idxAuthzEnd = listing.indexOf('/', idxAuthzEnd);

				final String token = listing.substring(idxAuthz, idxAuthzEnd + (c == '/' ? 1 : 0));

				listing = Format.replace(listing, token, "");
			}
		}

		final BufferedReader br = new BufferedReader(new StringReader(listing));

		String sLine;

		while ((sLine = br.readLine()) != null)
			if (sLine.startsWith("-") || sLine.startsWith("d"))
				try {
					entries.add(new XrootdFile(sLine.trim()));
				} catch (final IllegalArgumentException iae) {
					logger.log(Level.WARNING, "Exception parsing response of " + command, iae);
				}
			else if (sLine.length() > 0 && sLine.trim().length() > 0)
				logger.log(Level.WARNING, "Unknown response line in the output of " + command + "\n\n" + sLine);
	}

	/**
	 * @return the subdirectories of this entry
	 */
	public Set<XrootdFile> getDirs() {
		final Set<XrootdFile> ret = new TreeSet<>();

		for (final XrootdFile entry : entries)
			if (entry.isDirectory())
				ret.add(entry);

		return ret;
	}

	/**
	 * @return the files in this directory (or itself if it's a file already)
	 */
	public Set<XrootdFile> getFiles() {
		final Set<XrootdFile> ret = new TreeSet<>();

		for (final XrootdFile entry : entries)
			if (entry.isFile())
				ret.add(entry);

		return ret;
	}

}
