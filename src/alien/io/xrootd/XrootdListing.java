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

import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcessBuilder;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.XrootDEnvelope;
import alien.config.ConfigUtils;
import alien.io.xrootd.envelopes.XrootDEnvelopeSigner;
import alien.se.SE;

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

		String xrdcommand = "ls " + path;

		if (se != null) {
			// dcache requires envelopes for listing

			final GUID guid = GUIDUtils.createGuid();

			final PFN pfn = new PFN(guid, se);

			pfn.pfn = se.generateProtocol() + path;

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

			// xrdcommand+="?authz=" + Format.encode(Format.replace(envelope,
			// "\n", ""));
			xrdcommand += "?authz=" + envelope;
		}

		final List<String> command = Arrays.asList("xrd", server, xrdcommand);

		logger.log(Level.INFO, "Executing:\n" + command);

		final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(command);

		pBuilder.returnOutputOnExit(true);

		pBuilder.timeout(24, TimeUnit.HOURS);

		pBuilder.redirectErrorStream(true);

		final ExitStatus exitStatus;

		try {
			exitStatus = pBuilder.start().waitFor();
		} catch (final InterruptedException ie) {
			throw new IOException("Interrupted while waiting for the following command to finish : " + command.toString());
		}

		if (exitStatus.getExtProcExitStatus() != 0) {
			// TODO something here or not ?
		}
		
		logger.log(Level.INFO, "Exit code was "+exitStatus.getExtProcExitStatus());

		final BufferedReader br = new BufferedReader(new StringReader(exitStatus.getStdOut()));

		String sLine;

		while ((sLine = br.readLine()) != null){
			if (path.equals("/"))
				logger.log(Level.INFO, "Response line: "+sLine);
			
			if (sLine.startsWith("-") || sLine.startsWith("d"))
				try {
					entries.add(new XrootdFile(sLine.trim()));
				} catch (final IllegalArgumentException iae) {
					System.err.println(iae.getMessage());
					iae.printStackTrace();
				}
			else
				System.err.println(sLine);
		}
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
