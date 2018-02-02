/**
 *
 */
package alien.io.protocols;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import lia.util.process.ExternalProcess.ExitStatus;

/**
 * @author costing
 * @since Dec 8, 2010
 */
public abstract class Protocol implements Serializable, Comparable<Protocol> {

	/**
	 *
	 */
	private static final long serialVersionUID = 6159560194424790552L;

	private transient final static Logger logger = ConfigUtils.getLogger(Protocol.class.getCanonicalName());

	/**
	 * Package protected
	 */
	Protocol() {
		// package protected
	}

	/**
	 * @param pfn
	 *            pfn to delete
	 * @return <code>true</code> if the file was deleted, <code>false</code> if the file doesn't exist
	 * @throws IOException
	 *             in case of access problems
	 */
	public abstract boolean delete(final PFN pfn) throws IOException;

	/**
	 * Download the file locally
	 *
	 * @param pfn
	 *            location
	 * @param localFile
	 *            local file. Can be <code>null</code> to generate a temporary file name. If specified, it must point to a file name that doesn't exist yet but can be created by this user.
	 * @return local file, either the same that was passed or a temporary file name
	 * @throws IOException
	 *             in case of problems
	 */
	public abstract File get(final PFN pfn, final File localFile) throws IOException;

	/**
	 * Upload the local file
	 *
	 * @param pfn
	 *            target PFN
	 * @param localFile
	 *            local file name (which must exist of course)
	 * @return storage reply envelope
	 * @throws IOException
	 *             in case of problems
	 */
	public abstract String put(final PFN pfn, final File localFile) throws IOException;

	/**
	 * Direct transfer between the two involved storage elements
	 *
	 * @param source
	 * @param target
	 * @return storage reply envelope
	 * @throws IOException
	 */
	public abstract String transfer(final PFN source, final PFN target) throws IOException;

	/**
	 * Check the consistency of the downloaded file
	 *
	 * @param f
	 * @param pfn
	 * @return <code>true</code> if the file matches catalogue information, <code>false</code> otherwise
	 */
	public static boolean checkDownloadedFile(final File f, final PFN pfn) {
		if (f == null || !f.exists() || !f.isFile())
			return false;

		final GUID guid = pfn.getGuid();

		if (f.length() != guid.size)
			return false;

		if (isValidMD5(guid.md5)) {
			try {
				String fileMD5 = IOUtils.getMD5(f);

				if (!fileMD5.equalsIgnoreCase(guid.md5))
					return false;

			} catch (IOException e) {
				logger.log(Level.SEVERE, "Error during MD5 check of " + f.getAbsolutePath());
				logger.log(Level.SEVERE, e.getMessage());
				return false;
			}
		}
		else {
			final LFN lfn = LFNUtils.getLFN(guid);
			if (lfn != null && isValidMD5(lfn.md5)) {
				try {
					String fileMD5 = IOUtils.getMD5(f);

					if (!fileMD5.equalsIgnoreCase(lfn.md5))
						return false;

				} catch (IOException e) {
					logger.log(Level.SEVERE, "Error during MD5 check of " + f.getAbsolutePath());
					logger.log(Level.SEVERE, e.getMessage());
					return false;
				}
			}
		}
		// otherwise don't check md5 at all

		return true;
	}

	private final static Pattern md5pattern = Pattern.compile("[a-fA-F0-9]{32}");

	/**
	 * Check if a string is a valid md5 hash
	 *
	 * @param s
	 *            string to check
	 * @return <code>true</code> if a string is a valid md5 hash, <code>false</code> otherwise
	 */
	private static boolean isValidMD5(String s) {
		if (s != null && s.length() > 0)
			return md5pattern.matcher(s).matches();

		return false;
	}

	@Override
	public int compareTo(final Protocol o) {
		return this.getPreference() - o.getPreference();
	}

	/**
	 * @return protocol preference, to sort by
	 */
	abstract int getPreference();

	/**
	 * @return <code>true</code> if the protocol is supported (by the tools for example)
	 */
	abstract boolean isSupported();

	/**
	 * @return unique identifier of each protocol
	 */
	public abstract byte protocolID();

	private ExitStatus lastExitStatus = null;

	/**
	 * @return exit status of last executed command
	 */
	public ExitStatus getLastExitCode() {
		return lastExitStatus;
	}

	/**
	 * Set the status of last executed command
	 *
	 * @param status
	 */
	protected void setLastExitStatus(final ExitStatus status) {
		this.lastExitStatus = status;
	}

	private List<String> lastCommand = null;

	/**
	 * @return full command line that was last executed
	 */
	public List<String> getLastCommand() {
		return lastCommand;
	}

	/**
	 * Keep the command line that was last executed, for logging purposes
	 *
	 * @param cmd
	 */
	protected void setLastCommand(final List<String> cmd) {
		this.lastCommand = cmd;
	}

	/**
	 * Add a parameter to an existing URL. It assumes the parameter is already formatted (usual "key=value" syntax)
	 *
	 * @param URL
	 * @param parameter
	 * @return the modified URL
	 */
	public static String addURLParameter(final String URL, final String parameter) {
		if (URL.indexOf('?') > 0)
			return URL + "&" + parameter;

		return URL + "?" + parameter;
	}
}
