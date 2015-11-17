/**
 *
 */
package alien.io.protocols;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import alien.catalogue.PFN;
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

		if (f.length() != pfn.getGuid().size)
			return false;

		return true;
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
