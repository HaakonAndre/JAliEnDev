package alien.catalogue;

import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.user.UsersHelper;

/**
 * @author ron
 * @since Mai 28, 2011
 */
public final class FileSystemUtils {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils
			.getLogger(CatalogueUtils.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory
			.getMonitor(CatalogueUtils.class.getCanonicalName());

	/**
	 * Get the absolute path
	 * 
	 * @param user
	 * @param currentDir
	 * @param path
	 * @return absolute path, or <code>null</code> if none could be found
	 */
	public static String getAbsolutePath(String user, String currentDir,
			String path) {

		if (path.indexOf('~') == 0)
			path = UsersHelper.getHomeDir(user)
					+ path.substring(1, path.length() - 1);
		

		if (path.indexOf('/') != 0)
			path = currentDir + path;


		if (path.contains("//")) {
			path = path.replace("///", "/");
			path = path.replace("//", "/");
		}
		
		if (path.endsWith("/") && path.length()!=1)
			path = path.substring(0, path.lastIndexOf('/'));

		while (path.contains("/./"))
			path = path.replace("/./", "/");

		
		while (path.contains("/..")) {
			int pos = path.indexOf("/..") - 1;
			String newpath = path.substring(0, pos);
			newpath = newpath.substring(0, newpath.lastIndexOf('/'));
			if (path.length() > (pos + 3))
				path = newpath + "/" + path.substring(pos + 4);
			else
				path = newpath;
		}

		if (path.endsWith("/."))
			path = path.substring(0, path.length() - 1);

		if (path.endsWith("/.."))
			path = path.substring(0, currentDir.lastIndexOf('/'));

		return path;
	}

	private static final String[] translation = new String[] { "---", "--x",
			"-w-", "-wx", "r--", "r-x", "rw-", "rwx" };

	/**
	 * Get the type+perm string for LFN
	 * 
	 * @param lfn
	 * @return type+perm String e.g. -rwxrwxr-x or drwxr-xr-x
	 */
	public static String getFormatedTypeAndPerm(final LFN lfn) {
		final StringBuilder ret = new StringBuilder(10);

		if (lfn.type != 'f')
			ret.append(lfn.type);
		else
			ret.append('-');

		for (int pos = 0; pos < 3; pos++) {
			ret.append(translation[lfn.perm.charAt(pos) - '0']);
		}

		return ret.toString();
	}

}
