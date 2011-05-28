package alien.catalogue;

import java.util.UUID;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.user.AliEnPrincipal;
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

		if (path.substring(0, 1) == "/")
			return path;
		else if (path.substring(0, 1) == "~")
			path = UsersHelper.getHomeDir(user) 
					+ path.substring(1, path.length() - 1);
		else
			path = currentDir + path;

		if (path.contains(".")) {
			if (path.contains("/..")) {
				path.indexOf("/..");
				// TODO:
			}
			if (path.contains("./")) {
				path.replace("./", "");
			}

		}
		if (path.contains("//")) {
			// TODO:
		}
		if (path.endsWith("/"))
			path = path.substring(0, path.length() - 2);

		return path;
	}

}
