/**
 *
 */
package alien.site.packman;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.site.JobAgent;
import lazyj.commands.SystemCommand;

/**
 * @author mmmartin
 *
 */
public class CVMFS extends PackMan {

	/**
	 * logger object
	 */
	static final Logger logger = ConfigUtils.getLogger(JobAgent.class.getCanonicalName());

	private boolean havePath = true;
	/**
	 * CVMFS setup specifics
	 */

	static String ALIENV_DIR = "/cvmfs/alice.cern.ch/bin";
	final static String JAVA32_DIR = "/cvmfs/alice.cern.ch/java/JDKs/i686/jdk-latest/bin";

	/**
	 * Constructor just checks CVMFS bin exist
	 * 
	 * @param location
	 */
	public CVMFS(String location) {		
		if (location != null && !location.isBlank()) {
			location.replaceAll("/$", ""); //Remove trailing '/' if exists
			if (Files.exists(Paths.get(location + "/alienv")))
				ALIENV_DIR = location;
			else {
				havePath = false;
				ALIENV_DIR = null;
			}
		}
	}

	/**
	 * returns if alienv was found on the system
	 */
	@Override
	public boolean getHavePath() {
		return havePath;
	}

	/**
	 * get the list of packages in CVMFS, returns an array
	 */
	@Override
	public List<String> getListPackages() {
		logger.log(Level.INFO, "PackMan-CVMFS: Getting list of packages ");

		if (this.getHavePath()) {
			final String listPackages = SystemCommand.bash(ALIENV_DIR + "/alienv q --packman").stdout;
			return Arrays.asList(listPackages.split("\n"));
		}

		return null;
	}

	/**
	 * get the list of installed packages in CVMFS, returns an array
	 */
	@Override
	public List<String> getListInstalledPackages() {
		logger.log(Level.INFO, "PackMan-CVMFS: Getting list of packages ");

		if (this.getHavePath()) {
			final String listPackages = SystemCommand.bash(ALIENV_DIR + "/alienv q --packman").stdout;
			return Arrays.asList(listPackages.split("\n"));
		}
		return null;
	}

	@Override
	public String getMethod() {
		return "CVMFS";
	}

	@Override
	public Map<String, String> installPackage(final String user, final String packages, final String version) {
		final HashMap<String, String> environment = new HashMap<>();
		String args = packages;

		if (version != null)
			args += "/" + version;

		final String source = SystemCommand.bash(ALIENV_DIR + "/alienv printenv " + args).stdout;

		final ArrayList<String> parts = new ArrayList<>(Arrays.asList(source.split(";")));
		parts.remove(parts.size() - 1);

		for (final String value : parts)
			if (!value.contains("export")) {
				final String[] str = value.split("=");

				if (str[1].contains("\\"))
					str[1] = str[1].replace("\\", "");

				environment.put(str[0], str[1].trim()); // alienv adds a space at the end of each entry
			}

		return environment;
	}

	/**
	 * @return the command to get the full environment to run JAliEn components
	 */
	public static String getAlienvForSource() {
		return ALIENV_DIR + "/alienv printenv JAliEn" + getJAliEnVersion();
	}

	private static String getJAliEnVersion() {
		try {
			final String loadedmodules = System.getenv().get("LOADEDMODULES");
			final int jalienModulePos = loadedmodules.lastIndexOf(":JAliEn/");

			String jalienVersionString = "";
			if (jalienModulePos > 0) {
				jalienVersionString = loadedmodules.substring(jalienModulePos + 7);

				if (jalienVersionString.contains(":"))
					jalienVersionString = jalienVersionString.substring(0, jalienVersionString.indexOf(':'));
			}
			return jalienVersionString;
		}
		catch (StringIndexOutOfBoundsException | NullPointerException e) {
			logger.log(Level.WARNING, "Could not get jAliEn version", e);
			return "";
		}
	}

	/**
	 * @return 32b JRE location in CVMFS, to be used for all WN activities due to its much lower virtual memory footprint
	 */
	public static String getJava32DirFromCVMFS() {
		return JAVA32_DIR;
	}
}
