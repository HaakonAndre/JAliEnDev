/**
 *
 */
package alien.site.packman;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.site.JobAgent;
import lia.util.Utils;

/**
 * @author mmmartin
 *
 */
public class CVMFS extends PackMan {

	static transient final Logger logger = ConfigUtils.getLogger(JobAgent.class.getCanonicalName());

	private String alienv_bin = "";
	private boolean havePath = true;

	/**
	 * Constructor just checks CVMFS bin exist
	 */
	public CVMFS() {
		try {
			//alienv_bin = Utils.getOutput("which /cvmfs/alice.cern.ch/bin/alienv").trim();
			alienv_bin = Utils.getOutput("which alienv").trim();
		} catch (final Exception e) {
			System.out.println("which alienv not ok: " + e.getMessage());
		}

		if (alienv_bin == null || alienv_bin.equals(""))
			havePath = false;

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
			final String listPackages = Utils.getOutput(alienv_bin + " q --packman");
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
			final String listPackages = Utils.getOutput(alienv_bin + " q --packman");
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

		final String source = Utils.getOutput(alienv_bin + " printenv " + args);

		final ArrayList<String> parts = new ArrayList<>(Arrays.asList(source.split(";")));
		parts.remove(parts.size() - 1);

		for (final String value : parts)
			if (!value.contains("export")) {
				final String[] str = value.split("=");

				if (str[1].contains("\\"))
					str[1] = str[1].replace("\\", "");

				environment.put(str[0], str[1]);
			}

		return environment;
	}

}
