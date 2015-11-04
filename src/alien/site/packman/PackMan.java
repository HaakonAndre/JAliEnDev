package alien.site.packman;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.site.JobAgent;

/**
 * 
 */
public class PackMan {

	private static transient final Logger logger = ConfigUtils.getLogger(JobAgent.class.getCanonicalName());

	/**
	 * @return ?
	 */
	public boolean getHavePath() {
		return false;
	}

	/**
	 * @return all defined packages
	 */
	public List<String> getListPackages() {
		logger.log(Level.INFO, "PackMan: Getting list of packages shouldn't be called here!");
		return null;
	}

	/**
	 * @return list of installed packages
	 */
	public List<String> getListInstalledPackages() {
		logger.log(Level.INFO, "PackMan: Getting list of installed packages shouldn't be called here!");
		return null;
	}

	/**
	 * @param packArray
	 */
	public void printPackages(List<String> packArray) {
		logger.log(Level.INFO, this.getClass().getCanonicalName() + " printing list of packages ");

		for (String pack : packArray != null ? packArray : getListPackages()) {
			System.out.println(pack);
		}

		return;
	}

	public String getMethod() {
		return "PackMan";
	}
	
	public Map<String,String> installPackage (String user, String packages, String version){
		return null;
	} 

}
