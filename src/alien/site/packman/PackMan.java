package alien.site.packman;

import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.site.JobAgent;

public class PackMan {

	static transient final Logger logger = ConfigUtils.getLogger(JobAgent.class.getCanonicalName());
	
	public boolean getHavePath (){
		return false;
	}
	
	public String[] getListPackages(){
		logger.log(Level.INFO, "PackMan: Getting list of packages shouldn't be called here!");
		return null;
	}

	public String[] getListInstalledPackages(){
		logger.log(Level.INFO, "PackMan: Getting list of installed packages shouldn't be called here!");
		return null;
	}

	public void printPackages(String[] packArray) {	
	}
	
}
