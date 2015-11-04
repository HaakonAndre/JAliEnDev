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

import lia.util.Utils;
import alien.config.ConfigUtils;
import alien.site.JobAgent;

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
			alienv_bin = Utils.getOutput("which /cvmfs/alice.cern.ch/bin/alienv").trim();
		} catch (Exception e) {
			System.out.println("which alienv not ok!");
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
			String listPackages = Utils.getOutput(alienv_bin + " q --packman");
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
			String listPackages = Utils.getOutput(alienv_bin + " q --packman");
			return Arrays.asList(listPackages.split("\n"));
		}
		return null;
	}

	public String getMethod() {
		return "CVMFS";
	}
	
	public Map<String,String> installPackage (String user, String packages, String version){
		HashMap<String,String> environment = new HashMap<>();		
		String args = packages;
		  
		if (version != null) {
			args += "/" + version;
		}

		String source = Utils.getOutput(alienv_bin + " printenv "+args);
		    
		ArrayList<String> parts =  new ArrayList<String>(Arrays.asList( source.split(";") ));
		parts.remove(parts.size()-1);
		
		for (String value : parts){
			if( !value.contains("export") ){
				String[] str = value.split("=");
				environment.put(str[0], str[1]);
			}
		}
		
		return environment;
	} 
	
}
