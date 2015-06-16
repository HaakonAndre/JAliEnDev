/**
 * 
 */
package alien.site.packman;

import java.util.logging.Level;
import java.util.logging.Logger;

import lia.util.Utils;
import alien.config.ConfigUtils;
import alien.site.JobAgent;
import alien.test.utils.Functions;

/**
 * @author mmmartin
 * 
 */
public class CVMFS extends PackMan{

	static transient final Logger logger = ConfigUtils.getLogger(JobAgent.class.getCanonicalName());
	
	private String alienv_bin = "";
	private boolean havePath = true;

	/**
	 * Constructor just checks CVMFS bin exist
	 */
	public CVMFS() {
		try {
			alienv_bin = Utils.getOutput("which /cvmfs/alice.cern.ch/bin/alienv").trim();
		}
		catch (Exception e){
			System.out.println("which alienv not ok!");
		}
		
		if( alienv_bin == null || alienv_bin.equals("") )
			havePath=false;
			
	}
	
	/**
	 * returns if alienv was found on the system
	 */
	public boolean getHavePath (){
		return havePath;
	}
	
	/**
	 * get the list of packages in CVMFS, returns an array
	 */
	public String[] getListPackages(){
		logger.log(Level.INFO, "PackMan-CVMFS: Getting list of packages ");

		String[] packArray = null;
		
		if(this.getHavePath()){
			String listPackages = Utils.getOutput(alienv_bin+" q --packman");	
			packArray = listPackages.split("\n");
			return packArray;
		}
		return packArray;
	}

	/**
	 * get the list of installed packages in CVMFS, returns an array
	 */
	public String[] getListInstalledPackages(){
		logger.log(Level.INFO, "PackMan-CVMFS: Getting list of packages ");

		String[] packArray = null;
		
		if(this.getHavePath()){
			String listPackages = Utils.getOutput(alienv_bin+" q --packman");	
			packArray = listPackages.split("\n");
			return packArray;
		}
		return packArray;
	}

	
	/**
	 * prints out the packages
	 */
	public void printPackages(String[] packArray) {	
		logger.log(Level.INFO, "PackMan-CVMFS: Printing list of packages ");

		if (packArray == null)
			packArray = this.getListPackages();
		
		for (String pack : packArray){
			System.out.println(pack);
		}
		return;
	}

}
