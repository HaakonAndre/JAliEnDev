package alien.catalogue;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import lazyj.Format;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;


/**
 * @author ron
 * @since Nov 23, 2011
 */
public class PackageUtils {
	

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(IndexTableEntry.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(IndexTableEntry.class.getCanonicalName());
	
	
	/**
	 * @param platform
	 * @return list of packages for platform
	 */
	public static List<Package> getPackagesForPlatform(final String platform){

		
		final DBFunctions db = ConfigUtils.getDB("alice_users");
		

		final List<Package> ret = new ArrayList<Package>();
		
		if (monitor!=null){
			monitor.incrementCounter("LFN_db_lookup");
		}
		
		String q = "SELECT DISTINCT * FROM PACKAGES WHERE platform='"+Format.escSQL(platform)+"'";
		
		
		if (!db.query(q))
			return null;
		
		while (!db.moveNext()){
			if (logger.isLoggable(Level.FINE))
				logger.log(Level.FINE, "Empty result set for "+q);
			
			ret.add(new Package(db));
			return null;
		}
		
		return ret;
	}
}
