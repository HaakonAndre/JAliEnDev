package alien.catalogue;

import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;

/**
 * LFN utilities
 * 
 * @author costing
 *
 */
public class LFNUtils {

	static transient final Logger logger = ConfigUtils.getLogger(LFNUtils.class.getCanonicalName());
	
	/**
	 * Get the LFN entry for this catalog filename
	 * 
	 * @param fileName
	 * @return LFN entry
	 */
	public static LFN getLFN(final String fileName){
		return getLFN(fileName, false);
	}

	/**
	 * Get the LFN entry for this catalog filename, optionally returning an empty object
	 * if the entry doesn't exist (yet)
	 * 
	 * @param fileName
	 * @param evenIfDoesntExist
	 * @return entry
	 */
	public static LFN getLFN(final String fileName, final boolean evenIfDoesntExist){
		if (fileName == null || fileName.length() == 0)
			return null;
		
		final IndexTableEntry ite = CatalogueUtils.getClosestMatch(fileName);
		
		if (ite==null){
			logger.log(Level.FINE, "IndexTableEntry is null for: "+fileName+" (even if doesn't exist: "+evenIfDoesntExist+")");
			
			return null;
		}
		
		if (logger.isLoggable(Level.FINER))
			logger.log(Level.FINER, "Using "+ite+" for: "+fileName);
		
		return ite.getLFN(fileName, evenIfDoesntExist);
	}
	
}
