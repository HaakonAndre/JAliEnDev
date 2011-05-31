package alien.catalogue;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import lazyj.Format;
import alien.config.ConfigUtils;

/**
 * LFN utilities
 * 
 * @author costing
 *
 */
public class LFNUtils {
	
	/**
	 * Logger
	 */
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
		
		
		String processedFileName = fileName;
		
		while (processedFileName.indexOf("//")>=0)
			processedFileName = Format.replace(processedFileName, "//", "/");
		
		final IndexTableEntry ite = CatalogueUtils.getClosestMatch(fileName);
		
		if (ite==null){
			logger.log(Level.FINE, "IndexTableEntry is null for: "+fileName+" (even if doesn't exist: "+evenIfDoesntExist+")");
			
			return null;
		}
		
		if (logger.isLoggable(Level.FINER))
			logger.log(Level.FINER, "Using "+ite+" for: "+fileName);
		
		return ite.getLFN(fileName, evenIfDoesntExist);
	}
	
	/**
	 * Make sure the parent directory exists
	 * 
	 * @param lfn
	 * @return the updated LFN entry
	 */
	static LFN ensureDir(final LFN lfn){
		if (lfn.exists)
			return lfn;
		
		LFN parent = lfn.getParentDir(true);
		
		if (!parent.exists){
			parent.owner = lfn.owner;
			parent.gowner = lfn.gowner;
			parent = ensureDir(parent);
		}
		
		if (parent == null)
			return null;
		
		lfn.parentDir = parent;
		lfn.type = 'd';
		
		if (insertLFN(lfn))
			return lfn;
		
		return null;
	}
	
	/**
	 * Insert an LFN in the catalogue
	 * 
	 * @param lfn
	 * @return true if the entry was inserted (or previously existed), false if there was an error
	 */
	static boolean insertLFN(final LFN lfn){
		if (lfn.exists){
			// nothing to be done, the entry already exists
			return true;
		}
		
		final IndexTableEntry ite = CatalogueUtils.getClosestMatch(lfn.getCanonicalName());
		
		if (ite==null){
			logger.log(Level.WARNING, "IndexTableEntry is null for: "+lfn.getCanonicalName());
			
			return false;
		}
		
		final LFN parent = ensureDir(lfn.getParentDir());
		
		if (parent==null){
			logger.log(Level.WARNING, "Parent dir is null for "+lfn.getCanonicalName());
			
			return false;
		}
		
		lfn.parentDir = parent;
		lfn.indexTableEntry = ite;
		
		if (lfn.indexTableEntry.equals(parent.indexTableEntry))
			lfn.dir = parent.entryId;
		
		return lfn.insert();
	}
	
	/**
	 * the "-s" flag of AliEn `find`
	 */
	public static final int FIND_NO_SORT = 1;
	
	/**
	 * the "-d" flag of AliEn `find`
	 */
	public static final int FIND_INCLUDE_DIRS = 2;
	
	/**
	 * @param path
	 * @param pattern
	 * @param flags a combination of FIND_* flags
	 * @return the list of LFNs that match
	 */
	public static List<LFN> find(final String path, final String pattern, final int flags){
		final List<LFN> ret = new ArrayList<LFN>();
		
		final List<IndexTableEntry> matchingTables = CatalogueUtils.getAllMatchingTables(path);

		final String processedPattern = Format.replace(pattern, "*", "%");
		
		for (final IndexTableEntry ite: matchingTables){
			final List<LFN> findResults = ite.find(path, processedPattern, flags);
			
			if (findResults!=null && findResults.size()>0)
				ret.addAll(findResults);
		}
		
		return ret;		
	}
	
}
