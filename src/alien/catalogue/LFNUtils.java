package alien.catalogue;

/**
 * LFN utilities
 * 
 * @author costing
 *
 */
public class LFNUtils {

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
		
		if (ite==null)
			return null;
		
		return ite.getLFN(fileName, evenIfDoesntExist);
	}
	
}
