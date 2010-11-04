/**
 * 
 */
package alien.se;

import java.util.HashMap;
import java.util.Map;

import lazyj.DBFunctions;
import alien.config.ConfigUtils;

/**
 * @author costing
 * @since Nov 4, 2010
 */
public final class SEUtils {

	private static Map<Integer, SE> seCache = null;
	
	private static long seCacheUpdated = 0;
	
	private static final void updateSECache(){
		if (System.currentTimeMillis() - seCacheUpdated > 1000*60*5 || seCache == null){
			final Map<Integer, SE> ses = new HashMap<Integer, SE>();
			
			final DBFunctions db = ConfigUtils.getDB("alice_users");
			
			db.query("SELECT * FROM SE;");
			
			while (db.moveNext()){
				final SE se = new SE(db);
				
				ses.put(Integer.valueOf(se.seNumber), se);
			}
			
			seCache = ses;
			seCacheUpdated = System.currentTimeMillis();
		}
	}
	
	/**
	 * Get the SE by its number
	 * 
	 * @param seNumber
	 * @return the SE, if it exists, or <code>null</code> if it doesn't
	 */
	public SE getSE(final int seNumber){
		updateSECache();
		
		return seCache.get(Integer.valueOf(seNumber));
	}
	
}
