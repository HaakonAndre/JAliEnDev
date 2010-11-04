/**
 * 
 */
package alien.catalogue;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import lazyj.DBFunctions;
import lazyj.cache.GenericLastValuesCache;
import alien.config.ConfigUtils;

/**
 * @author costing
 * @since Nov 3, 2010
 */
public final class CatalogueUtils {

	private static GenericLastValuesCache<Integer, Host> hostsCache = new GenericLastValuesCache<Integer, Host>() {
		private static final long serialVersionUID = 1L;

		@Override
		protected boolean cacheNulls(){
			return false;
		}
		
		@Override
		protected Host resolve(final Integer key) {
			final DBFunctions db = ConfigUtils.getDB("alice_users");
			
			db.query("SELECT * FROM HOSTS WHERE hostIndex="+key+";");
			
			if (db.moveNext()){
				return new Host(db);
			}
			
			return null;
		}
	};
	
	/**
	 * Get the host for this index
	 * 
	 * @param idx
	 * @return the Host or <code>null</code> if there is no such host
	 */
	public static Host getHost(final int idx){
		return hostsCache.get(Integer.valueOf(idx));
	}
	
	private static List<GUIDIndex> guidIndexCache = null;
	private static long guidIndexCacheUpdated = 0;
	
	private static synchronized final void updateGuidIndexCache(){
		if (System.currentTimeMillis() - guidIndexCacheUpdated > 1000*60*5 || guidIndexCache==null){
			final DBFunctions db = ConfigUtils.getDB("alice_users");
			
			db.query("SELECT * FROM GUIDINDEX ORDER BY guidTime ASC;");
			
			final LinkedList<GUIDIndex> ret = new LinkedList<GUIDIndex>();
			
			while (db.moveNext())
				ret.add(new GUIDIndex(db));
			
			guidIndexCache = ret;
			
			guidIndexCacheUpdated = System.currentTimeMillis();
		}
	}
	
	/**
	 * Get the GUIDINDEX entry that contains this timestamp (in milliseconds)
	 * 
	 * @param timestamp
	 * @return the GUIDIndex that contains this timestamp (in milliseconds)
	 */
	public static GUIDIndex getGUIDIndex(final long timestamp){
		updateGuidIndexCache();
		
		GUIDIndex old = null;
		
		for (final GUIDIndex idx: guidIndexCache){
			if (idx.guidTime>timestamp)
				return old;
			
			old = idx;
		}
		
		return null;
	}
	
	/**
	 * Get all GUIDINDEX rows
	 * 
	 * @return all GUIDINDEX rows
	 */
	public static List<GUIDIndex> getAllGUIDIndexes(){
		updateGuidIndexCache();
		
		return Collections.unmodifiableList(guidIndexCache);
	}
}
