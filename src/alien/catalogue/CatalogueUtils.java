/**
 * 
 */
package alien.catalogue;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
	
	/**
	 * Wrapper around a row in INDEXTABLE
	 * 
	 * @author costing
	 */
	public static class IndexTableEntry {
		/**
		 * Index id
		 */
		public final int indexId;
		
		/**
		 * Host and database where this table is located
		 */
		public final int hostIndex;
		
		/**
		 * Table name
		 */
		public final int tableName;
		
		/**
		 * LFN prefix
		 */
		public final String lfn;
		
		/**
		 * Initialize from one entry in INDEXTABLE
		 * 
		 * @param db
		 */
		public IndexTableEntry(final DBFunctions db){
			indexId = db.geti("indexId");
			hostIndex = db.geti("hostIndex");
			tableName = db.geti("tableName");
			lfn = db.gets("lfn");
		}
		
		@Override
		public String toString() {
			return "IndexTableEntry indexId: "+indexId+"\n"+
			       "hostIndex\t\t: "+hostIndex+"\n"+
			       "tableName\t\t: "+tableName+"\n"+
			       "lfn\t\t\t: "+lfn+"\n";
		}
	}
	
	private static Map<Integer, IndexTableEntry> indextable = null;
	private static long lastIndexTableUpdate = 0;

	private static synchronized void updateIndexTableCache(){
		if (System.currentTimeMillis() - lastIndexTableUpdate > 1000*60*5 || indextable==null){
			final Map<Integer, IndexTableEntry> newIndextable = new HashMap<Integer, CatalogueUtils.IndexTableEntry>();
			
			final DBFunctions db = ConfigUtils.getDB("alice_users");
			
			db.query("SELECT * FROM INDEXTABLE;");
			
			while (db.moveNext()){
				final IndexTableEntry entry = new IndexTableEntry(db);
				
				newIndextable.put(Integer.valueOf(entry.tableName), entry);
			}
			
			indextable = newIndextable;
			lastIndexTableUpdate = System.currentTimeMillis();
		}
	}
	
	/**
	 * Get the base folder for this table name 
	 * 
	 * @param tableName
	 * @return entry in INDEXTABLE for this table name
	 */
	public static IndexTableEntry getIndexTable(final int tableName){
		updateIndexTableCache();

		System.err.println(tableName);
		
		return indextable.get(Integer.valueOf(tableName));
	}
	
	/**
	 * For a given path, get the closest match for LFNs from INDEXTABLE
	 * 
	 * @param pattern
	 * @return the best match, or <code>null</code> if none could be found
	 */
	public static IndexTableEntry getClosestMatch(final String pattern){
		updateIndexTableCache();
		
		int bestLen = 0;
		
		IndexTableEntry best = null;
		
		for (final IndexTableEntry ite: indextable.values()){
			if (pattern.startsWith(ite.lfn)){
				if (ite.lfn.length() > bestLen){
					best = ite;
					bestLen = ite.lfn.length();
				}
			}
		}
		
		return best;
	}
}
