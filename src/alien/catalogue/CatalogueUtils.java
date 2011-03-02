/**
 * 
 */
package alien.catalogue;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import lazyj.cache.GenericLastValuesCache;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;

/**
 * @author costing
 * @since Nov 3, 2010
 */
public final class CatalogueUtils {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(CatalogueUtils.class.getCanonicalName());
	
	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(CatalogueUtils.class.getCanonicalName());
	
	private static GenericLastValuesCache<Integer, Host> hostsCache = new GenericLastValuesCache<Integer, Host>() {
		private static final long serialVersionUID = 1L;

		@Override
		protected boolean cacheNulls(){
			return false;
		}
		
		@Override
		protected Host resolve(final Integer key) {
			final DBFunctions db = ConfigUtils.getDB("alice_users");
			
			if (!db.query("SELECT * FROM HOSTS WHERE hostIndex="+key+";"))
				return null;
			
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
	
	private static final ReentrantReadWriteLock guidIndexRWLock = new ReentrantReadWriteLock();
	private static final ReadLock guidIndexReadLock = guidIndexRWLock.readLock();
	private static final WriteLock guidIndexWriteLock = guidIndexRWLock.writeLock();
	
	/**
	 * For how long the caches are active
	 */
	public static final long CACHE_TIMEOUT = 1000 * 60 * 5;
	
	private static final void updateGuidIndexCache() {
		guidIndexReadLock.lock();

		try{
			if (System.currentTimeMillis() - guidIndexCacheUpdated > CACHE_TIMEOUT || guidIndexCache == null) {
				guidIndexReadLock.unlock();
				
				guidIndexWriteLock.lock();
				
				try{
					if (System.currentTimeMillis() - guidIndexCacheUpdated > CACHE_TIMEOUT || guidIndexCache == null) {
						if (logger.isLoggable(Level.FINER)) {
							logger.log(Level.FINER, "Updating GUIDINDEX cache");
						}
	
						final DBFunctions db = ConfigUtils.getDB("alice_users");
	
						if (db!=null && db.query("SELECT * FROM GUIDINDEX ORDER BY guidTime ASC;")){
							final LinkedList<GUIDIndex> ret = new LinkedList<GUIDIndex>();
		
							while (db.moveNext())
								ret.add(new GUIDIndex(db));
		
							guidIndexCache = ret;
		
							guidIndexCacheUpdated = System.currentTimeMillis();
						}
						else{
							// in case of a DB connection failure, try again in 10 seconds, until then reuse the existing value (if any)
							guidIndexCacheUpdated = System.currentTimeMillis() - CACHE_TIMEOUT + 1000*10;
						}
					}
				}
				finally {
					guidIndexWriteLock.unlock();
				}
				
				guidIndexReadLock.lock();
			}
		}
		finally{
			guidIndexReadLock.unlock();
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

		if (guidIndexCache==null)
			return null;
		
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
		
		if (guidIndexCache==null)
			return null;
		
		return Collections.unmodifiableList(guidIndexCache);
	}
	
	private static Set<IndexTableEntry> indextable = null;
	private static Set<String> tableentries = null;
	private static long lastIndexTableUpdate = 0;
	
	private static final ReentrantReadWriteLock indextableRWLock = new ReentrantReadWriteLock();
	private static final ReadLock indextableReadLock = indextableRWLock.readLock();
	private static final WriteLock indextableWriteLock = indextableRWLock.writeLock();

	private static void updateIndexTableCache() {
		indextableReadLock.lock();

		try{
			if (System.currentTimeMillis() - lastIndexTableUpdate > CACHE_TIMEOUT || indextable == null) {
				indextableReadLock.unlock();
					
				indextableWriteLock.lock();
					
				try{
					if (System.currentTimeMillis() - lastIndexTableUpdate > CACHE_TIMEOUT || indextable == null) {
						if (logger.isLoggable(Level.FINER)) {
							logger.log(Level.FINER, "Updating INDEXTABLE cache");
						}
	
						final DBFunctions db = ConfigUtils.getDB("alice_users");
	
						if (db!=null && db.query("SELECT * FROM INDEXTABLE;")){
							final Set<IndexTableEntry> newIndextable = new HashSet<IndexTableEntry>();
							final Set<String> newTableentries = new HashSet<String>(); 
							
							while (db.moveNext()) {
								final IndexTableEntry entry = new IndexTableEntry(db);
		
								newIndextable.add(entry);
								
								newTableentries.add(db.gets("lfn"));
							}
							
							indextable = newIndextable;
							tableentries = newTableentries;
							
							lastIndexTableUpdate = System.currentTimeMillis();
						}
						else{
							// in case of a DB connection failure, try again in 10 seconds, until then reuse the existing value (if any)
							lastIndexTableUpdate = System.currentTimeMillis() - CACHE_TIMEOUT + 1000*10;
						}
					}
				}
				finally {
					indextableWriteLock.unlock();
				}
				
				indextableReadLock.lock();
			}
		}
		finally{
			indextableReadLock.unlock();
		}
	}
	
	/**
	 * Get the base folder for this table name 
	 * @param hostId 
	 * 
	 * @param tableName
	 * @return entry in INDEXTABLE for this table name
	 */
	public static IndexTableEntry getIndexTable(final int hostId, final int tableName){
		updateIndexTableCache();

		if (indextable == null)
			return null;
		
		for (final IndexTableEntry ite: indextable)
			if (ite.hostIndex == hostId && ite.tableName == tableName)
				return ite;
			
		return null;
	}
	
	/**
	 * For a given path, get the closest match for LFNs from INDEXTABLE
	 * 
	 * @param pattern
	 * @return the best match, or <code>null</code> if none could be found
	 */
	public static IndexTableEntry getClosestMatch(final String pattern){
		updateIndexTableCache();
		
		if (indextable==null)
			return null;
		
		if (monitor!=null)
			monitor.incrementCounter("INDEXTABLE_lookup");
		
		int bestLen = 0;
		
		IndexTableEntry best = null;
		
		for (final IndexTableEntry ite: indextable){
			if (pattern.startsWith(ite.lfn)){
				if (ite.lfn.length() > bestLen){
					best = ite;
					bestLen = ite.lfn.length();
				}
			}
		}
		
		return best;
	}
	
	/**
	 * @param path
	 * @return <code>true</code> if this path is held in a separate table
	 */
	public static boolean isSeparateTable(final String path){
		if (path==null || path.length()==0 || !path.startsWith("/"))
			return false;
		
		updateIndexTableCache();
		
		if (!path.endsWith("/"))
			return tableentries.contains(path+"/");
		
		return tableentries.contains(path);
	}
}
