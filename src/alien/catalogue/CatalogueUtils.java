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
	
	private static final ReentrantReadWriteLock guidIndexRWLock = new ReentrantReadWriteLock();
	private static final ReadLock guidIndexReadLock = guidIndexRWLock.readLock();
	private static final WriteLock guidIndexWriteLock = guidIndexRWLock.writeLock();
	
	private static final void updateGuidIndexCache() {
		guidIndexReadLock.lock();

		try{
			if (System.currentTimeMillis() - guidIndexCacheUpdated > 1000 * 60 * 5 || guidIndexCache == null) {
				guidIndexReadLock.unlock();
				
				guidIndexWriteLock.lock();
				
				try{
					if (System.currentTimeMillis() - guidIndexCacheUpdated > 1000 * 60 * 5 || guidIndexCache == null) {
						if (logger.isLoggable(Level.FINER)) {
							logger.log(Level.FINER, "Updating GUIDINDEX cache");
						}
	
						final DBFunctions db = ConfigUtils.getDB("alice_users");
	
						db.query("SELECT * FROM GUIDINDEX ORDER BY guidTime ASC;");
	
						final LinkedList<GUIDIndex> ret = new LinkedList<GUIDIndex>();
	
						while (db.moveNext())
							ret.add(new GUIDIndex(db));
	
						guidIndexCache = ret;
	
						guidIndexCacheUpdated = System.currentTimeMillis();
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
	
	private static Set<IndexTableEntry> indextable = null;
	private static long lastIndexTableUpdate = 0;
	
	private static final ReentrantReadWriteLock indextableRWLock = new ReentrantReadWriteLock();
	private static final ReadLock indextableReadLock = indextableRWLock.readLock();
	private static final WriteLock indextableWriteLock = indextableRWLock.writeLock();

	private static void updateIndexTableCache() {
		indextableReadLock.lock();

		try{
			if (System.currentTimeMillis() - lastIndexTableUpdate > 1000 * 60 * 5 || indextable == null) {
				indextableReadLock.unlock();
					
				indextableWriteLock.lock();
					
				try{
					if (System.currentTimeMillis() - lastIndexTableUpdate > 1000 * 60 * 5 || indextable == null) {
						if (logger.isLoggable(Level.FINER)) {
							logger.log(Level.FINER, "Updating INDEXTABLE cache");
						}
	
						final Set<IndexTableEntry> newIndextable = new HashSet<IndexTableEntry>();
	
						final DBFunctions db = ConfigUtils.getDB("alice_users");
	
						db.query("SELECT * FROM INDEXTABLE;");
	
						while (db.moveNext()) {
							final IndexTableEntry entry = new IndexTableEntry(db);
	
							newIndextable.add(entry);
						}
						indextable = newIndextable;
						lastIndexTableUpdate = System.currentTimeMillis();
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
}
