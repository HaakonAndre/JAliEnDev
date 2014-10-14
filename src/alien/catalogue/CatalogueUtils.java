/**
 * 
 */
package alien.catalogue;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import lazyj.DBFunctions;
import lazyj.Format;
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
		protected boolean cacheNulls() {
			return false;
		}

		@Override
		protected Host resolve(final Integer key) {
			final DBFunctions db = ConfigUtils.getDB("alice_users");

			if (db!=null){			
				try {
					if (!db.query("SELECT * FROM HOSTS WHERE hostIndex=?;", false, key))
						return null;
	
					if (db.moveNext())
						return new Host(db);
				} finally {
					db.close();
				}
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
	public static Host getHost(final int idx) {
		return hostsCache.get(Integer.valueOf(idx <= 0 ? 1 : idx));
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

		try {
			if (System.currentTimeMillis() - guidIndexCacheUpdated > CACHE_TIMEOUT || guidIndexCache == null) {
				guidIndexReadLock.unlock();

				guidIndexWriteLock.lock();

				try {
					if (System.currentTimeMillis() - guidIndexCacheUpdated > CACHE_TIMEOUT || guidIndexCache == null) {
						if (logger.isLoggable(Level.FINER))
							logger.log(Level.FINER, "Updating GUIDINDEX cache");

						final DBFunctions db = ConfigUtils.getDB("alice_users");

						if (db != null && db.query("SELECT * FROM GUIDINDEX ORDER BY guidTime ASC;")) {
							final LinkedList<GUIDIndex> ret = new LinkedList<>();

							try {
								while (db.moveNext())
									ret.add(new GUIDIndex(db));
							} finally {
								db.close();
							}

							guidIndexCache = ret;

							guidIndexCacheUpdated = System.currentTimeMillis();
						} else
							// in case of a DB connection failure, try again in
							// 10 seconds, until then reuse the existing value
							// (if any)
							guidIndexCacheUpdated = System.currentTimeMillis() - CACHE_TIMEOUT + 1000 * 10;
					}
				} finally {
					guidIndexWriteLock.unlock();
					guidIndexReadLock.lock();
				}
			}
		} finally {
			guidIndexReadLock.unlock();
		}
	}

	/**
	 * Get the GUIDINDEX entry that contains this timestamp (in milliseconds)
	 * 
	 * @param timestamp
	 * @return the GUIDIndex that contains this timestamp (in milliseconds)
	 */
	public static GUIDIndex getGUIDIndex(final long timestamp) {
		updateGuidIndexCache();

		if (guidIndexCache == null)
			return null;

		GUIDIndex old = null;

		for (final GUIDIndex idx : guidIndexCache) {
			if (idx.guidTime > timestamp)
				return old;

			old = idx;
		}

		return old;
	}

	/**
	 * Get all GUIDINDEX rows
	 * 
	 * @return all GUIDINDEX rows
	 */
	public static List<GUIDIndex> getAllGUIDIndexes() {
		updateGuidIndexCache();

		if (guidIndexCache == null)
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

		try {
			if (System.currentTimeMillis() - lastIndexTableUpdate > CACHE_TIMEOUT || indextable == null) {
				indextableReadLock.unlock();

				indextableWriteLock.lock();

				try {
					if (System.currentTimeMillis() - lastIndexTableUpdate > CACHE_TIMEOUT || indextable == null) {
						if (logger.isLoggable(Level.FINER))
							logger.log(Level.FINER, "Updating INDEXTABLE cache");

						final DBFunctions db = ConfigUtils.getDB("alice_users");

						if (db != null && db.query("SELECT * FROM INDEXTABLE;")) {
							final Set<IndexTableEntry> newIndextable = new HashSet<>();
							final Set<String> newTableentries = new HashSet<>();

							try {
								while (db.moveNext()) {
									final IndexTableEntry entry = new IndexTableEntry(db);

									newIndextable.add(entry);

									newTableentries.add(db.gets("lfn"));
								}
							} finally {
								db.close();
							}

							indextable = newIndextable;
							tableentries = newTableentries;

							lastIndexTableUpdate = System.currentTimeMillis();
						} else
							// in case of a DB connection failure, try again in
							// 10 seconds, until then reuse the existing value
							// (if any)
							lastIndexTableUpdate = System.currentTimeMillis() - CACHE_TIMEOUT + 1000 * 10;
					}
				} finally {
					indextableWriteLock.unlock();
				}

				indextableReadLock.lock();
			}
		} finally {
			indextableReadLock.unlock();
		}
	}

	/**
	 * Get the base folder for this table name
	 * 
	 * @param hostId
	 * 
	 * @param tableName
	 * @return entry in INDEXTABLE for this table name
	 */
	public static IndexTableEntry getIndexTable(final int hostId, final int tableName) {
		updateIndexTableCache();

		if (indextable == null)
			return null;

		for (final IndexTableEntry ite : indextable)
			if (ite.hostIndex == hostId && ite.tableName == tableName)
				return ite;

		return null;
	}

	/**
	 * @return all known L%L tables
	 */
	public static Set<IndexTableEntry> getAllIndexTables() {
		updateIndexTableCache();

		if (indextable == null)
			return null;

		return Collections.unmodifiableSet(indextable);
	}

	/**
	 * For a given path, get the closest match for LFNs from INDEXTABLE
	 * 
	 * @param pattern
	 * @return the best match, or <code>null</code> if none could be found
	 */
	public static IndexTableEntry getClosestMatch(final String pattern) {
		updateIndexTableCache();

		if (indextable == null)
			return null;

		if (monitor != null)
			monitor.incrementCounter("INDEXTABLE_lookup");

		int bestLen = 0;

		IndexTableEntry best = null;

		for (final IndexTableEntry ite : indextable)
			if (pattern.startsWith(ite.lfn))
				if (ite.lfn.length() > bestLen) {
					best = ite;
					bestLen = ite.lfn.length();
				}

		return best;
	}

	/**
	 * @param pattern
	 * @return all tables that belong to this tree
	 */
	public static Set<IndexTableEntry> getAllMatchingTables(final String pattern) {
		final IndexTableEntry best = getClosestMatch(pattern);

		if (best == null)
			return Collections.emptySet();

		final Set<IndexTableEntry> ret = new LinkedHashSet<>();

		ret.add(best);

		for (final IndexTableEntry ite : indextable)
			if (ite.lfn.startsWith(pattern))
				ret.add(ite);

		return ret;
	}

	/**
	 * @param pattern
	 * @return the Java pattern
	 */
	public static Pattern dbToJavaPattern(final String pattern) {
		String p = Format.replace(pattern, "*", "%");
		p = Format.replace(p, "%%", "%");
		p = Format.replace(p, ".", "\\.");
		p = Format.replace(p, "_", ".");
		p = Format.replace(p, "%", ".*");

		return Pattern.compile(p);
	}

	/**
	 * @param path
	 * @return <code>true</code> if this path is held in a separate table
	 */
	public static boolean isSeparateTable(final String path) {
		if (path == null || path.length() == 0 || !path.startsWith("/"))
			return false;

		updateIndexTableCache();

		if (!path.endsWith("/"))
			return tableentries.contains(path + "/");

		return tableentries.contains(path);
	}
	
	/**
	 * Create a local file with the list of GUIDs that have no LFNs pointing to them any more
	 * 
	 * @throws IOException if the indicated local file cannot be created
	 */
	public static void guidCleanup(final String ouputFile) throws IOException {
		final PrintWriter pw = new PrintWriter(new FileWriter(ouputFile));
		
		final HashMap<UUID, Long> guids = new HashMap<>(1100000000);
		
		final long started = System.currentTimeMillis();
		
		int cnt = 0;
		
		final List<GUIDIndex> guidTables = new ArrayList<>(CatalogueUtils.getAllGUIDIndexes());
		
		Collections.sort(guidTables);
		Collections.reverse(guidTables);
		
		int invalid = 0;
		
		long totalSize = 0;
		
		final long LIMIT = 1000000;
		
		for (final GUIDIndex idx : guidTables){
			cnt++;
			
			System.err.println("Reached G"+idx.tableName+"L ("+cnt+" / "+guidTables.size()+")");
			
			final Host h = CatalogueUtils.getHost(idx.hostIndex);
			
			final DBFunctions gdb = h.getDB();

			gdb.query("set wait_timeout=31536000;");
			
			int read;
			
			long offset = 0;
			
			do{
				read = 0;
				
				final String q = "select guid,size from G"+idx.tableName+"L LIMIT "+LIMIT+" OFFSET "+offset+";";
				
				while (!gdb.query(q))
					System.err.println("Retrying query "+q);
				
				while (gdb.moveNext()){
					read++;
					
					try{
						final byte[] data = gdb.getBytes(1);
						
						if (data!=null && data.length==16){
							final UUID uuid = GUID.getUUID(data);
							
							if (uuid!=null){
								guids.put(uuid, Long.valueOf(gdb.getl(2)));
								totalSize += gdb.getl(2);
							}
							else
								invalid++;
						}
						else{
							invalid++;
						}
					}
					catch (final Exception e){
					    invalid++;
					}
					
					if (guids.size()%1000000==0){
						System.err.println("Reached "+guids.size()+" in G"+idx.tableName+"L");
						System.err.println(Format.toInterval(System.currentTimeMillis() - started)+" : free "+Format.size(Runtime.getRuntime().freeMemory())+" / total "+Format.size(Runtime.getRuntime().totalMemory()));
					}
				}
				
				offset += read;
			}
			while (read==LIMIT);
			
			if (guids.size()>1000000000){
				System.err.println("Intermediate cleanup @ "+guids.size());
				
				if (!lfnCleanup(guids, true))
					System.err.println("Intermediate cleanup was not completely successful");
				
				System.err.println("Intermediate cleanup result: "+guids.size());
			}
		}

		System.err.println("Final parsing starting with "+guids.size()+" UUIDs in memory, "+invalid+" rows had invalid GUID representation, total size: "+Format.size(totalSize));
		System.err.println(Format.toInterval(System.currentTimeMillis() - started)+" : free "+Format.size(Runtime.getRuntime().freeMemory())+" / total "+Format.size(Runtime.getRuntime().totalMemory()));
		
		if (!lfnCleanup(guids, false)){
			System.err.println("Final iteration could not load all content from LFN tables, bailing out");
			pw.close();
			return;
		}
		
		System.err.println("Finally we are left with "+guids.size()+" orphan UUIDs");
		
		long totalToReclaim = 0;
		
		for (final Map.Entry<UUID, Long> uuid: guids.entrySet()){
			pw.println(uuid.getKey()+" "+uuid.getValue());
			
			totalToReclaim += uuid.getValue().longValue();
		}
		
		System.err.println("sum(GUID size) = "+totalToReclaim);
		
		pw.close();
	}

	private static boolean lfnCleanup(final Map<UUID, Long> guids, final boolean intermediate){
		final Set<IndexTableEntry> indextableCollection = CatalogueUtils.getAllIndexTables();
		
		int cnt=0;
		
		final int LIMIT = 1000000;
		
		boolean ret = true;
		
		for (final IndexTableEntry ite: indextableCollection){
			cnt++;
			
			System.err.println("Checking the content of L"+ite.tableName+"L from "+ite.hostIndex+" ("+cnt+"/"+indextableCollection.size()+"), "+guids.size()+" UUIDs left");
			
			final DBFunctions db = ite.getDB();
			
			db.query("set wait_timeout=31536000;");
			
			long read = 0;
			
			long offset = 0;
			
			do{
				read = 0;
				
				final String q = "SELECT guid FROM L"+ite.tableName+"L where guid is not null LIMIT "+LIMIT+" OFFSET "+offset+";";
				
				while (!db.query(q)){
					System.err.println("Retrying query");
				}
				
				while (db.moveNext()){
					read++;
					try{
						final byte[] data = db.getBytes(1);
						
						if (data!=null && data.length==16){
							final UUID uuid = GUID.getUUID(data);
							
							if (uuid!=null)
								guids.remove(uuid);
						}
					}
					catch (final Exception e){
					    // ignore
					}
				}
				
				offset += read;
			}
			while (read == LIMIT);
		}
		
		return ret;
	}
}
