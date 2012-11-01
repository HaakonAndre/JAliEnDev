package alien.catalogue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;

import lazyj.DBFunctions;
import lazyj.Format;
import lazyj.StringFactory;

/**
 * Wrapper around a row in INDEXTABLE
 * 
 * @author costing
 */
public class IndexTableEntry implements Serializable, Comparable<IndexTableEntry>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2978796807690712492L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(IndexTableEntry.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(IndexTableEntry.class.getCanonicalName());
	
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
	
	private final int hashCode;
	
	/**
	 * Initialize from one entry in INDEXTABLE
	 * 
	 * @param db
	 */
	public IndexTableEntry(final DBFunctions db){
		indexId = db.geti("indexId");
		hostIndex = db.geti("hostIndex");
		tableName = db.geti("tableName");
		lfn = StringFactory.get(db.gets("lfn"));
		
		hashCode = hostIndex * 13 + tableName * 29 + indexId * 43;
	}
	
	@Override
	public String toString() {
		return "IndexTableEntry indexId: "+indexId+"\n"+
		       "hostIndex\t\t: "+hostIndex+"\n"+
		       "tableName\t\t: "+tableName+"\n"+
		       "lfn\t\t\t: "+lfn+"\n";
	}

	/**
	 * @return the database connection to this host/database
	 */
	public DBFunctions getDB(){
		final Host h = CatalogueUtils.getHost(hostIndex);
		
		if (h==null)
			return null;
		
		if (logger.isLoggable(Level.FINEST)){
			logger.log(Level.FINEST, "Host is : "+h);
		}
		
		return h.getDB();
	}
	
	/**
	 * Get the LFN from this table
	 * 
	 * @param sPath
	 * @return the LFN, or <code>null</code> if it doesn't exist
	 */
	public LFN getLFN(final String sPath){
		return getLFN(sPath, false);
	}
	
	/**
	 * Get the LFN having the indicated GUID
	 * 
	 * @param guid
	 * @return the LFN, if it exists in this table, or <code>null</code> if not
	 */
	public LFN getLFN(final UUID guid){
		final DBFunctions db = getDB();
		
		if (db==null)
			return null;
		
		if (monitor!=null){
			monitor.incrementCounter("LFN_db_lookup");
		}
		
		final String q = "SELECT * from L"+tableName+"L WHERE guid=string2binary(?);"; 
		
		if (!db.query(q, false, guid.toString())){
			return null;
		}
		
		if (!db.moveNext()){
			if (logger.isLoggable(Level.FINE))
				logger.log(Level.FINE, "Empty result set for "+q+" and "+guid);
			
			return null;
		}
		
		return new LFN(db, this);
	}
	
	/**
	 * Get the LFN from this table
	 * 
	 * @param sPath
	 * @param evenIfDoesntExist
	 * @return the LFN, either the existing entry, or if <code>evenIfDoesntExist</code> is <code>true</code>
	 *      then a bogus entry is returned
	 */
	public LFN getLFN(final String sPath, final boolean evenIfDoesntExist){
		String sSearch = sPath;
		
		if (sSearch.startsWith("/"))
			sSearch = sSearch.substring(lfn.length());
		
		final DBFunctions db = getDB();
		
		if (db==null)
			return null;
		
		if (monitor!=null){
			monitor.incrementCounter("LFN_db_lookup");
		}
		
		String q = "SELECT * FROM L"+tableName+"L WHERE lfn=?";
		
		if (!sSearch.endsWith("/")){
			q += " OR lfn=?";
			
			if (!db.query(q, false, sSearch, sSearch+"/"))
				return null;
		}
		else{
			if (!db.query(q, false, sSearch))
				return null;
		}
		
		if (!db.moveNext()){
			if (logger.isLoggable(Level.FINE))
				logger.log(Level.FINE, "Empty result set for "+q+" and "+sSearch);
			
			if (evenIfDoesntExist){
				return new LFN(sSearch, this);
			}
			
			return null;
		}
		
		return new LFN(db, this);
	}
	
	/**
	 * @param sPath base path where to start searching, must be an absolute path ending in /
	 * @param sPattern pattern to search for, in SQL wildcard format
	 * @param flags a combination of {@link LFNUtils}.FIND_* fields
	 * @return the LFNs from this table that match
	 */
	public List<LFN> find(final String sPath, final String sPattern, final int flags){
		final DBFunctions db = getDB();
		
		if (db==null)
			return null;
		
		if (monitor!=null){
			monitor.incrementCounter("LFN_find");
		}
		
		final List<LFN> ret = new ArrayList<LFN>();
		
		String sSearch = sPath;
		
		if (sSearch.startsWith("/")){
			if (lfn.length() <= sSearch.length())
				sSearch = sSearch.substring(lfn.length());
			else
				sSearch = "";
		}
		
		String q = "SELECT * FROM L"+tableName+"L WHERE ";
		
		if ( (flags & LFNUtils.FIND_REGEXP)==0 ) {
			if (!sPattern.startsWith("%"))
				sSearch += "%";
			
			sSearch += sPattern;
			
			if (!sPattern.endsWith("%"))
				sSearch += "%";
			
			q += "lfn LIKE '"+Format.escSQL(sSearch)+"' AND replicated=0";
		}
		else{
			q += "lfn RLIKE '"+Format.escSQL(sSearch+sPattern)+"' AND replicated=0";
		}

		if ( (flags & LFNUtils.FIND_INCLUDE_DIRS) == 0)
			q += " AND type!='d'";
		
		if ( (flags & LFNUtils.FIND_NO_SORT) == 0)
			q += " ORDER BY lfn";
		
		if (!db.query(q))
			return null;
		
		while (db.moveNext()){
			final LFN l = new LFN(db, this);
			
			ret.add(l);
		}
		
		return ret;
	}
	
	/**
	 * Get the LFN from this table
	 * 
	 * @param entryId
	 * @return the LFN, or <code>null</code>
	 */
	public LFN getLFN(final long entryId){
		final DBFunctions db = getDB();		
		
		if (!db.query("SELECT * FROM L"+tableName+"L WHERE entryId=?;", false, Long.valueOf(entryId)))
			return null;
		
		if (!db.moveNext())
			return null;
		
		return new LFN(db, this);
	}

	@Override
	public int compareTo(final IndexTableEntry o) {
		int diff = hostIndex - o.hostIndex;
		
		if (diff!=0)
			return diff;
		
		diff = tableName - o.tableName;
		
		if (diff!=0)
			return diff;
		
		diff = indexId - o.indexId;
		
		return diff;
	}
	
	@Override
	public boolean equals(final Object obj) {
		if (! (obj instanceof IndexTableEntry))
			return false;
		
		return compareTo((IndexTableEntry) obj)==0;
	}
	
	@Override
	public int hashCode() {
		return hashCode;
	}
}