package alien.catalogue;

import lazyj.DBFunctions;
import lazyj.Format;

/**
 * Wrapper around a row in INDEXTABLE
 * 
 * @author costing
 */
public class IndexTableEntry {
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
	
	/**
	 * @return the database connection to this host/database
	 */
	public DBFunctions getDB(){
		final Host h = CatalogueUtils.getHost(hostIndex);
		
		if (h==null)
			return null;
		
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
		
		if (!db.query("SELECT * FROM L"+tableName+"L WHERE lfn='"+Format.escSQL(sSearch)+"';"))
			return null;
		
		if (!db.moveNext()){
			if (evenIfDoesntExist){
				return new LFN(sSearch, hostIndex, tableName);
			}
			
			return null;
		}
		
		return new LFN(db, hostIndex, tableName);
	}
	
	/**
	 * Get the LFN from this table
	 * 
	 * @param entryId
	 * @return the LFN, or <code>null</code>
	 */
	public LFN getLFN(final long entryId){
		final DBFunctions db = getDB();
		
		System.err.println("getting for entryid = "+ entryId);
		
		if (!db.query("SELECT * FROM L"+tableName+"L WHERE entryId="+entryId+";"))
			return null;
		
		if (!db.moveNext())
			return null;
		
		return new LFN(db, hostIndex, tableName);
	}
}