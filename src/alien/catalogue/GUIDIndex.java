package alien.catalogue;

import lazyj.DBFunctions;

/**
 * One row from alice_users.GUIDINDEX
 * 
 * @author costing
 * @since Nov 3, 2010
 */
public class GUIDIndex {
	/**
	 * Host index ID
	 */
	public final int indexId;
	
	/**
	 * Host index
	 */
	public final int hostIndex;
	
	/**
	 * Table name
	 */
	public final String tableName;
	
	/**
	 * GUID time, in milliseconds
	 */
	public final long guidTime;
	
	/**
	 * Initialize from a DB query from alice_users.GUIDINDEX
	 * 
	 * @param db
	 * @see CatalogueUtils#getGUIDIndex(int)
	 */
	GUIDIndex(final DBFunctions db){
		indexId = db.geti("indexId");
		hostIndex = db.geti("hostIndex");
		tableName = db.gets("tableName");
		
		String sTime = db.gets("guidTime", "0");
		
		// 11DF800000000000
		if (sTime.length()>11){
			sTime = sTime.substring(0, 11);
		}
		
		guidTime = Long.parseLong(sTime, 16);
	}
}