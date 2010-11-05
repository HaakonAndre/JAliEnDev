package alien.catalogue;

import java.util.logging.Logger;

import alien.config.ConfigUtils;

import lazyj.DBFunctions;

/**
 * One row from alice_users.GUIDINDEX
 * 
 * @author costing
 * @since Nov 3, 2010
 */
public class GUIDIndex {
	static transient final Logger logger = ConfigUtils.getLogger(GUIDIndex.class.getCanonicalName());
	
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
	public final int tableName;
	
	/**
	 * GUID time, in milliseconds
	 */
	public final long guidTime;
	
	/**
	 * Initialize from a DB query from alice_users.GUIDINDEX
	 * 
	 * @param db
	 * @see CatalogueUtils#getGUIDIndex(long)
	 */
	GUIDIndex(final DBFunctions db){
		indexId = db.geti("indexId");
		hostIndex = db.geti("hostIndex");
		tableName = db.geti("tableName");
		
		String sTime = db.gets("guidTime");

		if (sTime.length()>1)
			guidTime = Long.parseLong(sTime.substring(1), 16);
		else
			guidTime = 0;
	}
}