package alien.catalogue;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import lazyj.DBFunctions;

/**
 * @author costing
 *
 */
public final class GUIDUtils {
	
	/**
	 * Get the host where this entry should be located
	 * 
	 * @param guid
	 * @return host id
	 * @see Host
	 */
	public static int getGUIDHost(final UUID guid){
		final long guidTime = guid.timestamp();
		
		final GUIDIndex index = CatalogueUtils.getGUIDIndex(guidTime);
		
		if (index==null)
			return -1;
		
		return index.hostIndex;
	}
	
	/**
	 * Get the DB connection that applies for a particular GUID
	 * 
	 * @param guid
	 * @return the DB connection, or <code>null</code> if something is not right
	 * @see #getTableNameForGUID(UUID)
	 */
	public static DBFunctions getDBForGUID(final UUID guid){
		final int host = getGUIDHost(guid);
		
		if (host<0)
			return null;
		
		final Host h = CatalogueUtils.getHost(host);
		
		if (h==null)
			return null;
		
		return h.getDB();
	}

	/**
	 * Get the tablename where this GUID should be located (if any)
	 * 
	 * @param guid
	 * @return table name, or <code>null</code> if any problem
	 * @see #getDBForGUID(UUID)
	 */
	public static int getTableNameForGUID(final UUID guid){
		final long guidTime = guid.timestamp();
		
		final GUIDIndex index = CatalogueUtils.getGUIDIndex(guidTime);
		
		if (index==null)
			return -1;

		return index.tableName;
	}
	
	/**
	 * Get the GUID catalogue entry when the uuid is known
	 * 
	 * @param guid
	 * @return the GUID, or <code>null</code> if it cannot be located
	 */
	public static GUID getGUID(final UUID guid){
		final int host = getGUIDHost(guid);
		
		if (host<0)
			return null;
		
		final Host h = CatalogueUtils.getHost(host);
		
		if (h==null)
			return null;
		
		final DBFunctions db = h.getDB();
		
		if (db==null)
			return null;
		
		final int tableName = GUIDUtils.getTableNameForGUID(guid);
		
		if (tableName < 0)
			return null;
		
		db.query("SELECT * FROM G"+tableName+"L WHERE guid=string2binary('"+guid+"');");
		
		if (db.moveNext())
			return new GUID(db, host, tableName);
		
		return null;
	}

	/**
	 * Get all LFNs associated with this GUIDs
	 * 
	 * @param guid
	 * @return LFNs
	 */
	public static Set<LFN> getLFNsForGUID(final GUID guid){
		final DBFunctions db = getDBForGUID(guid.guid);
		
		if (db==null)
			return null;
		
		final int tablename = getTableNameForGUID(guid.guid);
		
		db.query("SELECT distinct lfnRef FROM G"+tablename+"L_REF WHERE guidId="+guid.guidId);
		
		if (!db.moveNext())
			return null;
		
		final String sLFNRef = db.gets(1);
		
		final int idx = sLFNRef.indexOf('_');
		
		final int iHostID = Integer.parseInt(sLFNRef.substring(0, idx));
		
		final int iLFNTableIndex = Integer.parseInt(sLFNRef.substring(idx+1));
		
		final DBFunctions db2 = CatalogueUtils.getHost(iHostID).getDB();
		
		db2.query("SELECT * FROM L"+iLFNTableIndex+"L WHERE guid=string2binary('"+guid.guid+"');");
	
		final Set<LFN> ret = new LinkedHashSet<LFN>();
		
		while (db2.moveNext()){
			ret.add(new LFN(db2, CatalogueUtils.getIndexTable(iHostID, iLFNTableIndex)));
		}
		
		return ret;
	}
	
}
