package alien.catalogue;

import java.util.UUID;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;

/**
 * @author costing
 *
 */
public final class GUIDUtils {
	
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(GUIDUtils.class.getCanonicalName());
	
	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(GUIDUtils.class.getCanonicalName());
	
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
		
		if (monitor!=null)
			monitor.incrementCounter("GUID_db_lookup");
		
		db.query("SELECT * FROM G"+tableName+"L WHERE guid=string2binary('"+guid+"');");
		
		if (db.moveNext())
			return new GUID(db, host, tableName);
		
		return null;
	}
	
}
