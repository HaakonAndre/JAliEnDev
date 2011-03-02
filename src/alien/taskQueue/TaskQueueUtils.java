package alien.taskQueue;

import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import lazyj.Format;
import lazyj.cache.GenericLastValuesCache;
import alien.catalogue.CatalogueUtils;
import alien.catalogue.Host;
import alien.catalogue.LFN;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;

public class TaskQueueUtils {
	/**
	 * @author steffen
	 * @since Mar 1, 2011
	 */

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(TaskQueueUtils.class.getCanonicalName());
	
	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(TaskQueueUtils.class.getCanonicalName());
	
	/**
	 * @return the database connection to this host/database
	 */
	public static DBFunctions getDB(){
		final DBFunctions db = ConfigUtils.getDB("processes");
		
		return db;
	}

	/**
	 * Get the Job from the QUEUE
	 * 
	 * @param sPath
	 * @param evenIfDoesntExist
	 * @return the LFN, either the existing entry, or if <code>evenIfDoesntExist</code> is <code>true</code>
	 *      then a bogus entry is returned
	 */
	public static Job getJob(final int queueId){
		String sSearch = String.valueOf(queueId);

		final DBFunctions db = getDB();
		
//		if (monitor!=null){
//			monitor.incrementCounter("LFN_db_lookup");
//		}
		
		if (!db.query("SELECT * FROM QUEUE WHERE queueId='"+Format.escSQL(sSearch)+"';"))
			return null;
		
		if (!db.moveNext()){		
			return null;
		}
		
		return new Job(db,0);
	}
	

	/**
	 * For how long the caches are active
	 */
	public static final long CACHE_TIMEOUT = 1000 * 60 * 5;
	
}
