package alien.taskQueue;

import java.util.logging.Logger;

import lazyj.DBFunctions;
import lazyj.Format;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;

/**
 * @author steffen
 * @since Mar 1, 2011
 */
public class TaskQueueUtils {

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
	 * @param queueId 
	 * @return the job, or <code>null</code> if it cannot be located 
	 */
	public static Job getJob(final int queueId){
		String sSearch = String.valueOf(queueId);

		final DBFunctions db = getDB();
		
		if (monitor!=null){
			monitor.incrementCounter("TQ_db_lookup");
		}
		
		if (!db.query("SELECT * FROM QUEUE WHERE queueId='"+Format.escSQL(sSearch)+"';"))
			return null;
		
		if (!db.moveNext()){
			return null;
		}
		
		return new Job(db);
	}
	

	/**
	 * For how long the caches are active
	 */
	public static final long CACHE_TIMEOUT = 1000 * 60 * 5;
	
}
