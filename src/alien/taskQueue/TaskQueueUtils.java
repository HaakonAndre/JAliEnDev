package alien.taskQueue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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
		final DBFunctions db = getDB();
		
		if (monitor!=null){
			monitor.incrementCounter("TQ_db_lookup");
			monitor.incrementCounter("TQ_jobdetails");
		}
		
		if (!db.query("SELECT * FROM QUEUE WHERE queueId="+queueId+";"))
			return null;
		
		if (!db.moveNext()){
			return null;
		}
		
		return new Job(db);
	}
	
	/**
	 * Get the list of active masterjobs
	 * 
	 * @param account the account for which the masterjobs are needed, or <code>null</code> for all active masterjobs, of everybody
	 * @return the list of active masterjobs for this account
	 */
	public static List<Job> getMasterjobs(final String account){
		final DBFunctions db = getDB();
		
		if (monitor!=null){
			monitor.incrementCounter("TQ_db_lookup");
			monitor.incrementCounter("TQ_getmasterjobs");
		}
		
		String q = "SELECT * FROM QUEUE WHERE split=0 ";
		
		if (account!=null && account.length()>0){
			q += "AND submitHost LIKE '"+Format.escSQL(account)+"@%' ";
		}
		
		q += "ORDER BY queueId ASC;";
				
		final List<Job> ret = new ArrayList<Job>();
		
		db.query(q);

		while (db.moveNext()){
			ret.add(new Job(db));
		}
		
		return ret;
	}
	
	/**
	 * @param account
	 * @return the masterjob for this account and the subjob statistics for them
	 */
	public static Map<Job, Map<String, Integer>> getMasterjobStats(final String account){
		final List<Job> jobs = getMasterjobs(account);
		
		final Map<Job, Map<String, Integer>> ret = new TreeMap<Job, Map<String, Integer>>();
		
		if (jobs.size()==0)
			return ret;
		
		final DBFunctions db = getDB();

		final StringBuilder sb = new StringBuilder(jobs.size() * 10);
		
		final Map<Integer, Job> reverse = new HashMap<Integer, Job>(); 
		
		for (final Job j: jobs){
			if (sb.length()>0)
				sb.append(',');
			
			sb.append(j.queueId);
			
			reverse.put(Integer.valueOf(j.queueId), j);
		}

		if (monitor!=null){
			monitor.incrementCounter("TQ_db_lookup");
			monitor.incrementCounter("TQ_getmasterjob_stats");
		}

		db.query("select split,status,count(1) from QUEUE where split in ("+sb.toString()+") group by split,status;");
			
		Map<String, Integer> m = null;
		int oldJobID = -1;
			
		while (db.moveNext()){
			final int j = db.geti(1);
			
			if (j!=oldJobID){
				m = new HashMap<String, Integer>();
				ret.put(reverse.get(Integer.valueOf(j)), m);
			}
			
			// ignore the NPE warning, this cannot be null
			m.put(db.gets(2), Integer.valueOf(db.geti(3)));
		}
				
		return ret;
	}

	/**
	 * Get the subjobs of this masterjob
	 * 
	 * @param queueId
	 * @return the subjobs, if any
	 */
	public static List<Job> getSubjobs(final int queueId){
		final DBFunctions db = getDB();
		
		if (monitor!=null){
			monitor.incrementCounter("TQ_db_lookup");
			monitor.incrementCounter("TQ_getsubjobs");
		}
		
		final String q = "SELECT * FROM QUEUE WHERE split="+queueId+" ORDER BY queueId ASC;";
		
		final List<Job> ret = new ArrayList<Job>();

		db.query(q);
		
		while (db.moveNext()){
			ret.add(new Job(db));
		}
		
		return ret;		
	}
	
	/**
	 * @param jobs
	 * @return the jobs grouped by their state
	 */
	public static Map<String, List<Job>> groupByStates(final List<Job> jobs){
		if (jobs==null)
			return null;
		
		final Map<String, List<Job>> ret = new TreeMap<String, List<Job>>();
		
		if (jobs.size()==0)
			return ret;
		
		for (final Job j: jobs){
			List<Job> l = ret.get(j.status);
			
			if (l==null){
				l = new ArrayList<Job>();
				ret.put(j.status, l);
			}
			
			l.add(j);
		}
		
		return ret;
	}

	/**
	 * @param queueId
	 * @return trace log
	 */
	public static String getJobTraceLog(final int queueId){
		final JobTraceLog trace = new JobTraceLog(queueId);
		return trace.getTraceLog();
	}
	
	
}
