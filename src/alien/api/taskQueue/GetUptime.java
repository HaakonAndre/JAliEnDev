package alien.api.taskQueue;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

import lazyj.DBFunctions;
import alien.api.Cacheable;
import alien.api.Request;
import alien.taskQueue.TaskQueueUtils;

/**
 * Get the "uptime" or "w" statistics
 * 
 * @author costing
 * @since Nov 4, 2011
 */
public class GetUptime extends Request implements Cacheable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 766147136470300455L;

	/**
	 * Statistics for one user
	 * 
	 * @author costing
	 */
	public static final class UserStats implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 7614077240613964647L;

		/**
		 * Currently active jobs for this user
		 */
		public int runningJobs = 0;
		
		/**
		 * Currently waiting jobs for this user
		 */
		public int waitingJobs = 0;
		
		/**
		 * Sum up the values (for computing totals)
		 * 
		 * @param other
		 */
		public void add(final UserStats other){
			this.runningJobs += other.runningJobs;
			this.waitingJobs += other.waitingJobs;
		}
	}
	
	private Map<String, UserStats> stats = null;
	
	/**
	 */
	public GetUptime(){
		// nothing
	}
	
	@Override
	public void run() {
		final DBFunctions db = TaskQueueUtils.getQueueDB();
		
		stats = new TreeMap<String, UserStats>();
		
		db.query("select substring_index(submithost,'@',1),count(1) from QUEUE where status in ('ASSIGNED', 'STARTED', 'RUNNING', 'SAVING') group by 1 order by 1;");
		
		while (db.moveNext()){
			final UserStats u = new UserStats();
			u.runningJobs = db.geti(2);
			
			stats.put(db.gets(1), u);
		}
		
		db.query("select substring_index(submithost,'@',1),count(1) from QUEUE where status in ('INSERTING', 'WAITING') group by 1 order by 1;");
		
		while (db.moveNext()){
			final String user = db.gets(1);
			
			UserStats u = stats.get(user);
			
			if (u==null){
				u = new UserStats();
				stats.put(user, u);
			}
			
			u.waitingJobs = db.geti(2);
		}
	}
	
	/**
	 * @return a JDL
	 */
	public Map<String, UserStats> getStats(){
		return this.stats;
	}
	
	@Override
	public String toString() {
		return "Asked for uptime, answer is: "+this.stats;
	}

	@Override
	public String getKey() {
		return "uptime";
	}

	@Override
	public long getTimeout() {
		return 1000*60*1;
	}
}
