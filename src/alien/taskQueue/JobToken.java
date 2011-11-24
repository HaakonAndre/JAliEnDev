package alien.taskQueue;

import java.util.Random;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import lazyj.Format;
import lazyj.StringFactory;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;

/**
 * @author ron
 * @since Nov 2, 2011
 * 
 */
public class JobToken implements Comparable<JobToken> {
	
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(JobToken.class.getCanonicalName());
	
	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(JobToken.class.getCanonicalName());

	/**
	 * jobId
	 */
	public int jobId;

	/**
	 * Username
	 */
	public String username;

	/**
	 * Token
	 */
	public String token;

	/**
	 * Set to <code>true</code> if the entry existed in the database, or to <code>false</code> if not.
	 * Setting the other fields will only be permitted if this field is false.
	 */
	private boolean exists;
	
	/**
	 * Load one row from a TOKENS table
	 * 
	 * @param db
	 */
	JobToken(final DBFunctions db){
		init(db);
		
		this.exists = true;
	}

	private static final char[] tokenStreet = new char[]{
	    'X', 'Q', 't', '2', '!', '^', '9', '5', '3', '4', '5', 'o', 'r', 't', '{', ')', '}', '[',
	    ']', 'h', '9', '|', 'm', 'n', 'b', 'v', 'c', 'x', 'z', 'a', 's', 'd', 'f', 'g', 'h', 'j',
	    'k', 'l', ':', 'p', 'o', 'i', 'u', 'y', 'Q', 'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P',
	    'A', 'S', 'D', 'F', 'G', 'H', 'J', 'Z', 'X', 'C', 'V', 'B', 'N', 'M'
	};
	
	
	/**
	 * Create a new JobToken object
	 * @param jobId 
	 * @param username 
	 */
	JobToken(final int jobId, final String username){
		this.jobId = jobId;
		
		this.username = username;
		
		this.exists = false;
	}

	/**
	 * Create a 32 chars long token (job token)
	 * @param db 
	 */
	public void spawnToken(final DBFunctions db) {
		final char[] tok = new char[32];

		final Random ran = new Random(System.nanoTime());

		for (int i = 0; i < 32; i++)
			tok[i] = tokenStreet[ran.nextInt(tokenStreet.length)];

		this.token = new String(tok);

//		System.out.println("token generated: " + token);

		update(db);
	}
	
	/**
	 * The special value for when the job is in INSERTING and then a real value will be assigned by AliEn
	 * 
	 * @param db
	 */
	public void emptyToken(final DBFunctions db){
		this.token = "-1";
		
		update(db);
	}

	private void init(final DBFunctions db){
		this.jobId = db.geti("jobId");
		
		this.username = StringFactory.get(db.gets("userName"));
		
		this.token = db.gets("jobToken");
		
		this.exists = true;
	}
	
	private boolean insert(final DBFunctions db){
		String q = "INSERT INTO jobToken ( jobId, userName, jobToken)  VALUES ("+ jobId+",'"+ Format.escSQL(username) +"','"+ Format.escSQL(token) +"');";
			
		if (db.query(q)){
			if (monitor != null)
				monitor.incrementCounter("jobToken_db_insert");
			
			exists = true;
					
			return true;
		}
			
		return false;
	}

	/**
	 *  update the entry in the database, inserting it if necessary
	 * 
	 * @param db 
	 * @return <code>true</code> if successful
	 */
	boolean update(final DBFunctions db){
		if (db == null){
			return false;
		}
			
		if (!exists){
//			System.out.println("inserting...");
			final boolean insertOK = insert(db);
			return insertOK;
		}

		String q = "UPDATE jobToken SET jobToken='"+Format.escSQL(token)+"' WHERE jobId="+jobId;
		
//		System.out.println("SQL "+q);

		// only the token list can change
		if (!db.query(q)){
			// wrong table name or what?
			return false;
		}
		
		if (db.getUpdateCount()==0){
			// the entry did not exist in fact, what's going on?
			return false;
		}

		if (monitor != null)
			monitor.incrementCounter("jobToken_db_update");
		
		return true;
	}
	
	@Override
	public String toString() {
		return "jobId\t\t: "+jobId+"\n"+
			   "username\t\t: "+username+"\n"+
		       "token\t\t: "+token+"\n";
	}
	
	@Override
	public int compareTo(final JobToken o) {
		int diff = jobId-o.jobId;
		
		if (diff!=0)
			return diff;
		
		return token.compareTo(o.token);
	}
	
	@Override
	public boolean equals(final Object obj) {
		if (! (obj instanceof JobToken))
			return false;
		
		return compareTo((JobToken) obj) == 0;
	}
	
	@Override
	public int hashCode() {
		return jobId;
	}
	
	/**
	 * @return <code>true</code> if the guid was taken from the database, <code>false</code> if it is a newly generated one
	 */
	public boolean exists(){
		return exists;
	}
	
	/**
	 * Delete a jobToken in the DB
	 * 
	 * @param db
	 * @return success of the deletion
	 */
	boolean destroy(final DBFunctions db){
		String q = "DELETE FROM jobToken where  jobId = "+ jobId +" and userName = '"+ Format.escSQL(username)+ 
				   "' and jobToken = '"+ Format.escSQL(token) +"';";
		
		if (db.query(q)){
			if (monitor != null)
				monitor.incrementCounter("jobToken_db_delete");
			
			exists = false;
			return true;
		}
		
		return false;
	}
}
