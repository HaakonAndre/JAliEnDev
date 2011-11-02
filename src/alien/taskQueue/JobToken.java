package alien.taskQueue;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import lazyj.Format;
import lazyj.StringFactory;
import alien.catalogue.CatalogueUtils;
import alien.catalogue.Host;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.se.SE;

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
	 * @param host 
	 * @param tableName 
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
	 * Create a 32 chars long token (job token)
	 * @param token value
	 */
	JobToken(final int jobId, final String username){

			  String token = "";
			  for (int i = 0 ; i < 32 ; i++)
				  token += tokenStreet[(new Random(System.currentTimeMillis())).nextInt() % tokenStreet.length];
		      this.token = token;
		
		this.jobId = jobId;
		this.username = username;
		
		this.exists = false;

	}

	private void init(final DBFunctions db){
		jobId = db.geti("jobId");
		
		username = StringFactory.get(db.gets("userName"));
		
		token = StringFactory.get(db.gets("jobToken"));
		
	}
	
	private boolean insert(final DBFunctions db){

		
		String q = "INSERT INTO jobToken ( jobId, userName, jobToken)  VALUES ('"+
					jobId+"','"+ username +"','"+ token +"');";
	
		
		if (db.query(q)){
			if (monitor != null)
				monitor.incrementCounter("jobToken_db_insert");
			
			exists = true;
			
			db.query("SELECT jobId FROM jobToken WHERE Token='"+token+"');");
			
			if (!db.moveNext()){
				// that would be weird, we have just inserted it. but double checking cannot hurt
				return false;
			}
			
			jobId = db.geti(1);
			return true;
		}
		
		return false;
	}
	

	/**
	 * @return update the entry in the database, inserting it if necessary
	 */
	boolean update(DBFunctions db){
		

		if (db == null){
			return false;
		}
			
		if (!exists){
			final boolean insertOK = insert(db);
			
			return insertOK;
		}
		
		// only the token list can change
		if (!db.query("UPDATE jobToken SET jobToken='"+token+"' WHERE jobId="+jobId)){
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
		return token.hashCode();
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
	public boolean destroy(final DBFunctions db){

		
		String q = "DELETE FROM jobToken where  jobId = "+ jobId +" and userName = '"+ username 
				+"' and jobToken = '"+ token +"';";
	
		
		if (db.query(q)){
			if (monitor != null)
				monitor.incrementCounter("jobToken_db_delete");
			exists = false;
			return true;
		}
		return false;
	}
	
	
	
	
}
