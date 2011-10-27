package alien.taskQueue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lazyj.DBFunctions;
import lazyj.Format;
import alien.catalogue.LFN;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.user.AliEnPrincipal;
import alien.user.LDAPHelper;

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
		return getJob(queueId, false);
	}
	
	private static final String ALL_BUT_JDL="queueId, priority, execHost, sent, split, name, spyurl, commandArg, finished, masterjob, status, splitting, node, error, current, received, validate, command, merging, submitHost, path, site, started, expires, finalPrice, effectivePriority, price, si2k, jobagentId, agentid, notify, chargeStatus, optimized, mtime";
	
	/**
	 * Get the Job from the QUEUE
	 * 
	 * @param queueId 
	 * @param loadJDL 
	 * @return the job, or <code>null</code> if it cannot be located 
	 */
	public static Job getJob(final int queueId, final boolean loadJDL){
		final DBFunctions db = getDB();
		
		if (monitor!=null){
			monitor.incrementCounter("TQ_db_lookup");
			monitor.incrementCounter("TQ_jobdetails");
		}
		
		final long lQueryStart = System.currentTimeMillis();

		final String q = "SELECT "+(loadJDL ? "*" : ALL_BUT_JDL)+" FROM QUEUE WHERE queueId="+queueId;
	
		if (!db.query(q))
			return null;
		
		monitor.addMeasurement("TQ_jobdetails_time", (System.currentTimeMillis() - lQueryStart)/1000d);
		
		if (!db.moveNext()){
			return null;
		}
		
		return new Job(db, loadJDL);
	}
	
	/**
	 * Get the list of active masterjobs
	 * 
	 * @param account the account for which the masterjobs are needed, or <code>null</code> for all active masterjobs, of everybody
	 * @return the list of active masterjobs for this account
	 */
	public static List<Job> getMasterjobs(final String account){
		return getMasterjobs(account, false);
	}	
	
	/**
	 * Get the list of active masterjobs
	 * 
	 * @param account the account for which the masterjobs are needed, or <code>null</code> for all active masterjobs, of everybody
	 * @param loadJDL 
	 * @return the list of active masterjobs for this account
	 */
	public static List<Job> getMasterjobs(final String account, final boolean loadJDL){
		final DBFunctions db = getDB();
		
		if (monitor!=null){
			monitor.incrementCounter("TQ_db_lookup");
			monitor.incrementCounter("TQ_getmasterjobs");
		}
		
		String q = "SELECT "+(loadJDL ? "*" : ALL_BUT_JDL)+" FROM QUEUE WHERE split=0 ";
		
		if (account!=null && account.length()>0){
			q += "AND submitHost LIKE '"+Format.escSQL(account)+"@%' ";
		}
		
		q += "ORDER BY queueId ASC;";
		
		final List<Job> ret = new ArrayList<Job>();
		
		final long lQueryStart = System.currentTimeMillis();
		
		db.query(q);
		
		if (monitor!=null)
			monitor.addMeasurement("TQ_getmasterjobs_time", (System.currentTimeMillis() - lQueryStart)/1000d);

		while (db.moveNext()){
			ret.add(new Job(db, loadJDL));
		}
		
		return ret;
	}
	
	/**
	 * @param initial
	 * @param maxAge the age in milliseconds
	 * @return the jobs that are active or have finished since at most maxAge
	 */
	public static List<Job> filterMasterjobs(final List<Job> initial, final long maxAge){
		if (initial==null)
			return null;
		
		final List<Job> ret = new ArrayList<Job>(initial.size());
		
		final long now = System.currentTimeMillis();
		
		for (final Job j: initial){
			if (j.isActive() ||  j.mtime==null || (now - j.mtime.getTime() < maxAge))
				ret.add(j);
		}
		
		return ret;
	}
	
	/**
	 * @param account
	 * @return the masterjobs for this account and the subjob statistics for them
	 */
	public static Map<Job, Map<String, Integer>> getMasterjobStats(final String account){
		return getMasterjobStats(getMasterjobs(account));
	}
	
	/**
	 * @param jobs
	 * @return the same masterjobs and the respective subjob statistics
	 */
	public static Map<Job, Map<String, Integer>> getMasterjobStats(final List<Job> jobs){
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
		
		final long lQueryStart = System.currentTimeMillis();

		db.query("select split,status,count(1) from QUEUE where split in ("+sb.toString()+") group by split,status order by 1,2;");
		
		if (monitor!=null)
			monitor.addMeasurement("TQ_getmasterjob_stats_time", (System.currentTimeMillis() - lQueryStart)/1000d);
			
		Map<String, Integer> m = null;
		int oldJobID = -1;
			
		while (db.moveNext()){
			final int j = db.geti(1);
			
			if (j!=oldJobID){
				m = new HashMap<String, Integer>();

				final Integer jobId = Integer.valueOf(j);
				ret.put(reverse.get(jobId), m);
				reverse.remove(jobId);
				
				oldJobID = j;
			}
			
			// ignore the NPE warning, this cannot be null
			m.put(db.gets(2), Integer.valueOf(db.geti(3)));
		}
		
		// now, what is left, something that doesn't have subjobs ?
		for (final Job j: reverse.values()){
			m = new HashMap<String, Integer>(1);
			m.put(j.status, Integer.valueOf(1));
			ret.put(j, m);
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
		return getSubjobs(queueId, false);
	}
		
	/**
	 * Get the subjobs of this masterjob
	 * 
	 * @param queueId
	 * @param loadJDL 
	 * @return the subjobs, if any
	 */
	public static List<Job> getSubjobs(final int queueId, final boolean loadJDL){
		final DBFunctions db = getDB();
		
		if (db==null)
			return null;
		
		if (monitor!=null){
			monitor.incrementCounter("TQ_db_lookup");
			monitor.incrementCounter("TQ_getsubjobs");
		}
		
		final String q = "SELECT "+(loadJDL ? "*" : ALL_BUT_JDL)+" FROM QUEUE WHERE split="+queueId+" ORDER BY queueId ASC;";
		
		final List<Job> ret = new ArrayList<Job>();
		
		final long lQueryStart = System.currentTimeMillis();

		db.query(q);
		
		if (monitor!=null)
			monitor.addMeasurement("TQ_getsubjobs_time", (System.currentTimeMillis() - lQueryStart)/1000d);
		
		while (db.moveNext()){
			ret.add(new Job(db, loadJDL));
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
	
	/**
	 * @param job
	 * @param newStatus
	 */
	public static void setJobStatus(final int job, final String newStatus){
		final DBFunctions db = getDB();
		
		if (db==null)
			return;
		
		db.query("UPDATE QUEUE SET status='"+Format.escSQL(newStatus)+"' WHERE queueId="+job+" AND status!='"+Format.escSQL(newStatus)+"'");
	}
	
	/**
	 * @param queueId
	 * @return the JDL of this subjobID, if known, <code>null</code> otherwise
	 */
	public static String getJDL(final int queueId){
		final DBFunctions db = getDB();
		
		if (db==null)
			return null;
		
		if (monitor!=null){
			monitor.incrementCounter("TQ_db_lookup");
			monitor.incrementCounter("TQ_get_jdl");
		}
		
		final String q = "SELECT jdl FROM QUEUE WHERE queueId="+queueId;
		
		if (!db.query(q) || !db.moveNext())
			return null;
		
		return db.gets(1);
	}
	
	
	/**
	 * @param states 
	 * @param users 
	 * @param sites 
	 * @param nodes 
	 * @param mjobs 
	 * @param jobids 
	 * @param masterOnly 
	 * @param orderByKey 
	 * @param limit 
	 * @return the ps listing
	 */
	public static List<Job> getPS(final List<String> states,final List<String> users,final List<String> sites,
			final List<String> nodes,final List<String> mjobs,final List<String> jobids, final boolean masterOnly, final String orderByKey, final int limit){
				
		final DBFunctions db = getDB();
		
		if (db==null)
			return null;
		
		final List<Job> ret = new ArrayList<Job>();
		
		if (monitor!=null){
			monitor.incrementCounter("TQ_db_lookup");
		}
		
		int lim = 2000;
		if(limit>0 && limit<2000)
			lim = limit;

		String where = "";

		if (states != null && states.size()>0){
			String whe = " ( ";
			for (String s : states){
				if("%".equals(s)){
					whe = "";
					break;
				}
				whe += "status = '" + Format.escSQL(s) + "' or ";
			}
			if(whe.length()>0)
				where += whe.substring(0, whe.length()-3) + " ) and ";
		}
					
		if (users != null && users.size()>0){
			String whe = " ( ";
			for (String u : users){
				if("%".equals(u)){
					whe = "";
					break;
				}
				whe += "submitHost like '" + Format.escSQL(u) + "@%' or ";
			}
			if(whe.length()>0)
				where += whe.substring(0, whe.length()-3) + " ) and ";
		}
		
		if (sites != null && sites.size()>0){
			String whe = " ( ";
			for (String s : sites){
				if("%".equals(s)){
					whe = "";
					break;
				}
				whe += "site = '" + Format.escSQL(s) + "' or ";
			}
			if(whe.length()>0)
				where += whe.substring(0, whe.length()-3) + " ) and ";
		}
		
		
		if (nodes != null && nodes.size()>0){
			String whe = " ( ";
			for (String n : nodes){
				if("%".equals(n)){
					whe = "";
					break;
				}
				whe += "node = '" + Format.escSQL(n)  + "' or ";
			}
			if(whe.length()>0)
				where += whe.substring(0, whe.length()-3) + " ) and ";
		}

		if (mjobs != null && mjobs.size()>0){
			String whe = " ( ";
			for (String m : mjobs){
				if("%".equals(m)){
					whe = "";
					break;
				}
				whe += "split = '" + Format.escSQL(m)  + "' or ";
			}
			if(whe.length()>0)
				where += whe.substring(0, whe.length()-3) + " ) and ";
		}
		
		if (jobids != null && jobids.size()>0){
			String whe = " ( ";
			for (String i : jobids){
				if("%".equals(i)){
					whe = "";
					break;
				}
				whe += "queueId = '" + Format.escSQL(i) + "' or ";
			}
			if(whe.length()>0)
				where += whe.substring(0, whe.length()-3) + " ) and ";
		}
		
		if(masterOnly)
			where += " masterjob=1 and ";
		
					
		if(where.endsWith(" and "))
			where = where.substring(0,where.length()-5);
		
		String orderBy = " order by ";
		if (orderByKey==null || orderByKey.length()==0)
			orderBy +=  " queueId asc ";
		else
			orderBy += orderByKey + " asc ";
		
		final String q = "SELECT * FROM QUEUE WHERE "+ where + orderBy +" limit "+lim+";";
			
		if (!db.query(q))
			return null;
		
		while (db.moveNext()){
			final Job j = new Job(db, false);
			ret.add(j);
		}
		
		return ret;

	}
	
	
	
	/**
	 * @return matching jobs histograms
	 */
	public static Map<Integer, Integer> getMatchingHistogram(){
		final Map<Integer, Integer> ret = new TreeMap<Integer, Integer>();
		
		final DBFunctions db = getDB();
		
		if (db==null)
			return ret;
		
		final Map<Integer, AtomicInteger> work = new HashMap<Integer, AtomicInteger>();
		
		if (monitor!=null){
			monitor.incrementCounter("TQ_db_lookup");
			monitor.incrementCounter("TQ_matching_histogram");
		}
		
		db.query("select site,sum(counter) from JOBAGENT where counter>0 group by site");
		
		while (db.moveNext()){
			final String site = db.gets(1);
			
			int key = 0;
			
			final StringTokenizer st = new StringTokenizer(site, ",; ");
			
			while (st.hasMoreTokens()){
				if (st.nextToken().length()>0)
					key++;
			}
			
			final int value = db.geti(2);
			
			final Integer iKey = Integer.valueOf(key);
			
			final AtomicInteger ai = work.get(iKey);
			
			if (ai==null)
				work.put(iKey, new AtomicInteger(value));
			else
				ai.addAndGet(value);
		}
		
		for (final Map.Entry<Integer, AtomicInteger> entry: work.entrySet()){
			ret.put(entry.getKey(), Integer.valueOf(entry.getValue().intValue()));
		}
		
		return ret;
	}
	
	/**
	 * @return the number of matching waiting jobs for each site
	 */
	public static Map<String, Integer> getMatchingJobsSummary(){
		final Map<String, Integer> ret = new TreeMap<String, Integer>();

		final DBFunctions db = getDB();
		
		if (db==null)
			return ret;
		
		final Map<String, AtomicInteger> work = new HashMap<String, AtomicInteger>();
		
		if (monitor!=null){
			monitor.incrementCounter("TQ_db_lookup");
			monitor.incrementCounter("TQ_matching_jobs_summary");
		}
		
		db.query("select site,sum(counter) from JOBAGENT where counter>0 group by site order by site;");
		
		int addToAll = 0;
		
		while (db.moveNext()){
			final String sites = db.gets(1);
			final int count = db.geti(2);
			
			if (sites.length()==0)
				addToAll = count;
			else{
				final StringTokenizer st = new StringTokenizer(sites,",; ");
				
				while (st.hasMoreTokens()){
					final String site = st.nextToken().trim();
					
					final AtomicInteger ai = work.get(site);
					
					if (ai==null)
						work.put(site, new AtomicInteger(count));
					else
						ai.addAndGet(count);
				}
			}
		}
		
		for (final Map.Entry<String, AtomicInteger> entry: work.entrySet()){
			ret.put(entry.getKey(), Integer.valueOf(entry.getValue().intValue() + addToAll));
		}
		
		if (addToAll > 0){
			final Set<String> sites = LDAPHelper.checkLdapInformation("(objectClass=organizationalUnit)", "ou=Sites,", "ou", false);
			
			if (sites!=null){
				for (final String site: sites){
					if (!ret.containsKey(site))
						ret.put(site, Integer.valueOf(addToAll));
				}
			}
		}
		
		return ret;
	}
	
	
	
	/**
	 * Kill a job in the queue 
	 * 
	 * @param user
	 * @param queueId
	 * @return status of the kill
	 */
	public static boolean submit(final AliEnPrincipal user, final int queueId){
		// TODO check if the user is allowed to kill and do it
		return false;
	}
	
	
	/**
	 * Submit the JDL indicated by this file
	 * 
	 * @param file the catalogue name of the JDL to be submitted
	 * @param account account from where the submit command was received, in the form "username@hostname"
	 * @param arguments arguments to the JDL, should be at least as many as the largest $N that shows up in the JDL
	 * @return the job ID
	 * @throws IOException in case of problems like downloading the respective JDL or not enough arguments provided to it 
	 */
	public static int submit(final LFN file, final String account, final String[] arguments) throws IOException {
		final String jdlContents = IOUtils.getContents(file);
		
		if (jdlContents==null || jdlContents.length()==0)
			throw new IOException("Could not download "+file.getCanonicalName());
		
		return submit(jdlContents, account, arguments);
	}
	
	private static final Pattern p = Pattern.compile("\\$(\\d+)");
	
	/**
	 * Submit this JDL body
	 * 
	 * @param jdl job description, in plain text
	 * @param account account from where the submit command was received, in the form "username@hostname"
	 * @param arguments arguments to the JDL, should be at least as many as the largest $N that shows up in the JDL
	 * @return the job ID
	 * @throws IOException in case of problems such as the number of provided arguments is not enough
	 */
	public static int submit(final String jdl, final String account, final String... arguments) throws IOException{
		String jdlToSubmit = jdl;
		
		Matcher m = p.matcher(jdlToSubmit);
		
		while (m.find()){
			final String s = m.group(1);
			
			final int i = Integer.parseInt(s);
			
			if (arguments==null || arguments.length<i)
				throw new IOException("JDL indicates argument $"+i+" but you haven't provided it");
			
			jdlToSubmit = jdlToSubmit.replaceAll("\\$"+i+"(?!\\d)", arguments[i-1]);
			
			m = p.matcher(jdlToSubmit);
		}
		
		final DBFunctions db = getDB();
		
		final Map<String, Object> values = new HashMap<String, Object>();
		
		final JDL j = new JDL(jdlToSubmit);
		
		String executable = j.getExecutable();
		String sPrice = j.gets("Price");
		
		Float price;
		
		try{
			price = Float.valueOf(sPrice);
		}
		catch (Throwable t){
			price = Float.valueOf(1);
		}
		
		values.put("name", executable);
		values.put("status", "INSERTING");
		values.put("received", Long.valueOf(System.currentTimeMillis()/1000));
		values.put("submitHost", account);
		values.put("jdl", jdlToSubmit);
		values.put("price", price);
		
		final String insert = DBFunctions.composeInsert("QUEUE", values);
		
		db.setLastGeneratedKey(true);
		
		if (!db.query(insert))
			throw new IOException("Could not insert the job in the queue");
		
		final Integer pid = db.getLastGeneratedKey();
		
		if (pid==null)
			return -1;
		
		return pid.intValue();
	}
	
	
	
	/**
	 * @param queueId
	 * @return state of the kill operation
	 */
	public static boolean killJob(final int queueId){
		return false;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
