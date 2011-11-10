package alien.taskQueue;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import lazyj.StringFactory;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;
import alien.user.LDAPHelper;

/**
 * @author ron
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
	
	
//	private static final DateFormat formatter = new SimpleDateFormat("MMM dd HH:mm");

	
	/**
	 * @return the database connection to 'processes'
	 */
	public static DBFunctions getQueueDB(){
		final DBFunctions db = ConfigUtils.getDB("processes");
		return db;
	}
	
	/**
	 * @return the database connection to 'ADMIN'
	 */
	public static DBFunctions getAdminDB(){
		final DBFunctions db = ConfigUtils.getDB("admin");
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
		final DBFunctions db = getQueueDB();
		
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
		final DBFunctions db = getQueueDB();
		
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
	public static Map<Job, Map<JobStatus, Integer>> getMasterjobStats(final String account){
		return getMasterjobStats(getMasterjobs(account));
	}
	
	/**
	 * @param jobs
	 * @return the same masterjobs and the respective subjob statistics
	 */
	public static Map<Job, Map<JobStatus, Integer>> getMasterjobStats(final List<Job> jobs){
		final Map<Job, Map<JobStatus, Integer>> ret = new TreeMap<Job, Map<JobStatus, Integer>>();
		
		if (jobs.size()==0)
			return ret;
		
		final DBFunctions db = getQueueDB();

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
			
		Map<JobStatus, Integer> m = null;
		int oldJobID = -1;
			
		while (db.moveNext()){
			final int j = db.geti(1);
			
			if (j!=oldJobID || m==null){
				m = new HashMap<JobStatus, Integer>();

				final Integer jobId = Integer.valueOf(j);
				ret.put(reverse.get(jobId), m);
				reverse.remove(jobId);
				
				oldJobID = j;
			}
			
			m.put(JobStatus.getStatus(db.gets(2)), Integer.valueOf(db.geti(3)));
		}
		
		// now, what is left, something that doesn't have subjobs ?
		for (final Job j: reverse.values()){
			m = new HashMap<JobStatus, Integer>(1);
			m.put(j.status(), Integer.valueOf(1));
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
		final DBFunctions db = getQueueDB();
		
		if (db==null)
			return null;
		
		if (monitor!=null){
			monitor.incrementCounter("TQ_db_lookup");
			monitor.incrementCounter("TQ_getsubjobs");
		}
		
		final String q = "SELECT "+(loadJDL ? "*" : ALL_BUT_JDL)+" FROM QUEUE WHERE split="+Format.escSQL(queueId+"")+" ORDER BY queueId ASC;";
		
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
	 * Get the subjob status of this masterjob
	 * 
	 * @param queueId
	 * @param status 
	 * @param id 
	 * @param site 
	 * @param bPrintId 
	 * @param bPrintSite 
	 * @param bMerge 
	 * @param bKill 
	 * @param bResubmit 
	 * @param bExpunge 
	 * @param limit 
	 * @return the subjobs, if any
	 */
	public static List<Job> getMasterJobStat(final int queueId, final Set<JobStatus> status, final List<Integer> id, final List<String> site,
			final boolean bPrintId, final boolean bPrintSite, final boolean bMerge, final boolean bKill, 
			final boolean bResubmit, final boolean bExpunge, final int limit){
		
		final DBFunctions db = getQueueDB();
		
		if (db==null)
			return null;
		
		if (monitor!=null){
			monitor.incrementCounter("TQ_db_lookup");
		}

		String where = "";

		if (queueId>0)
			where = " split = '" + Format.escSQL(queueId+"") + "' and ";
		else
			return null;
		
		
		if (status != null && status.size()>0 && !status.contains(JobStatus.ANY)){
			final StringBuilder whe = new StringBuilder(" ( status in (");
			
			boolean first = true;
			
			for (final JobStatus s : status){
				if (!first)
					whe.append(',');
				else
					first = false;
				
				whe.append('\'').append(s.toSQL()).append('\'');
			}
			
			where += whe+") ) and ";
		}
			
		if (id != null && id.size()>0){
			final StringBuilder whe = new StringBuilder(" ( queueId in (");
			
			boolean first = true;
			
			for (int i : id){
				if (!first)
					whe.append(',');
				else
					first = false;
				
				whe.append(i);
			}
			
			where += whe + ") ) and ";
		}
	
		if (site != null && site.size()>0){
			final StringBuilder whe = new StringBuilder(" ( ");
			
			boolean first = true;
			
			for (final String s : site){
				if (!first)
					whe.append(" or ");
				else
					first = false;

				whe.append("ifnull(substring(exechost,POSITION('\\@' in exechost)+1),'')='").append(Format.escSQL(s)).append('\'');
			}
			
			where += whe.substring(0, whe.length()-3) + " ) and ";
		}
		
		if(where.endsWith(" and "))
			where = where.substring(0,where.length()-5);

		
		if(where.length()>0)
			where = " WHERE " + where;
		

		int lim = 10000;
		if(limit>0 && limit<10000)
			lim = limit;

		
		final String q = "SELECT queueId,status,split,execHost FROM QUEUE "+ where + " ORDER BY queueId ASC limit "+ lim+";";
					
		if (!db.query(q))
			return null;
		
		final List<Job> ret = new ArrayList<Job>();

		db.query(q);

		while (db.moveNext()){
			ret.add(new Job(db, false));
		}

		return ret;
	}
	
	/**
	 * @param jobs
	 * @return the jobs grouped by their state
	 */
	public static Map<JobStatus, List<Job>> groupByStates(final List<Job> jobs){
		if (jobs==null)
			return null;
		
		final Map<JobStatus, List<Job>> ret = new TreeMap<JobStatus, List<Job>>();
		
		if (jobs.size()==0)
			return ret;
		
		for (final Job j: jobs){
			List<Job> l = ret.get(j.status());
			
			if (l==null){
				l = new ArrayList<Job>();
				ret.put(j.status(), l);
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
	 * @return <code>true</code> if the job status was changed
	 */
	public static boolean setJobStatus(final int job, final JobStatus newStatus){
		return setJobStatus(job, newStatus, null);
	}
	
	/**
	 * @param job
	 * @param newStatus
	 * @param oldStatusConstraint change the status only if the job is still in this state. Can be <code>null</code> to disable checking the current status. 
	 * @return <code>true</code> if the job status was changed
	 */
	public static boolean setJobStatus(final int job, final JobStatus newStatus, final JobStatus oldStatusConstraint){
		final DBFunctions db = getQueueDB();
		
		if (db==null){
			logger.log(Level.SEVERE, "Cannot get the queue database entry");
			
			return false;
		}
		
		if (!db.query("SELECT status FROM QUEUE where queueId="+job)){
			logger.log(Level.SEVERE, "Error executing the select query from QUEUE");
			
			return false;
		}
		
		if (!db.moveNext()){
			logger.log(Level.FINE, "Could not find queueId "+job+" in the queue");
			
			return false;
		}
		
		final JobStatus oldStatus = JobStatus.getStatus(db.gets(1));
		
		if (oldStatusConstraint!=null && oldStatus!=oldStatusConstraint){
			logger.log(Level.FINE, "Refusing to do the update of "+job+" to state "+newStatus.name()+" because old status is not "+oldStatusConstraint.name()+" but "+oldStatus.name());
			
			return false;
		}
		
		if (!db.query("UPDATE QUEUE SET status='"+newStatus.toSQL()+"' WHERE queueId="+job))
			return false;
		
		final boolean updated = db.getUpdateCount()!=0;

		putJobLog(job, "state", "Job state transition from "+oldStatus.name()+" to "+newStatus.name(), null);
		
		return updated;
	}
	
	/**
	 * @param queueId
	 * @return the JDL of this subjobID, if known, <code>null</code> otherwise
	 */
	public static String getJDL(final int queueId){
		final DBFunctions db = getQueueDB();
		
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
	 * @param orderByKey 
	 * @param limit 
	 * @return the ps listing
	 */
	public static List<Job> getPS(final Collection<JobStatus> states,final Collection<String> users,final Collection<String> sites,
			final Collection<String> nodes,final Collection<Integer> mjobs,final Collection<Integer> jobids, final String orderByKey, final int limit){
				
		final DBFunctions db = getQueueDB();
		
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

		if (states != null && states.size()>0 && !states.contains(JobStatus.ANY)){
			final StringBuilder whe = new StringBuilder(" (status in (");
			
			boolean first = true;
			
			for (final JobStatus s : states){
				if (!first)
					whe.append(",");
				else
					first = false;

				whe.append('\'').append(s.toSQL()).append('\'');
			}
			
			where += whe + ") ) and ";
		}
					
		if (users != null && users.size()>0 && !users.contains("%")){
			final StringBuilder whe = new StringBuilder(" ( ");
			
			boolean first = true;
			
			for (final String u : users){
				if (!first)
					whe.append(" or ");
				else
					first = false;
								
				whe.append("submitHost like '").append(Format.escSQL(u)).append("@%'");
			}
			
			where += whe + " ) and ";
		}
		
		if (sites != null && sites.size()>0 && !sites.contains("%")){
			final StringBuilder whe = new StringBuilder(" ( site in (");
			
			boolean first = true;
			
			for (final String s : sites){
				if (!first)
					whe.append(',');
				else
					first = false;
				
				whe.append('\'').append(Format.escSQL(s)).append('\'');
			}
			
			where += whe + ") ) and ";
		}		
		
		if (nodes != null && nodes.size()>0 && !nodes.contains("%")){
			StringBuilder whe = new StringBuilder(" ( node in (");
			
			boolean first = true;
			
			for (final String n : nodes){
				if (!first)
					whe.append(',');
				else
					first = false;

				whe.append('\'').append(Format.escSQL(n)).append('\'');
			}
			
			where += whe + ") ) and ";
		}

		if (mjobs != null && mjobs.size()>0 && !mjobs.contains(Integer.valueOf(0))){
			final StringBuilder whe = new StringBuilder(" ( split in (");
			
			boolean first = true;
						
			for (final Integer m : mjobs){
				if (!first)
					whe.append(',');
				else
					first = false;
				
				whe.append(m);
			}
			
			where += whe + ") ) and ";
		}
		
		if (jobids != null && jobids.size()>0 && !jobids.contains(Integer.valueOf(0))){
			final StringBuilder whe = new StringBuilder(" ( queueId in (");
			
			boolean first = true;
						
			for (final Integer i : jobids){
				if (!first)
					whe.append(',');
				else
					first = false;
				
				whe.append(i);
			}

			where += whe + ") ) and ";
		}
					
		if (where.endsWith(" and "))
			where = where.substring(0,where.length()-5);
		
		String orderBy = " order by ";
		
		if (orderByKey==null || orderByKey.length()==0)
			orderBy +=  " queueId asc ";
		else
			orderBy += orderByKey + " asc ";
		
		if(where.length()>0)
			where = " WHERE " + where;
		
		final String q = "SELECT "+ ALL_BUT_JDL +" FROM QUEUE "+ where + orderBy +" limit "+lim+";";
		
		System.out.println("SQL: " + q);
					
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
		
		final DBFunctions db = getQueueDB();
		
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

		final DBFunctions db = getQueueDB();
		
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
	public static boolean kill(final AliEnPrincipal user, final int queueId){
		// TODO check if the user is allowed to kill and do it
		return false;
	}
	
	private static final Pattern p = Pattern.compile("\\$(\\d+)");
	
	/**
	 * @param jdlContents JDL specification
	 * @param arguments arguments to the JDL, should be at least as many as the largest $N that shows up in the JDL
	 * @return the parsed JDL, with all $N parameters replaced with the respective argument
	 * @throws IOException if there is any problem parsing the JDL content
	 */
	public static JDL applyJDLArguments(final String jdlContents, final String... arguments) throws IOException {
		String jdlToSubmit = jdlContents;
		
		Matcher m = p.matcher(jdlToSubmit);
		
		while (m.find()){
			final String s = m.group(1);
			
			final int i = Integer.parseInt(s);
			
			if (arguments==null || arguments.length<i)
				throw new IOException("The JDL indicates argument $"+i+" but you haven't provided it");
			
			jdlToSubmit = jdlToSubmit.replaceAll("\\$"+i+"(?!\\d)", arguments[i-1]);
			
			m = p.matcher(jdlToSubmit);
		}
		
		final JDL jdl = new JDL(jdlToSubmit);
		
		return jdl;
	}
	
	/**
	 * Submit the JDL indicated by this file
	 * 
	 * @param file the catalogue name of the JDL to be submitted
	 * @param account account from where the submit command was received
	 * @param arguments arguments to the JDL, should be at least as many as the largest $N that shows up in the JDL
	 * @return the job ID
	 * @throws IOException in case of problems like downloading the respective JDL or not enough arguments provided to it 
	 */
	public static int submit(final LFN file, final AliEnPrincipal account, final String... arguments) throws IOException {
		if (file==null || !file.exists || !file.isFile()){
			throw new IllegalArgumentException("The LFN is not a valid file");
		}
		
		final String jdlContents = IOUtils.getContents(file);
		
		if (jdlContents==null || jdlContents.length()==0)
			throw new IOException("Could not download "+file.getCanonicalName());
		
		final JDL jdl = applyJDLArguments(jdlContents, arguments);
		
		jdl.set("JDLPath", file.getCanonicalName());
		
		return submit(jdl, account);
	}
	
	private static void prepareJDLForSubmission(final JDL jdl, final AliEnPrincipal owner) throws IOException {
		Float price = jdl.getFloat("Price");
		
		if (price==null)
			price = Float.valueOf(1);
		
		jdl.set("Price", price);
		
		Integer ttl = jdl.getInteger("TTL");
		
		if (ttl==null || ttl.intValue()<=0)
			ttl = Integer.valueOf(21600);
		
		jdl.set("TTL", ttl);
		
		jdl.set("Type", "Job");
		
		if (jdl.get("OrigRequirements")==null){
			jdl.set("OrigRequirements", jdl.get("Requirements"));
		}
		
		if (jdl.get("MemorySize")==null)
			jdl.set("MemorySize", "8GB");
		
		jdl.set("User", owner.getName());	

		// set the requirements anew
		jdl.delete("Requirements");
		
		jdl.addRequirement("other.Type == \"machine\"");
		
		final List<String> packages = jdl.getList("Packages");
		
		if (packages!=null){
			for (final String pack: packages){
				// TODO : check if the package is really defined and throw an exception if not
				
				jdl.addRequirement("member(other.Packages,\""+pack+"\")");
			}
		}
		
		jdl.addRequirement(jdl.gets("OrigRequirements"));
		
		jdl.addRequirement("other.TTL > "+ttl);
		jdl.addRequirement("other.Price <= 1");
		
		// InputFile -> (InputDownload and InputBox)
		
		final List<String> inputFiles = jdl.getList("InputFile");
		
		if (inputFiles!=null){
			for (final String file: inputFiles){
				if (file.indexOf('/')<0){
					throw new IOException("InputFile contains an illegal entry: "+file);
				}
				
				String lfn = file;
				
				if (lfn.startsWith("LF:")){
					lfn = lfn.substring(3);
				}
				else{
					throw new IOException("InputFile doesn't start with 'LF:' : "+lfn);
				}
				
				final LFN l = LFNUtils.getLFN(lfn);
				
				if (l==null || !l.isFile()){
					throw new IOException("InputFile "+lfn+" doesn't exist in the catalogue");
				}
				
				jdl.append("InputBox", lfn);
				jdl.append("InputDownload", l.getFileName()+"->"+lfn);
			}
		}
		
		final List<String> inputData = jdl.getList("InputData");
		
		if (inputData!=null){
			for (final String file: inputData){
				if (file.indexOf('/')<0){
					throw new IOException("InputData contains an illegal entry: "+file);
				}
				
				String lfn = file;
				
				if (lfn.startsWith("LF:")){
					lfn = lfn.substring(3);
				}
				else{
					throw new IOException("InputData doesn't start with 'LF:' : "+lfn);
				}
				
				if (lfn.indexOf(',')>=0)
					lfn = lfn.substring(0, lfn.indexOf(','));	// "...,nodownload" for example
				
				final LFN l = LFNUtils.getLFN(lfn);
				
				if (l==null || !l.isFile()){
					throw new IOException("InputData "+lfn+" doesn't exist in the catalogue");
				}			
			}
		}
		
		// sanity check of other tags
		
		for (final String tag: Arrays.asList("ValidationCommand", "InputDataCollection")){
			final List<String> files = jdl.getList(tag);
			
			if (files==null)
				continue;
			
			for (final String file: files){
				String fileName = file;
				
				if (fileName.indexOf(',')>=0)
					fileName = fileName.substring(0, fileName.indexOf(','));
				
				final LFN l = LFNUtils.getLFN(fileName);
				
				if (l==null || (!l.isFile() && !l.isCollection())){
					throw new IOException(tag+" tag required "+fileName+" which is not valid: "+l);
				}
			}
		}
		
		if (jdl.getExecutable()==null){
			throw new IOException("Your JDL doesn't indicate an accessible Executable");
		}
	}
	
	/**
	 * Submit this JDL body
	 * 
	 * @param j job description, in plain text
	 * @param account account from where the submit command was received
	 * @return the job ID
	 * @throws IOException in case of problems such as the number of provided arguments is not enough
	 * @see #applyJDLArguments(String, String...)
	 */
	public static int submit(final JDL j, final AliEnPrincipal account) throws IOException{
		// TODO : check this account's quota before submitting
		
		final DBFunctions db = getQueueDB();
		
		final Map<String, Object> values = new HashMap<String, Object>();
		
		prepareJDLForSubmission(j, account);
		
		final String executable = j.getExecutable();
		
		final Float price = j.getFloat("Price");

		final String clientAddress;
		
		final InetAddress addr = account.getRemoteEndpoint();
			
		if (addr!=null)
			clientAddress = addr.getCanonicalHostName();
		else
			clientAddress = MonitorFactory.getSelfHostname();
		
		//final JobStatus targetStatus = j.get("split")!=null ? JobStatus.SPLITTING : JobStatus.INSERTING;
		
		final JobStatus targetStatus = JobStatus.INSERTING; 
		
		values.put("priority", Integer.valueOf(0));
		values.put("submitHost", account.getName()+"@"+clientAddress);
		values.put("status", targetStatus.toSQL());
		values.put("name", executable);
		values.put("chargeStatus", Integer.valueOf(0));
		values.put("jdl", "\n    [\n"+j.toString()+"\n    ]");
		values.put("price", price);
		values.put("received", Long.valueOf(System.currentTimeMillis()/1000));
		values.put("split", Integer.valueOf(0));
		
		final String insert = DBFunctions.composeInsert("QUEUE", values);
		
		db.setLastGeneratedKey(true);
		
		if (!db.query(insert))
			throw new IOException("Could not insert the job in the queue");
		
		final Integer pid = db.getLastGeneratedKey();
		
		if (pid==null)
			return -1;
		
		db.query("INSERT INTO QUEUEPROC (queueId) VALUES ("+pid+");");
		
		setAction(targetStatus);
		
		return pid.intValue();
	}
	
	/**
	 * @param user 
	 * @param role 
	 * @param queueId
	 * @return state of the kill operation
	 */
	public static boolean killJob(final AliEnPrincipal user, final String role, final int queueId) {
		if (AuthorizationChecker.canModifyJob(TaskQueueUtils.getJob(queueId), user, role)) {
			System.out.println("Authorized job kill for [" + queueId + "] by user/role [" + user.getName() + "/" + role + "].");

			Job j = getJob(queueId, true);

			// setJobStatus(j, JobStatus.KILLED,"",null,null,null);

			if (j.execHost != null && false) {

				// my ($port) = $self->{DB}->getFieldFromHosts($data->{exechost}, "hostport")
				// or $self->info("Unable to fetch hostport for host $data->{exechost}")
				// and return (-1, "unable to fetch hostport for host $data->{exechost}");
				//
				// $DEBUG and $self->debug(1, "Sending a signal to $data->{exechost} $port to kill the process... ");
				String target = j.execHost.substring(j.execHost.indexOf('@' + 1));

				int expires = (int) (System.currentTimeMillis() / 1000) + 300;

				insertMessage(target, "ClusterMonitor", "killProcess", j.queueId + "", expires);

			}

			// The removal has to be done properly, in Perl it was just the default !/alien-job directory
			// $self->{CATALOGUE}->execute("rmdir", $procDir, "-r")

			return false;

		}

		System.out.println("Job kill authorization failed for [" + queueId + "] by user/role [" + user.getName() + "/" + role + "].");
		return false;
	}
	
	
	
	//status and jdl
	private static boolean updateJob(final Job j, final JobStatus newStatus, final Map<String, String> jdltags) {

		if (newStatus.smallerThanEquals(j.status()) && (j.status() == JobStatus.ZOMBIE || j.status() == JobStatus.IDLE || j.status() == JobStatus.INTERACTIV) && j.isMaster()) {

			// if ( ($self->{JOBLEVEL}->{$status} <= $self->{JOBLEVEL}->{$dboldstatus})
			// && ($dboldstatus !~ /^((ZOMBIE)|(IDLE)|(INTERACTIV))$/)
			// && (!$masterjob)) {
			// if ($set->{path}) {
			// return $self->updateJob($id, {path => $set->{path}});
			// }
			// my $message =
			// "The job $id [$dbsite] was in status $dboldstatus [$self->{JOBLEVEL}->{$dboldstatus}] and cannot be changed to $status [$self->{JOBLEVEL}->{$status}]";
			// if ($set->{jdl} and $status =~ /^(SAVED)|(SAVED_WARN)|(ERROR_V)$/) {
			// $message .= " (although we update the jdl)";
			// $self->updateJob($id, {jdl => $set->{jdl}});
			// }
			// $self->{LOGGER}->set_error_msg("Error updating the job: $message");
			// $self->info("Error updating the job: $message", 1);
			// return;
			// }
			//
			// #update the value, it is correct
			// if (!$self->updateJob($id, $set, {where => "status=?", bind_values => [$dboldstatus]},)) {
			// my $message = "The update failed (the job changed status in the meantime??)";
			// $self->{LOGGER}->set_error_msg($message);
			// $self->info("There was an error: $message", 1);
			// return;
			// }

			return false;
		}

		if (j.status() == JobStatus.WAITING && j.jobagentId > 0)
			if (!deleteJobAgent(j.jobagentId))
				logger.log(Level.WARNING, "Error killing jobAgent: [" + j.jobagentId + "].");

		// $self->info("THE UPDATE WORKED!! Let's see if we have to delete an agent $status");
		// if ($dboldstatus =~ /WAITING/ and $oldjobinfo->{agentid}) {
		// $self->deleteJobAgent($oldjobinfo->{agentid});
		// }

		// TODO:
		// # send a job's status to MonaLisa
		// sub sendJobStatus {
		// my $self = shift;
		// my ($jobID, $newStatus, $execHost, $submitHost) = @_;
		//
		// if ($self->{MONITOR}) {
		// my $statusID = AliEn::Util::statusForML($newStatus);
		// $execHost = $execHost || "NO_SITE";
		// my @params = ("jobID", $jobID, "statusID", $statusID);
		// push(@params, "submitHost", "$jobID/$submitHost") if $submitHost;
		// $self->{MONITOR}->sendParameters("TaskQueue_Jobs_" . $self->{CONFIG}->{ORG_NAME}, $execHost, @params);
		// }
		// }

		if (j.notify != null && !j.notify.equals(""))
			sendNotificationMail(j);

		if (j.split != 0)
			setSubJobMerges(j);

		if (j.status() != newStatus) {
			if (newStatus == JobStatus.ASSIGNED) {
				// $self->_do("UPDATE $self->{SITEQUEUETABLE} SET $status=$status+1 where site=?", {bind_values =>
				// [$dbsite]})
				// TODO:
			}
			else {
				// do(
				// "UPDATE $self->{SITEQUEUETABLE} SET $dboldstatus = $dboldstatus-1, $status=$status+1 where site=?",
				// {bind_values => [$dbsite]}
			}
		}

		if (newStatus == JobStatus.KILLED || newStatus == JobStatus.SAVED || newStatus == JobStatus.SAVED_WARN || newStatus == JobStatus.STAGING)
			setAction(newStatus);

		// if ($status =~ /^DONE_WARN$/) {
		// $self->sendJobStatus($id, "DONE", $execHost, "");
		// }

		return true;
	}
	
	private static boolean setJobStatus(final Job j, final JobStatus newStatus, final String arg, final String site, final String spyurl, final String node) {

		String time = String.valueOf(System.currentTimeMillis() / 1000);

		HashMap<String, String> jdltags = new HashMap<String, String>();
		jdltags.put("procinfotime", time);
		if (spyurl != null)
			jdltags.put("spyurl", spyurl);
		if (site != null)
			jdltags.put("site", site);
		if (node != null)
			jdltags.put("node", node);

		if (newStatus == JobStatus.WAITING)
			jdltags.put("exechost", arg);
		else
			if (newStatus == JobStatus.RUNNING)
				jdltags.put("started", time);
			else
				if (newStatus == JobStatus.STARTED) {
					jdltags.put("started", time);
					jdltags.put("batchid", arg);
				}
				else
					if (newStatus == JobStatus.SAVING)
						jdltags.put("error", arg);
					else
						if ((newStatus == JobStatus.SAVED && arg != null && !"".equals(arg)) || newStatus == JobStatus.ERROR_V || newStatus == JobStatus.STAGING)
							jdltags.put("jdl", arg);
						else
							if (newStatus == JobStatus.DONE || newStatus == JobStatus.DONE_WARN) {
								jdltags.put("finished", time);
								if (j.usesValidation()) {
									String host = j.execHost.substring(j.execHost.indexOf('@') + 1);
									int port = 0; // $self->{CONFIG}->{CLUSTERMONITOR_PORT};

									// my $executable = "";
									// $data->{jdl} =~ /executable\s*=\s*"?(\S+)"?\s*;/i and $executable = $1;
									// $executable =~ s/\"//g;
									// my $validatejdl = "[
									// Executable=\"$executable.validate\";
									// Arguments=\"$queueId $data->{host} $port\";
									// Requirements= member(other.GridPartition,\"Validation\");
									// Type=\"Job\";
									// ]";
									// $DEBUG and $self->debug(1,
									// "In changeStatusCommand sending the command to validate the result of $queueId...");
									// $self->enterCommand("$data->{submithost}", "$validatejdl");
									// }

								}

							}
							else
								if (newStatus.isErrorState() || newStatus == JobStatus.SAVED_WARN || newStatus == JobStatus.SAVED || newStatus == JobStatus.KILLED || newStatus == JobStatus.EXPIRED) {

									jdltags.put("spyurl", "");
									jdltags.put("finished", time);
									deleteJobToken(j.queueId);

								}
		// put the JobLog message

		final HashMap<String, String> joblogtags = new HashMap<String, String>(jdltags);

		String message = "Job state transition from " + j.getStatusName() + " to " + newStatus;

		final boolean success = updateJob(j, newStatus, jdltags);

		if (!success)
			message = "FAILED: " + message;

		putJobLog(j.queueId, "state", message, joblogtags);

		if (site != null) {
			// # lock queues with submission errors ....
			// if ($status eq "ERROR_S") {
			// $self->_setSiteQueueBlocked($site)
			// or $self->{LOGGER}->error("JobManager", "In changeStatusCommand cannot block site $site for ERROR_S");
			// } elsif ($status eq "ASSIGNED") {
			// my $sitestat = $self->getSiteQueueStatistics($site);
			// if (@$sitestat) {
			// if (@$sitestat[0]->{'ASSIGNED'} > 5) {
			// $self->_setSiteQueueBlocked($site)
			// or $self->{LOGGER}->error("JobManager", "In changeStatusCommand cannot block site $site for ERROR_S");
			// }
			// }
			// }
		}
		return success;
	}
	
	
	private static boolean updateJDLAndProcInfo(final Job j, final Map<String,String> jdltags, final Map<String,String> procInfo){
		
//		  my $procSet = {};
//		  foreach my $key (keys %$set) {
//		    if ($key =~
//		/(si2k)|(cpuspeed)|(maxrsize)|(cputime)|(ncpu)|(cost)|(cpufamily)|(cpu)|(vsize)|(rsize)|(runtimes)|(procinfotime)|(maxvsize)|(runtime)|(mem)|(batchid)/
//		      ) {
//		      $procSet->{$key} = $set->{$key};
//		      delete $set->{$key};
//		    }
//		  }
		
		//TODO: set the procinfo, is necessary
		
		//TODO: update the jdltags
		return true;
		
	}

	private static boolean insertMessage(final String target, final String service, 
			final String message, final String messageArgs, final int expires){
		final DBFunctions db = getQueueDB();
		
		String q = "INSERT INTO MESSAGES ( TargetService, Message, MessageArgs, Expires)  VALUES ('"
					+ Format.escSQL(target)+"','"
					+ Format.escSQL(service) +"','"
					+ Format.escSQL(message) +"','"
					+ Format.escSQL(messageArgs) +"',"
					+ Format.escSQL(expires+"")
					+ ");";
		 
			
			if (db.query(q)){
				if (monitor != null)
					monitor.incrementCounter("Message_db_insert");
			
				return true;
			}
			
			return false;
	}
	

	private static JobToken insertJobToken(final int jobId, final String username, 
			final boolean forceUpdate) {
		final DBFunctions db = getAdminDB();
		
		JobToken jb = getJobToken(jobId);
		
		if(jb!=null)
			System.out.println("TOKEN EXISTED");

		
		if(jb!=null && !forceUpdate)
			return null;
	
		if(jb==null)
			jb = new JobToken(jobId,username);
		
		jb.spawnToken(db);
	
		
		System.out.println("forceUpdate token: " + jb.toString());
		
		if(jb.exists())
			return jb;
		
		System.out.println("jb does not exist");
		return null;
		
	}
	
	private static JobToken getJobToken(final int jobId){
		final DBFunctions db = getAdminDB();
		
		if (monitor!=null){
			monitor.incrementCounter("ADM_db_lookup");
			monitor.incrementCounter("ADM_jobtokendetails");
		}
		
		final long lQueryStart = System.currentTimeMillis();

		final String q = "SELECT * FROM jobToken WHERE jobId="+jobId;
	
		if (!db.query(q))
			return null;
		
		monitor.addMeasurement("ADM_jobtokendetails_time", (System.currentTimeMillis() - lQueryStart)/1000d);
		
		if (!db.moveNext()){
			return null;
		}
		
		return new JobToken(db);
	}
	
	private static boolean deleteJobToken(final int queueId){
		final DBFunctions db = getAdminDB();
		
		if (monitor!=null){
			monitor.incrementCounter("ADM_db_lookup");
		}
	
		final String q = "SELECT * FROM jobToken WHERE jobId="+queueId;
	
		if (!db.query(q))
			return false;
				
		if (!db.moveNext()){
			return false;
		}
		
		final JobToken jb =  new JobToken(db);
		if (jb.exists())
			return jb.destroy(db);
		
		return false;

	}
	
	/**
	 * @param queueId
	 * @param action
	 * @param message
	 * @param joblogtags
	 * @return <code>true</code> if the log was successfully added
	 */
	public static boolean putJobLog(final int queueId, final String action, final String message, final HashMap<String,String> joblogtags){
		final DBFunctions db = getQueueDB();
		
		if (monitor!=null){
			monitor.incrementCounter("TQ_db_lookup");
			monitor.incrementCounter("TQ_JOBMESSAGES_insert");
		}
		
		final Map<String, Object> insertValues = new HashMap<String, Object>(4);
		
		insertValues.put("timestamp", Long.valueOf(System.currentTimeMillis()/1000));
		insertValues.put("jobId", Integer.valueOf(queueId));
		insertValues.put("procinfo", message);
		insertValues.put("tag", action);
		
		return db.query(DBFunctions.composeInsert("JOBMESSAGES", insertValues));
	}

	private static boolean deleteJobAgent(final int jobagentId){
		System.out.println("We would be asked to kill jobAgent: [" + jobagentId + "].");
		//TODO:
		return true;
	}
	
	private static void checkFinalAction(final Job j){
		if(j.notify!=null && !j.notify.equals(""))
			sendNotificationMail(j);

	}
	
	private static void sendNotificationMail(final Job j){
		//send j.notify an info
		// TODO:
	}
	
	private static boolean setSubJobMerges(final Job j){
			
		// if ($info->{split}) {
		//	    $self->info("We have to check if all the subjobs of $info->{split} have finished");
		//	    $self->do(
		//	"insert  into JOBSTOMERGE (masterId) select ? from DUAL  where not exists (select masterid from JOBSTOMERGE where masterid = ?)",
		//	      {bind_values => [ $info->{split}, $info->{split} ]}
		//	    );
		//	    $self->do("update ACTIONS set todo=1 where action='MERGING'");
		//	  }
		final DBFunctions db = getQueueDB();
		
		if (monitor!=null){
			monitor.incrementCounter("TQ_db_lookup");
			monitor.incrementCounter("TQ_JOBSTOMERGE_lookup");
		}
		
		final String q = "INSERT INTO JOBSTOMERGE (masterId) SELECT "+ j.split +" FROM DUAL WHERE NOT EXISTS (SELECT masterid FROM JOBSTOMERGE WHERE masterid = "+ j.split+");";
					
		if (!db.query(q))
			return false;
				
		return setAction(JobStatus.MERGING);

	}
	
	private static boolean setAction(final JobStatus status){
		//$self->update("ACTIONS", {todo => 1}, "action='$status'");
		final DBFunctions db = getQueueDB();
		
		if (monitor!=null){
			monitor.incrementCounter("TQ_db_update");
			monitor.incrementCounter("TQ_ACTIONS_update");
		}
		
		final String q = "UPDATE ACTIONS SET todo=1 WHERE action='" + status.toSQL() + "' AND todo=0;";
	
		if (!db.query(q))
			return false;
			
		return true;
	}
	
	

	/**
	 * @param args
	 */
	public static void main(String[] args){
		System.out.println("QUEUE TESTING...");
		
		if(getAdminDB()==null)
			System.out.println("ADMIN DB NULL.");

		
		System.out.println("---------------------------------------------------------------------");
		
		if(insertJobToken(12341234, "me",true)==null)
			System.out.println("exists, update refused.");
		
		System.out.println("---------------------------------------------------------------------");
		
		if(insertJobToken(12341234, "me",true)==null)
			System.out.println("exists, update refused.");
		
		System.out.println("---------------------------------------------------------------------");

		deleteJobToken(12341234);
	
		
	}
	
	/**
	 * Get the number of jobs in the respective state
	 * 
	 * @param states
	 * @return the aggregated number of jobs per user
	 */
	public static Map<String, Integer> getJobCounters(final Set<JobStatus> states){
		final Map<String, Integer> ret = new TreeMap<String, Integer>();
		
		final DBFunctions db = getQueueDB();
		
		final StringBuilder sb = new StringBuilder();
		
		if (monitor!=null){
			monitor.incrementCounter("TQ_db_lookup");
		}
		
		if (states!=null && !states.contains(JobStatus.ANY)){
			for (final JobStatus s: states){
				if (sb.length()>0)
					sb.append(',');
				
				sb.append('\'').append(s.toSQL()).append('\'');
			}
		}
		
		String q = "select substring_index(submithost,'@',1),count(1) from QUEUE ";
		
		if (sb.length()>0)
			q += "where status in ("+sb+") ";
			
		q += "group by 1 order by 1;";
		
		db.query(q);
		
		while (db.moveNext()){
			ret.put(StringFactory.get(db.gets(1)), Integer.valueOf(db.geti(2)));
		}
		
		return ret;
	}
}
