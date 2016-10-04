package alien.site;

import apmon.ApMon;
import apmon.ApMonException;
import apmon.ApMonMonitoringConstants;
import apmon.BkThread;
import apmon.MonitoredJob;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.ProcessBuilder.Redirect;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.StringTokenizer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lazyj.Format;

import lia.util.Utils;

import org.sqlite.SQLiteConnection;

import alien.api.JBoxServer;
import alien.api.catalogue.CatalogueApiUtils;
import alien.api.taskQueue.GetMatchJob;
import alien.api.taskQueue.TaskQueueApiUtils;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.XmlCollection;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.io.Transfer;
import alien.io.protocols.Protocol;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.MonitoringObject;
import alien.se.SE;
import alien.shell.commands.JAliEnCOMMander;
import alien.site.packman.CVMFS;
import alien.site.packman.PackMan;
import alien.site.supercomputing.titan.FileDownloadApplication;
import alien.site.supercomputing.titan.FileDownloadController;
import alien.site.supercomputing.titan.Pair;
import alien.taskQueue.JDL;
import alien.taskQueue.Job;
import alien.taskQueue.JobStatus;
import alien.site.supercomputing.titan.TitanJobStatus;
import alien.site.supercomputing.titan.TitanBatchInfo;
import alien.site.supercomputing.titan.ProcInfoPair;
import alien.site.supercomputing.titan.JobUploader;

/**
 * @author mmmartin, ron, pavlo
 * @since Apr 1, 2015
 */


public class TitanJobService extends Thread implements MonitoringObject {

	// Folders and files
	private File tempDir = null;
	private static final String defaultOutputDirPrefix = "/jalien-job-";
	private String jobWorkdir = "";

	// Variables passed through VoBox environment
	private final Map<String, String> env = System.getenv();
	private final String site;
	private final String ce;
	private int origTtl;

	// Job variables
	//private JDL jdl = null;
	//private long queueId;
	private String jobToken;
	private String username;
	private String jobAgentId = "";
	private String globalWorkdir = null;
	//private HashMap<String, Object> matchedJob = null;
	private String partition;
	private String ceRequirements = "";
	private List<String> packages;
	private List<String> installedPackages;
	private ArrayList<String> extrasites;
	private HashMap<String, Object> siteMap = new HashMap<>();
	//private int workdirMaxSizeMB;
	//private int jobMaxMemoryMB;
	private int payloadPID;
	private MonitoredJob mj;
	private Double prevCpuTime;
	private long prevTime = 0;
	//private JobStatus jobStatus;

	private int totalJobs;
	private final long jobAgentStartTime = new java.util.Date().getTime();

	// Other
	private PackMan packMan = null;
	private String hostName = null;
	private String alienCm = null;
	private final int pid;
	private final JAliEnCOMMander commander = JAliEnCOMMander.getInstance();
	private final CatalogueApiUtils c_api = new CatalogueApiUtils(commander);
	private static final HashMap<String, Integer> jaStatus = new HashMap<>();

	static {
		jaStatus.put("REQUESTING_JOB", Integer.valueOf(1));
		jaStatus.put("INSTALLING_PKGS", Integer.valueOf(2));
		jaStatus.put("JOB_STARTED", Integer.valueOf(3));
		jaStatus.put("RUNNING_JOB", Integer.valueOf(4));
		jaStatus.put("DONE", Integer.valueOf(5));
		jaStatus.put("ERROR_HC", Integer.valueOf(-1)); // error in getting host classad
		jaStatus.put("ERROR_IP", Integer.valueOf(-2)); // error installing packages
		jaStatus.put("ERROR_GET_JDL", Integer.valueOf(-3)); // error getting jdl
		jaStatus.put("ERROR_JDL", Integer.valueOf(-4)); // incorrect jdl
		jaStatus.put("ERROR_DIRS", Integer.valueOf(-5)); // error creating directories, not enough free space in workdir
		jaStatus.put("ERROR_START", Integer.valueOf(-6)); // error forking to start job
	}

	private final int jobagent_requests = 1; // TODO: restore to 5

	static transient final Logger logger = ConfigUtils.getLogger(TitanJobService.class.getCanonicalName());

	static transient final Monitor monitor = MonitorFactory.getMonitor(TitanJobService.class.getCanonicalName());
	static transient final ApMon apmon = MonitorFactory.getApMonSender();

	// Resource monitoring vars

	private static final Double ZERO = Double.valueOf(0);

	private Double RES_WORKDIR_SIZE = ZERO;
	private Double RES_VMEM = ZERO;
	private Double RES_RMEM = ZERO;
	private Double RES_VMEMMAX = ZERO;
	private Double RES_RMEMMAX = ZERO;
	private Double RES_MEMUSAGE = ZERO;
	private Double RES_CPUTIME = ZERO;
	private Double RES_CPUUSAGE = ZERO;
	private String RES_RESOURCEUSAGE = "";
	private Long RES_RUNTIME = Long.valueOf(0);
	private String RES_FRUNTIME = "";
	private Integer RES_NOCPUS = Integer.valueOf(1);
	private String RES_CPUMHZ = "";
	private String RES_CPUFAMILY = "";

	// EXPERIMENTAL 
	// for ORNL Titan
	private String dbname;
	private String monitoring_dbname;
	private String dblink;
	private int numCores;

	TitanBatchController batchController; 

	class TitanBatchController {
		private HashMap<String, TitanBatchInfo> batchesInfo = new HashMap<>();
		private String globalWorkdir;
		private List<TitanJobStatus> idleRanks;

		private static final int minTtl = 300;

		public TitanBatchController(String global_work_dir){
			if(global_work_dir == null)
				throw new IllegalArgumentException("No global workdir specified");
			globalWorkdir = global_work_dir;
			idleRanks = new LinkedList<>();
		}

		public boolean updateDatabaseList(){
			//ls -d */ -1 | sed -e 's#/$##' | grep -E '^[0-9]+$' | sort -g	
			//ProcessBuilder pb = newProcessBuilder(System.getProperty("user.dir")+"/src/generate_list.sh",filename);
			System.out.println("Starting database update");
			ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", "for i in $(ls -d " 
						+ globalWorkdir + "/*/ | egrep \"/[0-9]+/\"); do basename $i; done");
			HashMap<String, TitanBatchInfo> tmpBatchesInfo = new HashMap<>();
			int dbcount = 0;
			try{
				Process p = pb.start();
				BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
				String line = null;
				while ( (line = reader.readLine()) != null) {
					//if(batchesInfo.get(line) ==  null)
						try{
							TitanBatchInfo bi = batchesInfo.get(line);
							if(bi==null)
								tmpBatchesInfo.put(line, 
										new TitanBatchInfo(Long.parseLong(line), 
											globalWorkdir + "/" + line));
							else
								tmpBatchesInfo.put(line, bi);
							dbcount++;
							System.out.println("Now controlling batch: " + line);
						}
						catch(InvalidParameterException e){
							System.err.println("Not a batch folder at " + 
									globalWorkdir + "/" + line + " , skipping....");
						}
						catch(Exception e){
							System.err.println(e.getMessage());
							System.err.println("Unable to initialize batch folder at " + 
									globalWorkdir + "/" + line + " , skipping....");
						}
				}
				batchesInfo = tmpBatchesInfo;
			}
			catch(IOException e){
				System.err.println("Error running batch info reader process: " + e.getMessage());
			}
			catch(Exception e){
				System.err.println("Exception at database list update: " + e.getMessage());
			}
			System.out.println(String.format("Now controlling %d batches", dbcount));

			return !batchesInfo.isEmpty();
		}

		public boolean queryDatabases(){
			idleRanks.clear();
			Long current_timestamp = System.currentTimeMillis() / 1000L;
			for(Object o : batchesInfo.values()){
				TitanBatchInfo bi = (TitanBatchInfo) o;
				System.out.println("Querying: " + bi.pbsJobId);
				if(!checkBatchTtlValid(bi, current_timestamp))
					continue;
				if(!bi.isRunning()){
					System.out.println("Batch " + bi.pbsJobId + " not running, cleaning up.");
					//bi.cleanup();
					//continue;
				}
				try{
					idleRanks.addAll(bi.getIdleRanks());
				}
				catch(Exception e){
					System.err.println("Exception caught in queryDatabases: " + e.getMessage());
					continue;
				}
			}
			return !idleRanks.isEmpty();
		}


		public List<TitanJobStatus> queryRunningDatabases(){
			List<TitanJobStatus> runningRanks = new LinkedList<>();
			Long current_timestamp = System.currentTimeMillis() / 1000L;
			for(Object o : batchesInfo.values()){
				TitanBatchInfo bi = (TitanBatchInfo) o;
				if(!checkBatchTtlValid(bi, current_timestamp))
					continue;
				try{
					runningRanks.addAll(bi.getRunningRanks());
				}
				catch(Exception e){
					continue;
				}
			}
			return runningRanks;
		}

		public final List<ProcInfoPair> getBatchesMonitoringData(){
			List<ProcInfoPair> l = new LinkedList<>();
			for(Object o : batchesInfo.values()){
				TitanBatchInfo bi = (TitanBatchInfo) o;
				l.addAll(bi.getMonitoringData());
			}
			return l;
		}

		private boolean checkBatchTtlValid(TitanBatchInfo bi, Long current_timestamp){
			// EXPERIMENTAL
			//return bi.getTtlLeft(current_timestamp) > minTtl;
			return bi.getTtlLeft(current_timestamp) > minTtl*8;
		}

		public void runDataExchange(){
			//List<TitanJobStatus> idleRanks = queryDatabases();
			//for(TitanJobStatus)
			int count = idleRanks.size();
			System.out.println(String.format("We can start %d jobs", count));
			
			if(count==0){
				return;
			}

			// create upload threads
			ArrayList<Thread> upload_threads = new ArrayList<>();
			for(TitanJobStatus js: idleRanks){
				if(js.status.equals("D")){
					JobUploader ju = new JobUploader(js);
					//ju.setDbName(dbname);
					upload_threads.add(ju);
					ju.start();
				}
			}
			
			// join all threads
			for(Thread t: upload_threads){
				try{
					t.join();
				}
				catch(InterruptedException e){
					System.err.println("Join for upload thread has been interrupted");
				}
			}

			System.out.println("Everything joined");
			System.out.println("================================================");

			//if(count>0) {
			//	monitor.sendParameter("ja_status", getJaStatusForML("REQUESTING_JOB"));
			//	monitor.sendParameter("TTL", siteMap.get("TTL"));
			//}

			upload_threads.clear();
			System.out.println("========= Starting download threads ==========");
			//while (count > 0) {
			int cnt = idleRanks.size();
			Date d1 = new Date();
			for(TitanJobStatus js: idleRanks){
				//System.out.println(siteMap.toString());
				//TitanJobStatus js = idleRanks.pop();

				JobDownloader jd = new JobDownloader(js, siteMap);
				//jd.setDbName(dbname);
				upload_threads.add(jd);
				jd.start();
				System.out.println("Starting downloader " + cnt--);
				if(cnt==0)
					break;
				//count--;
				//System.out.println("Wants to start Downloader thread");
				//System.out.println(js.batch.origTtl);
				//System.out.println(js.batch.numCores);
			}

			System.out.println(String.format("Count: %d", count));
			Date d3 = new Date();

			// join all threads
			for(Thread t: upload_threads){
				try{
					t.join();
					System.out.println("Joined downloader " + ++cnt);
				}
				catch(InterruptedException e){
					System.err.println("Join for upload thread has been interrupted");
				}
			}
			idleRanks.clear();
			Date d2 = new Date();
			System.out.println("Everything joined");
			System.out.println("Downloading took: " + (d2.getTime()-d1.getTime())/1000 + " seconds");
			System.out.println("Created downloaders during: " + 
					(d3.getTime()-d1.getTime())/1000 + " seconds");
			System.out.println("================================================");
		}
	}
	
	/**
	 */
	public TitanJobService() {
		site = env.get("site"); // or ConfigUtils.getConfig().gets("alice_close_site").trim();
		ce = env.get("CE");

		totalJobs = 0;

		partition = "";
		if (env.containsKey("partition"))
			partition = env.get("partition");

		if (env.containsKey("TTL"))
			origTtl = Integer.parseInt(env.get("TTL"));
		else
			origTtl = 12 * 3600;

		if (env.containsKey("cerequirements"))
			ceRequirements = env.get("cerequirements");

		if (env.containsKey("closeSE"))
			extrasites = new ArrayList<>(Arrays.asList(env.get("closeSE").split(",")));

		try {
			hostName = InetAddress.getLocalHost().getCanonicalHostName();
		} catch (final UnknownHostException e) {
			System.err.println("Couldn't get hostname");
			e.printStackTrace();
		}

		alienCm = hostName;
		if (env.containsKey("ALIEN_CM_AS_LDAP_PROXY"))
			alienCm = env.get("ALIEN_CM_AS_LDAP_PROXY");

		if (env.containsKey("ALIEN_JOBAGENT_ID"))
			jobAgentId = env.get("ALIEN_JOBAGENT_ID");
		pid = Integer.parseInt(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);

		globalWorkdir = env.get("HOME");
		if (env.containsKey("WORKDIR"))
			globalWorkdir = env.get("WORKDIR");
		if (env.containsKey("TMPBATCH"))
			globalWorkdir = env.get("TMPBATCH");

		siteMap = getSiteParameters();

		Hashtable<Long, String> cpuinfo;
		try {
			cpuinfo = BkThread.getCpuInfo();
			RES_CPUFAMILY = cpuinfo.get(ApMonMonitoringConstants.LGEN_CPU_FAMILY);
			RES_CPUMHZ = cpuinfo.get(ApMonMonitoringConstants.LGEN_CPU_MHZ);
			RES_CPUMHZ = RES_CPUMHZ.substring(0, RES_CPUMHZ.indexOf("."));
			RES_NOCPUS = Integer.valueOf(BkThread.getNumCPUs());

			System.out.println("CPUFAMILY: " + RES_CPUFAMILY);
			System.out.println("CPUMHZ: " + RES_CPUMHZ);
			System.out.println("NOCPUS: " + RES_NOCPUS);
		} catch (final IOException e) {
			System.out.println("Problem with the monitoring objects IO Exception: " + e.toString());
		} catch (final ApMonException e) {
			System.out.println("Problem with the monitoring objects ApMon Exception: " + e.toString());
		}

		monitor.addMonitoring("jobAgent-TODO", this);

		batchController = new TitanBatchController(globalWorkdir);
		FileDownloadController.setCacheFolder("/lustre/atlas/proj-shared/csc108/psvirin/catalog_cache");
		
		JobUploader.ce = ce;
		JobUploader.hostName = hostName;
		JobUploader.defaultOutputDirPrefix = defaultOutputDirPrefix;

		JobDownloader.ce = ce;
		JobDownloader.hostName = hostName;
		JobDownloader.defaultOutputDirPrefix = defaultOutputDirPrefix;

		// here create monitor thread
		class TitanMonitorThread extends Thread {
			private TitanBatchController tbc;

			public TitanMonitorThread(TitanBatchController tbc){
				this.tbc = tbc;
			}

			private void sendProcessResources() {
				//List<ProcInfoPair> job_resources = new LinkedList<ProcInfoPair>();
				List<ProcInfoPair> job_resources = tbc.getBatchesMonitoringData();

				/*
				try{
					// open db
					Connection connection = DriverManager.getConnection(monitoring_dbname);
					Statement statement = connection.createStatement();
					ResultSet rs = statement.executeQuery("SELECT * FROM alien_jobs_monitoring");
					// read all
					while(rs.next()){
						job_resources.add(new ProcInfoPair( rs.getString("queue_id"), rs.getString("resources")));
						//idleRanks.add(new TitanJobStatus(rs.getInt("rank"), rs.getLong("queue_id"), rs.getString("job_folder"),
						//		rs.getString("status"), rs.getInt("exec_code"), rs.getInt("val_code")));
					}
					// delete all
					statement.executeUpdate("DELETE FROM alien_jobs_monitoring");
					// close database
					connection.close();
				}
				catch(SQLException e){
					System.err.println("Unable to get monitoring data: " + e.getMessage());
				}
				*/
				// foreach send

				// runtime(date formatted) start cpu(%) mem cputime rsz vsize ncpu cpufamily cpuspeed resourcecost maxrss maxvss ksi2k
				//final String procinfo = String.format("%s %d %.2f %.2f %.2f %.2f %.2f %d %s %s %s %.2f %.2f 1000", RES_FRUNTIME, RES_RUNTIME, RES_CPUUSAGE, RES_MEMUSAGE, RES_CPUTIME, RES_RMEM, RES_VMEM,
						//RES_NOCPUS, RES_CPUFAMILY, RES_CPUMHZ, RES_RESOURCEUSAGE, RES_RMEMMAX, RES_VMEMMAX);
				//System.out.println("+++++ Sending resources info +++++");
				//System.out.println(procinfo);

				// create pool of 16 thread
				for(ProcInfoPair pi: job_resources){
					// notify to all processes waiting
					commander.q_api.putJobLog(pi.queue_id, "proc", pi.procinfo);
				}

				// ApMon calls
				System.out.println("Running periodic Apmon update on running jobs");
				List<TitanJobStatus> runningJobs = tbc.queryRunningDatabases();
				for(TitanJobStatus pi: runningJobs){
					/*final HashMap<String, Object> extrafields = new HashMap<>();
					extrafields.put("spyurl", hostName + ":" + JBoxServer.getPort());
					extrafields.put("node", hostName);
					TaskQueueApiUtils.setJobStatus(pi.queueId, JobStatus.RUNNING, extrafields);*/

					System.out.println("Running ApMon update for PID: " + pi.queueId);
					Vector<String> varnames = new Vector<>();
					varnames.add("host");
					varnames.add("statusID");
					varnames.add("jobID");
					varnames.add("job_user");
					varnames.add("masterjob_id");
					varnames.add("host_pid");
					varnames.add("exechost");
					Vector<Object> varvalues = new Vector<>();
					varvalues.add(hostName);
					varvalues.add(10);
					varvalues.add(Double.valueOf(pi.queueId));
					varvalues.add("psvirin");
					varvalues.add(0);
					varvalues.add(10000);
					//varvalues.add(ce);
					varvalues.add(hostName);
					try{
						//apmon.sendParameters(ce+"_Jobs", String.format("%d",pi.queueId), 6, varnames, varvalues);
						apmon.sendParameters("TaskQueue_Jobs_ALICE", 
									String.format("%d",pi.queueId), 
									6, varnames, varvalues);
					}
					catch(ApMonException e){
						System.out.println("Apmon exception: " + e.getMessage());
					}
					catch(UnknownHostException e){
						System.out.println("Unknown host exception");
					}

					catch(SocketException e){
						System.out.println("Socket exception");
					}

					catch(IOException e){
						System.out.println("IO exception");
					}

					// notify to all processes waiting
					//commander.q_api.putJobLog(pi.queue_id, "proc", pi.procinfo);
				}
			}

			public void run() {
				// here create a pool of 16 sending processes
				
				while(true){
					try{
						Thread.sleep(1*60*1000);
					}
					catch(InterruptedException e){}
					sendProcessResources();
				}
			}
		}

		new TitanMonitorThread(batchController).start();
		// END EXPERIMENTAL
	}

	@Override
	public void run() {

		logger.log(Level.INFO, "Starting JobAgent in " + hostName);

		// We start, if needed, the node JBox
		// Does it check a previous one is already running?
		try {
			System.out.println("Trying to start JBox");
			JBoxServer.startJBoxService(0);
		} catch (final Exception e) {
			System.err.println("Unable to start JBox.");
			e.printStackTrace();
		}

		while(true){ 
			System.out.println("========================");
			System.out.println("Entering round");
			System.out.println("Updating bunches information");
			if(!batchController.updateDatabaseList()){
				System.out.println("No batches, sleeping.");
				roundSleep();
				continue;
			}

			if(batchController.queryDatabases()){
				System.out.println("Now running jobs exchange");
				monitor.sendParameter("ja_status", getJaStatusForML("REQUESTING_JOB"));
				monitor.sendParameter("TTL", siteMap.get("TTL"));
				batchController.runDataExchange();
			}

			if (!updateDynamicParameters()){
				System.err.println("update for dynamic parameters failed. Stopping the agent.");
				break;
			}


			/*
			LinkedList<TitanJobStatus> idleRanks = new LinkedList<TitanJobStatus>();
			try{
				Connection connection = DriverManager.getConnection(dbname);
				Statement statement = connection.createStatement();
				ResultSet rs = statement.executeQuery("SELECT rank, queue_id, job_folder, status, exec_code, val_code FROM alien_jobs WHERE status='D' OR status='I'");
				while(rs.next()){
					idleRanks.add(new TitanJobStatus(rs.getInt("rank"), rs.getLong("queue_id"), rs.getString("job_folder"), 
										rs.getString("status"), rs.getInt("exec_code"), rs.getInt("val_code")));
				}
				
				connection.close();
			} catch(SQLException e){
				System.err.println("Getting free slots failed: " + e.getMessage());
				continue;
			}
			*/
			/*
			int count = idleRanks.size();
			System.out.println(String.format("We can start %d jobs", count));
			
			if(count==0){
				try{
					Thread.sleep(10000);
				}
				catch(InterruptedException e){}
				finally{
					System.out.println("Going for the next round....");
				}
				continue;
			}

			// create upload threads
			ArrayList<Thread> upload_threads = new ArrayList<>();
			for(TitanJobStatus js: idleRanks){
				if(js.status.equals("D")){
					JobUploader ju = new JobUploader(js);
					ju.setDbName(dbname);
					upload_threads.add(ju);
					ju.start();
				}
			}
			
			// join all threads
			for(Thread t: upload_threads){
				try{
					t.join();
				}
				catch(InterruptedException e){
					System.err.println("Join for upload thread has been interrupted");
				}
			}

			System.out.println("Everything joined");
			System.out.println("================================================");
			
			if(count>0) {
				monitor.sendParameter("ja_status", getJaStatusForML("REQUESTING_JOB"));
				monitor.sendParameter("TTL", siteMap.get("TTL"));
			}

			upload_threads.clear();
			System.out.println("========= Starting download threads ==========");
			while (count > 0) {
				System.out.println(siteMap.toString());
				TitanJobStatus js = idleRanks.pop();
				JobDownloader jd = new JobDownloader(js);
				jd.setDbName(dbname);
				upload_threads.add(jd);
				jd.start();
				count--;
			}

			// join all threads
			for(Thread t: upload_threads){
				try{
					t.join();
				}
				catch(InterruptedException e){
					System.err.println("Join for upload thread has been interrupted");
				}
			}
			*/
			System.out.println("=========== Round finished =========");
			roundSleep();

		}

		logger.log(Level.INFO, "JobAgent finished, id: " + jobAgentId + " totalJobs: " + totalJobs);
		System.exit(0);
	}

	// =========================================================================================================
	// ================ run finished

	private void roundSleep(){
		try{
			sleep(60000);
		}
		catch(InterruptedException e){
			System.err.println("Sleep after full JA cycle failed: " + e.getMessage());
		}
	}


	private static Integer getJaStatusForML(final String status) {
		final Integer value = jaStatus.get(status);

		return value != null ? value : Integer.valueOf(0);
	}

	/**
	 * @return the site parameters to send to the job broker (packages, ttl, ce/site...)
	 */
	private HashMap<String, Object> getSiteParameters() {
		logger.log(Level.INFO, "Getting jobAgent map");

		// getting packages from PackMan
		packMan = getPackman();
		packages = packMan.getListPackages();
		installedPackages = packMan.getListInstalledPackages();

		// get users from cerequirements field
		final ArrayList<String> users = new ArrayList<>();
		if (!ceRequirements.equals("")) {
			final Pattern p = Pattern.compile("\\s*other.user\\s*==\\s*\"(\\w+)\"");
			final Matcher m = p.matcher(ceRequirements);
			while (m.find())
				users.add(m.group(1));
		}
		// setting entries for the map object
		siteMap.put("TTL", Integer.valueOf(origTtl));

		// We prepare the packages for direct matching
		String packs = ",";
		Collections.sort(packages);
		for (final String pack : packages)
			packs += pack + ",,";

		packs = packs.substring(0, packs.length() - 1);

		String instpacks = ",";
		Collections.sort(installedPackages);
		for (final String pack : installedPackages)
			instpacks += pack + ",,";

		instpacks = instpacks.substring(0, instpacks.length() - 1);

		siteMap.put("Platform", ConfigUtils.getPlatform());
		siteMap.put("Packages", packs);
		siteMap.put("InstalledPackages", instpacks);
		siteMap.put("CE", ce);
		siteMap.put("Site", site);
		if (users.size() > 0)
			siteMap.put("Users", users);
		if (extrasites != null && extrasites.size() > 0)
			siteMap.put("Extrasites", extrasites);
		siteMap.put("Host", alienCm);
		siteMap.put("Disk", Long.valueOf(new File(globalWorkdir).getFreeSpace() / 1024));

		if (!partition.equals(""))
			siteMap.put("Partition", partition);

		return siteMap;
	}

	private PackMan getPackman() {
		switch (env.get("installationMethod")) {
		case "CVMFS":
			siteMap.put("CVMFS", Integer.valueOf(1));
			return new CVMFS();
		default:
			siteMap.put("CVMFS", Integer.valueOf(1));
			return new CVMFS();
		}
	}

	/**
	 * updates jobagent parameters that change between job requests
	 *
	 * @return false if we can't run because of current conditions, true if positive
	 */
	private boolean updateDynamicParameters() {
		logger.log(Level.INFO, "Updating dynamic parameters of jobAgent map");

		// free disk recalculation
		final long space = new File(globalWorkdir).getFreeSpace() / 1024;

		// ttl recalculation
		final long jobAgentCurrentTime = new java.util.Date().getTime();
		//final int time_subs = (int) (jobAgentCurrentTime - jobAgentStartTime);
		final long time_subs = (long) (jobAgentCurrentTime - jobAgentStartTime);
		//int timeleft = origTtl - time_subs - 300;
		long timeleft = origTtl*1000 - time_subs - 300*1000;

		logger.log(Level.INFO, "Still have " + timeleft + " seconds to live (" + 
				jobAgentCurrentTime + "-" + jobAgentStartTime + "=" + time_subs + ")");

		// we check if the proxy timeleft is smaller than the ttl itself
		final int proxy = getRemainingProxyTime();
		logger.log(Level.INFO, "Proxy timeleft is " + proxy);
		//if (proxy > 0 && proxy < timeleft)
		if (proxy > 0 && proxy*1000 < timeleft)
			timeleft = proxy;

		// safety time for saving, etc
		timeleft -= 300;

		// what is the minimum we want to run with? (100MB?)
		if (space <= 100 * 1024 * 1024) {
			logger.log(Level.INFO, "There is not enough space left: " + space);
			return false;
		}

		// EXPERIMENTAL
		// for ORNL Titan
		/*
		if (timeleft <= 0) {
			logger.log(Level.INFO, "There is not enough time left: " + timeleft);
			return false;
		}
		*/

		siteMap.put("Disk", Long.valueOf(space));
		//siteMap.put("TTL", Integer.valueOf(timeleft));
		siteMap.put("TTL", Long.valueOf(timeleft/1000));

		return true;
	}

	/**
	 * @return the time in seconds that proxy is still valid for
	 */
	private int getRemainingProxyTime() {
		// TODO: to be modified!
		return origTtl;
	}



	/**
	 * @param command
	 * @param arguments
	 * @param timeout
	 * @return <cod>0</code> if everything went fine, a positive number with the process exit code (which would mean a problem) and a negative error code in case of timeout or other supervised
	 *         execution errors
	 */
	/*private int executeCommand(final String command, final List<String> arguments, final long timeout, final TimeUnit unit, final boolean monitorJob) {
		final List<String> cmd = new LinkedList<>();

		final int idx = command.lastIndexOf('/');

		final String cmdStrip = idx < 0 ? command : command.substring(idx + 1);

		final File fExe = new File(tempDir, cmdStrip);

		if (!fExe.exists())
			return -1;

		fExe.setExecutable(true);

		cmd.add(fExe.getAbsolutePath());

		if (arguments != null && arguments.size() > 0)
			for (final String argument : arguments)
				if (argument.trim().length() > 0) {
					final StringTokenizer st = new StringTokenizer(argument);

					while (st.hasMoreTokens())
						cmd.add(st.nextToken());
				}

		System.err.println("Executing: " + cmd + ", arguments is " + arguments + " pid: " + pid);

		// EXPERIMENTAL
		//pBuilder = new ProcessBuilder(cmd);
		ProcessBuilder pBuilder = new ProcessBuilder("sleep", "200");

		pBuilder.directory(tempDir);

		final HashMap<String, String> environment_packages = getJobPackagesEnvironment();
		final Map<String, String> processEnv = pBuilder.environment();
		processEnv.putAll(environment_packages);
		processEnv.putAll(loadJDLEnvironmentVariables());

		pBuilder.redirectOutput(Redirect.appendTo(new File(tempDir, "stdout")));
		pBuilder.redirectError(Redirect.appendTo(new File(tempDir, "stderr")));
		// pBuilder.redirectErrorStream(true);

		final Process p;

		try {
			changeStatus(JobStatus.RUNNING);

			p = pBuilder.start();
		} catch (final IOException ioe) {
			System.out.println("Exception running " + cmd + " : " + ioe.getMessage());
			return -2;
		}

		final Timer t = new Timer();
		t.schedule(new TimerTask() {
			@Override
			public void run() {
				p.destroy();
			}
		}, TimeUnit.MILLISECONDS.convert(timeout, unit));

		mj = new MonitoredJob(pid, jobWorkdir, ce, hostName);
		final Vector<Integer> child = mj.getChildren();
		if (child == null || child.size() <= 1) {
			System.err.println("Can't get children. Failed to execute? " + cmd.toString() + " child: " + child);
			return -1;
		}
		System.out.println("Child: " + child.get(1).toString());

		boolean processNotFinished = true;
		int code = 0;

		if (monitorJob) {
			payloadPID = child.get(1).intValue();
			apmon.addJobToMonitor(payloadPID, jobWorkdir, ce, hostName); // TODO: test
			mj = new MonitoredJob(payloadPID, jobWorkdir, ce, hostName);
			checkProcessResources();
			sendProcessResources();
		}

		int monitor_loops = 0;
		try {
			while (processNotFinished)
				try {
					Thread.sleep(60 * 1000);
					code = p.exitValue();
					processNotFinished = false;
				} catch (final IllegalThreadStateException e) {
					// TODO: check job-token exist (job not killed)

					// process hasn't terminated
					if (monitorJob) {
						monitor_loops++;
						final String error = checkProcessResources();
						if (error != null) {
							p.destroy();
							System.out.println("Process overusing resources: " + error);
							return -2;
						}
						if (monitor_loops == 10) {
							monitor_loops = 0;
							sendProcessResources();
						}
					}
				}
			return code;
		} catch (final InterruptedException ie) {
			System.out.println("Interrupted while waiting for this command to finish: " + cmd.toString());
			return -2;
		} finally {
			t.cancel();
		}
	}   */


	/*
	private String checkProcessResources() {
		String error = null;
		// EXPERIMENTAL
		// for ORNL Titan
		System.out.println("Checking resources usage");

		try {
			final HashMap<Long, Double> jobinfo = mj.readJobInfo();
			final HashMap<Long, Double> diskinfo = mj.readJobDiskUsage();

			// gettng cpu, memory and runtime info
			RES_WORKDIR_SIZE = diskinfo.get(ApMonMonitoringConstants.LJOB_WORKDIR_SIZE);
			RES_VMEM = Double.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_VIRTUALMEM).doubleValue() / 1024);
			RES_RMEM = Double.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_RSS).doubleValue() / 1024);
			RES_CPUTIME = Double.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_CPU_TIME).doubleValue());
			RES_CPUUSAGE = Double.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_CPU_USAGE).doubleValue());
			RES_RUNTIME = Long.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_RUN_TIME).longValue());
			RES_MEMUSAGE = jobinfo.get(ApMonMonitoringConstants.LJOB_MEM_USAGE);
			RES_RESOURCEUSAGE = Format.showDottedDouble(RES_CPUTIME.doubleValue() * Double.parseDouble(RES_CPUMHZ) / 1000, 2);

			//RES_WORKDIR_SIZE = 0;
			//RES_VMEM = 0;
			//RES_RMEM = 0;
			//RES_CPUTIME = 0;
			//RES_CPUUSAGE = 0;
			//RES_RUNTIME = 0;
			//RES_MEMUSAGE = 0;
			//RES_RESOURCEUSAGE = 0;


			// max memory consumption
			if (RES_RMEM.doubleValue() > RES_RMEMMAX.doubleValue())
				RES_RMEMMAX = RES_RMEM;

			if (RES_VMEM.doubleValue() > RES_VMEMMAX.doubleValue())
				RES_VMEMMAX = RES_VMEM;

			// formatted runtime
			if (RES_RUNTIME.doubleValue() < 60)
				RES_FRUNTIME = String.format("00:00:%02d", RES_RUNTIME);
			else if (RES_RUNTIME.doubleValue() < 3600){
				System.out.println(RES_RUNTIME.doubleValue()/60);
				System.out.println(Double.valueOf(RES_RUNTIME.doubleValue() % 60));
				//RES_FRUNTIME = String.format("00:%02d:%02d", Double.valueOf(RES_RUNTIME.doubleValue() / 60), Double.valueOf(RES_RUNTIME.doubleValue() % 60));
				RES_FRUNTIME = String.format("00:%02d:%02d", RES_RUNTIME / 60, RES_RUNTIME % 60);
			}
			else
				RES_FRUNTIME = String.format("%02d:%02d:%02d", Double.valueOf(RES_RUNTIME.doubleValue() / 3600),
						Double.valueOf((RES_RUNTIME.doubleValue() - (RES_RUNTIME.doubleValue() / 3600) * 3600) / 60),
						Double.valueOf((RES_RUNTIME.doubleValue() - (RES_RUNTIME.doubleValue() / 3600) * 3600) % 60));

			// check disk usage
			if (workdirMaxSizeMB != 0 && RES_WORKDIR_SIZE.doubleValue() > workdirMaxSizeMB)
				error = "Disk space limit is " + workdirMaxSizeMB + ", using " + RES_WORKDIR_SIZE;

			// check disk usage
			if (jobMaxMemoryMB != 0 && RES_VMEM.doubleValue() > jobMaxMemoryMB)
				error = "Memory usage limit is " + jobMaxMemoryMB + ", using " + RES_VMEM;

			// cpu
			final long time = System.currentTimeMillis();

			if (prevTime != 0 && prevTime + (20 * 60 * 1000) < time && RES_CPUTIME == prevCpuTime)
				error = "The job hasn't used the CPU for 20 minutes";
			else {
				prevCpuTime = RES_CPUTIME;
				prevTime = time;
			}

		} catch (final IOException e) {
			System.out.println("Problem with the monitoring objects: " + e.toString());
		}

		return error;
	}
	*/


	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {
		apmon.setLogLevel("DEBUG");
		final TitanJobService ja = new TitanJobService();
		ja.run();
	}


	/**
	 * @param newStatus
	 */
	/* public void changeStatus(final Long queueId, final JobStatus newStatus) {
		// if final status with saved files, we set the path
		if (newStatus == JobStatus.DONE || newStatus == JobStatus.DONE_WARN || newStatus == JobStatus.ERROR_E || newStatus == JobStatus.ERROR_V) {
			final HashMap<String, Object> extrafields = new HashMap<>();
			extrafields.put("path", getJobOutputDir());

			TaskQueueApiUtils.setJobStatus(queueId, newStatus, extrafields);
		} else if (newStatus == JobStatus.RUNNING) {
			final HashMap<String, Object> extrafields = new HashMap<>();
			extrafields.put("spyurl", hostName + ":" + JBoxServer.getPort());
			extrafields.put("node", hostName);

			TaskQueueApiUtils.setJobStatus(queueId, newStatus, extrafields);
		} else
			TaskQueueApiUtils.setJobStatus(queueId, newStatus);

		jobStatus = newStatus;

		return;
	}
	*/


	@Override
	public void fillValues(final Vector<String> paramNames, final Vector<Object> paramValues) {
		Long queueId = 0L;
		if (queueId > 0) {
			paramNames.add("jobID");
			paramValues.add(Double.valueOf(queueId));

			// EXPERIMENTAL
			// temporarily commented out
			//paramNames.add("statusID");
			//paramValues.add(Double.valueOf(jobStatus.getAliEnLevel()));
		}
	}

}
