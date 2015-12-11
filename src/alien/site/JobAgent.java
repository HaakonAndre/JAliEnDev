package alien.site;

import java.io.File;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lia.util.Utils;
import alien.api.JBoxServer;
import alien.api.catalogue.CatalogueApiUtils;
import alien.api.taskQueue.GetMatchJob;
import alien.api.taskQueue.TaskQueueApiUtils;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.io.Transfer;
import alien.io.protocols.Protocol;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.shell.commands.JAliEnCOMMander;
import alien.site.packman.CVMFS;
import alien.site.packman.PackMan;
import alien.taskQueue.JDL;
import alien.taskQueue.Job;
import alien.taskQueue.JobStatus;
import apmon.ApMon;
import apmon.ApMonMonitoringConstants;
import apmon.MonitoredJob;

/**
 * @author mmmartin, ron
 * @since Apr 1, 2015
 */
public class JobAgent extends Thread {

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
	private JDL jdl = null;
	private int queueId;
	private String jobToken;
	private String username;
	private String jobAgentId = "";
	private String workdir = null;
	private HashMap<String, Object> matchedJob = null;
	private String partition;
	private String ceRequirements = "";
	private List<String> packages;
	private List<String> installedPackages;
	private ArrayList<String> extrasites;
	private HashMap<String, Object> siteMap = new HashMap<>();
	private int workdirMaxSizeMB;
	private int jobMaxMemoryMB;
	private int payloadPID;
	private MonitoredJob mj;
	private Double prevCpuTime;
	private long prevTime = 0;
	private JobStatus jobStatus;

	private int totalJobs;
	private final long jobAgentStartTime = new java.util.Date().getTime();

	// Other
	private PackMan packMan = null;
	private String hostName = null;
	private String alienCm = null;
	private int pid;
	private final JAliEnCOMMander commander = JAliEnCOMMander.getInstance();
	private final CatalogueApiUtils c_api = new CatalogueApiUtils(commander);
	private HashMap<String, Integer> jaStatus = new HashMap<>();
	private int jobagent_requests = 1; // TODO: restore to 5


	static transient final Logger logger = ConfigUtils.getLogger(JobAgent.class.getCanonicalName());

	static transient final Monitor monitor 	= MonitorFactory.getMonitor(JobAgent.class.getCanonicalName());
	static transient final ApMon apmon 		= MonitorFactory.getApMonSender();
	
	/**
	 */
	public JobAgent() {
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
		
		workdir = env.get("HOME");
		if (env.containsKey("WORKDIR"))
			workdir = env.get("WORKDIR");
		if (env.containsKey("TMPBATCH"))
			workdir = env.get("TMPBATCH");

		siteMap = getSiteParameters();
		
		jaStatus.put("REQUESTING_JOB",	1);
		jaStatus.put("INSTALLING_PKGS",	2);
		jaStatus.put("JOB_STARTED",		3);
		jaStatus.put("RUNNING_JOB",		4);
		jaStatus.put("DONE",			5);
		jaStatus.put("ERROR_HC",		-1); // error in getting host classad
		jaStatus.put("ERROR_IP",		-2); // error installing packages
		jaStatus.put("ERROR_GET_JDL",	-3); // error getting jdl
		jaStatus.put("ERROR_JDL",		-4); // incorrect jdl
		jaStatus.put("ERROR_DIRS",		-5); // error creating directories, not enough free space in workdir
		jaStatus.put("ERROR_START",		-6); // error forking to start job	
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

		int count = jobagent_requests;
		while (count > 0) {
			if (!updateDynamicParameters())
				break;

			System.out.println(siteMap.toString());

			try {
				logger.log(Level.INFO, "Trying to get a match...");
				
				monitor.sendParameter("ja_status", getJaStatusForML("REQUESTING_JOB"));
				monitor.sendParameter("TTL", siteMap.get("TTL"));

				final GetMatchJob jobMatch = commander.q_api.getMatchJob(siteMap);
				matchedJob = jobMatch.getMatchJob();
				
				// TODELETE
				if(matchedJob != null)
					System.out.println(matchedJob.toString());

				if ( matchedJob != null && !matchedJob.containsKey("Error") ) {
					jdl 		= new JDL( Job.sanitizeJDL((String) matchedJob.get("JDL")) );
					queueId 	= (int)	matchedJob.get("queueId");
					username 	= (String) matchedJob.get("User");
					jobToken 	= (String)	matchedJob.get("jobToken");
					
					//TODO: commander.setUser(username); commander.setSite(site);
					
					System.out.println(jdl.getExecutable());
					System.out.println(username);
					System.out.println(queueId);
					System.out.println(jobToken);
					
					// process payload
					handleJob();
															
					cleanup();
				} else {
					if (matchedJob != null && matchedJob.containsKey("Error")){
						logger.log(Level.INFO, (String) matchedJob.get("Error"));
					
						if( (int) matchedJob.get("Code") == -3 ){
							ArrayList<String> packToInstall = (ArrayList<String>) matchedJob.get("Packages");
							monitor.sendParameter("ja_status", getJaStatusForML("INSTALLING_PKGS"));
							installPackages(packToInstall);
						}
					}
					else
						logger.log(Level.INFO, "We didn't get anything back. Nothing to run right now. Idling 20secs zZz...");
					
					try {
						// TODO?: monitor.sendBgMonitoring
						sleep(20000);
					} catch (final InterruptedException e) {
						e.printStackTrace();
					}
				}
			} catch (final Exception e) {
				logger.log(Level.INFO, "Error getting a matching job: " + e);
			}
			count--;
		}

		logger.log(Level.INFO, "JobAgent finished, id: " + jobAgentId + " totalJobs: " + totalJobs);
		System.exit(0);
	}

	private void cleanup() {
		System.out.println("Cleaning up after execution...Removing sandbox: "+jobWorkdir);
		// Remove sandbox, TODO: use Java builtin
		Utils.getOutput("rm -rf "+jobWorkdir);
	}

	private Map<String,String> installPackages(ArrayList<String> packToInstall) {
		Map<String,String> ok = null;
		
		for (String pack : packToInstall){
			ok = packMan.installPackage(username,pack,null);
			if(ok == null){
				logger.log(Level.INFO, "Error installing the package "+pack);
			    monitor.sendParameter("ja_status", "ERROR_IP");
			    System.out.println("Error installing " + pack);
			    System.exit(1);
			}
		}
		return ok;
	}

	private Integer getJaStatusForML (String status){
		if (jaStatus.containsKey(status))
			return jaStatus.get(status);
		return 0;
	}

	/**
	 * @return the site parameters to send to the job broker (packages, ttl, ce/site...)
	 */
	private HashMap<String, Object> getSiteParameters() {
		logger.log(Level.INFO, "Getting jobAgent map");

		// getting packages from PackMan
		packMan = (PackMan) getPackman();
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
		if(users.size()>0)
			siteMap.put("Users", users);
		if(extrasites != null && extrasites.size()>0)
			siteMap.put("Extrasites", extrasites);
		siteMap.put("Host", alienCm);
		siteMap.put("Disk", Long.valueOf(new File(workdir).getFreeSpace() / 1024));

		if (!partition.equals(""))
			siteMap.put("Partition", partition);

		return siteMap;
	}

	private PackMan getPackman() {		
		switch( env.get("installationMethod") ){
			case "CVMFS":
				siteMap.put("CVMFS", 1);
				return new CVMFS();
			default:
				siteMap.put("CVMFS", 1);
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
		final long space = new File(workdir).getFreeSpace() / 1024;

		// ttl recalculation
		final long jobAgentCurrentTime = new java.util.Date().getTime();
		final int time_subs = (int) (jobAgentCurrentTime - jobAgentStartTime);
		int timeleft = origTtl - time_subs;

		logger.log(Level.INFO, "Still have " + timeleft + " seconds to live (" + jobAgentCurrentTime + "-" + jobAgentStartTime + "=" + time_subs + ")");

		// we check if the proxy timeleft is smaller than the ttl itself
		final int proxy = getRemainingProxyTime();
		logger.log(Level.INFO, "Proxy timeleft is " + proxy);
		if (proxy > 0 && proxy < timeleft)
			timeleft = proxy;

		// safety time for saving, etc
		timeleft -= 300;

		// what is the minimum we want to run with? (100MB?)
		if (space <= 100 * 1024 * 1024) {
			logger.log(Level.INFO, "There is not enough space left: " + space);
			return false;
		}

		if (timeleft <= 0) {
			logger.log(Level.INFO, "There is not enough time left: " + timeleft);
			return false;
		}

		siteMap.put("Disk", Long.valueOf(space));
		siteMap.put("TTL", Integer.valueOf(timeleft));

		return true;
	}

	/**
	 * @return the time in seconds that proxy is still valid for
	 */
	private int getRemainingProxyTime() {
		// to be modified!
		return origTtl;
	}

	private void handleJob() {
		totalJobs++;
		try {
			logger.log(Level.INFO, "Started JA with: " + jdl);
			
			TaskQueueApiUtils.setJobStatus(queueId, JobStatus.STARTED);
			jobStatus = JobStatus.STARTED;

			if (!createWorkDir() || !getInputFiles()) {
				TaskQueueApiUtils.setJobStatus(queueId, JobStatus.ERROR_IB);
				jobStatus = JobStatus.ERROR_IB;
				return;
			}
			
			getMemoryRequirements();
			
			// run payload
			if(execute() < 0){
				TaskQueueApiUtils.setJobStatus(queueId, JobStatus.ERROR_E);
				jobStatus = JobStatus.ERROR_E;
			}
				
			if (!validate()){
				TaskQueueApiUtils.setJobStatus(queueId, JobStatus.ERROR_V);
				jobStatus = JobStatus.ERROR_V;

			}
			
			if (jobStatus == JobStatus.RUNNING){
				TaskQueueApiUtils.setJobStatus(queueId, JobStatus.SAVING);
				jobStatus = JobStatus.SAVING;
			}
			
			uploadOutputFiles();

		} catch (final Exception e) {
			System.err.println("Unable to handle job");
			e.printStackTrace();
		}
	}


	/**
	 * @param command
	 * @param arguments
	 * @param timeout
	 * @return <code>0</code> if everything went fine, a positive number with the process exit code (which would mean a problem) and a negative error code in case of timeout or other supervised
	 *         execution errors
	 */
	private int executeCommand(final String command, final List<String> arguments, final long timeout, final TimeUnit unit) {
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

		System.err.println("Executing: " + cmd + ", arguments is " + arguments+" pid: "+pid);

		final ProcessBuilder pBuilder = new ProcessBuilder(cmd);
		
		pBuilder.directory(tempDir);
		
		HashMap<String, String> environment_packages = getJobPackagesEnvironment();
		Map<String, String> processEnv = pBuilder.environment();
		processEnv.putAll(environment_packages);
		processEnv.putAll( loadJDLEnvironmentVariables() );

		pBuilder.redirectOutput(Redirect.appendTo(new File(tempDir, "stdout")));
		pBuilder.redirectError(Redirect.appendTo(new File(tempDir, "stderr")));
//		pBuilder.redirectErrorStream(true);

		final Process p;

		try {
			TaskQueueApiUtils.setJobStatus(queueId, JobStatus.RUNNING);
			jobStatus = JobStatus.RUNNING;
			
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
		Vector<Integer> child = mj.getChildren();
		if (child == null || child.size() <= 1){
			System.err.println("Can't get children. Failed to execute? "+cmd.toString()+" child: "+child.toString());
			return -1;
		}
		System.out.println("Child: "+child.get(1).toString());
		
		boolean processNotFinished = true;
		int code = 0;		
		
		payloadPID = child.get(1);
		apmon.addJobToMonitor(payloadPID, jobWorkdir, ce, hostName); // TODO: test
		mj = new MonitoredJob(payloadPID, jobWorkdir, ce, hostName);
		
		try { 
			while (processNotFinished){
				try {
				    Thread.sleep(5 * 1000); // TODO: change to 60, and send every 600
				    code = p.exitValue();
				    processNotFinished = false;
				} catch (IllegalThreadStateException e) {
				    // process hasn't terminated
					String error = checkProcessResources();						
					if (error != null){
						p.destroy();
						System.out.println("Process overusing resources: "+error);
						return -2;
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
	}
	
	private String checkProcessResources() {
		String error = null;
		System.out.println("Checking resources usage");
		
		try {
			HashMap<Long, Double> jobinfo 	= mj.readJobInfo();
			HashMap<Long, Double> diskinfo 	= mj.readJobDiskUsage();
			
			System.out.println("Workdir MB: "+diskinfo.get(ApMonMonitoringConstants.LJOB_WORKDIR_SIZE));
			System.out.println("VMEM MB: "+jobinfo.get(ApMonMonitoringConstants.LJOB_VIRTUALMEM)/1024);
			System.out.println("CPUTime: "+jobinfo.get(ApMonMonitoringConstants.LJOB_CPU_TIME));
			
			System.out.println("Workdir JDL MB: "+workdirMaxSizeMB);
			System.out.println("VMEM JDL MB: "+jobMaxMemoryMB);
			
			// check disk usage
			if (workdirMaxSizeMB != 0 
					&& diskinfo.get(ApMonMonitoringConstants.LJOB_WORKDIR_SIZE) > workdirMaxSizeMB){
				error = "Disk space limit is "+workdirMaxSizeMB+", using "+diskinfo.get(ApMonMonitoringConstants.LJOB_WORKDIR_SIZE);
			}
			
			// check disk usage
			if (jobMaxMemoryMB != 0 
					&& jobinfo.get(ApMonMonitoringConstants.LJOB_VIRTUALMEM)/1024 > jobMaxMemoryMB){
				error = "Memory usage limit is "+jobMaxMemoryMB+", using "+jobinfo.get(ApMonMonitoringConstants.LJOB_VIRTUALMEM)/1024;
			}
			
			// cpu
			Double cputime 	= jobinfo.get(ApMonMonitoringConstants.LJOB_CPU_TIME);
			long time = System.currentTimeMillis();
			
			if ( prevTime != 0 && prevTime+(20*60*1000)<time 
					&& cputime==prevCpuTime ){
				error = "The job hasn't used the CPU for 20 minutes";
			}
			else {
				prevCpuTime = cputime;
				prevTime 	= time;
			}
						
			
		} catch (IOException e) {
			System.out.println("Problem with the monitoring objects: "+e.toString());
		}
		
       	return error;
	}
	
	
	private void getMemoryRequirements() {
		// Sandbox size
		String workdirMaxSize = jdl.gets("Workdirectorysize");
		
		if (workdirMaxSize != null){
			Pattern p = Pattern.compile("\\p{L}");
			Matcher m = p.matcher(workdirMaxSize);
			if (m.find()) {
			    String number 	= workdirMaxSize.substring(0, m.start());
			    String unit 	= workdirMaxSize.substring(m.start());
			    
			    switch(unit){
			    	case "KB":
			    		workdirMaxSizeMB = Integer.parseInt(number)/1024;
			    		break;
		    		case "GB":
			    		workdirMaxSizeMB = Integer.parseInt(number)*1024;
			    		break;
		    		default: // MB
				    	workdirMaxSizeMB = Integer.parseInt(number);
			    }
			}
			else {
				workdirMaxSizeMB = Integer.parseInt(workdirMaxSize);
			}
		} else 
			workdirMaxSizeMB = 0;
		
		// Memory use
		String maxmemory = jdl.gets("Memorysize");
		
		if(maxmemory!=null){
			Pattern p = Pattern.compile("\\p{L}");
			Matcher m = p.matcher(maxmemory);
			if (m.find()) {
			    String number 	= maxmemory.substring(0, m.start());
			    String unit 	= maxmemory.substring(m.start());
			    			    
			    switch(unit){
			    	case "KB":
			    		jobMaxMemoryMB = Integer.parseInt(number)/1024;
			    		break;
		    		case "GB":
		    			jobMaxMemoryMB = Integer.parseInt(number)*1024;
		    			break;
		    		default: // MB
		    			jobMaxMemoryMB = Integer.parseInt(number);
			    }
			}
			else {
				jobMaxMemoryMB = Integer.parseInt(maxmemory);
			}			
		}
		else
			jobMaxMemoryMB = 0;
		
	}

	private int execute() {		
		final int code = executeCommand(jdl.gets("Executable"), jdl.getArguments(), jdl.getInteger("TTL")+300, TimeUnit.SECONDS);

		System.err.println("Execution code: " + code);

		return code;
	}

	private boolean validate() {
		int code = 0;
		
		String validation = jdl.gets("ValidationCommand");
		
		if(validation != null)
			code = executeCommand(validation, null, 5, TimeUnit.MINUTES);

		System.err.println("Validation code: " + code);

		return code == 0;
	}


	private boolean getInputFiles() {
		final Set<String> filesToDownload = new HashSet<>();

		List<String> list = jdl.getInputFiles(false);

		if (list != null)
			filesToDownload.addAll(list);

		list = jdl.getInputData(false);

		if (list != null)
			filesToDownload.addAll(list);

		String s = jdl.getExecutable();

		if (s != null)
			filesToDownload.add(s);

		s = jdl.gets("ValidationCommand");

		if (s != null)
			filesToDownload.add(s);

		final List<LFN> iFiles = c_api.getLFNs(filesToDownload, true, false);

		if (iFiles == null || iFiles.size() != filesToDownload.size()) {
			System.out.println("Not all requested files could be located");
			return false;
		}

		final Map<LFN, File> localFiles = new HashMap<>();

		for (final LFN l : iFiles) {
			File localFile = new File(tempDir, l.getFileName());

			final int i = 0;

			while (localFile.exists() && i < 100000)
				localFile = new File(tempDir, l.getFileName() + "." + i);

			if (localFile.exists()) {
				System.out.println("Too many occurences of " + l.getFileName() + " in " + tempDir.getAbsolutePath());
				return false;
			}

			localFiles.put(l, localFile);
		}

		for (final Map.Entry<LFN, File> entry : localFiles.entrySet()) {
			final List<PFN> pfns = c_api.getPFNsToRead(entry.getKey(), null, null);

			if (pfns == null || pfns.size() == 0) {
				System.out.println("No replicas of " + entry.getKey().getCanonicalName() + " to read from");
				return false;
			}

			final GUID g = pfns.iterator().next().getGuid();

			final File f = IOUtils.get(g, entry.getValue());

			if (f == null) {
				System.out.println("Could not download " + entry.getKey().getCanonicalName() + " to " + entry.getValue().getAbsolutePath());
				return false;
			}
		}

		System.out.println("Sandbox prepared : " + tempDir.getAbsolutePath());

		return true;
	}

	
	private HashMap<String, String> getJobPackagesEnvironment() {
		String voalice = "VO_ALICE@";
		String packagestring = "";
		HashMap<String,String> packs = (HashMap<String, String>) jdl.getPackages();	
		HashMap<String, String> envmap = new HashMap<>(); 
		
		if (packs != null){
			for (String pack: packs.keySet()) {
				packagestring += voalice + pack + "::" + packs.get(pack) + ",";			
			}
			packagestring = packagestring.substring(0, packagestring.length()-1);
			
			ArrayList<String> packages = new ArrayList<String>();
			packages.add(packagestring);
			
			logger.log(Level.INFO, packagestring);
		
			envmap = (HashMap<String, String>) installPackages(packages);
		}
		
		logger.log(Level.INFO, envmap.toString());
		return envmap;
	}

	private boolean uploadOutputFiles() {
		boolean uploadedAllOutFiles = true;
		boolean uploadedNotAllCopies = false;

		String outputDir = jdl.getOutputDir();

		if (outputDir == null)
			outputDir = FileSystemUtils.getAbsolutePath(username, null, "~" + defaultOutputDirPrefix + queueId);

		System.out.println("queueId: " + queueId);
		System.out.println("outputDir: " + outputDir);

		if (c_api.getLFN(outputDir) != null) {
			System.err.println("OutputDir [" + outputDir + "] already exists.");
			TaskQueueApiUtils.setJobStatus(queueId, JobStatus.ERROR_SV);
			jobStatus = JobStatus.ERROR_SV;
			return false;
		}
		
		final LFN outDir = c_api.createCatalogueDirectory(outputDir);
		
		if (outDir == null) {
			System.err.println("Error creating the OutputDir [" + outputDir + "].");
			uploadedAllOutFiles = false;
		}
		else {
			String tag = "Output";
			if (jobStatus == JobStatus.ERROR_E)
				tag = "OutputErrorE";
			
			ParsedOutput filesTable = new ParsedOutput(queueId, jdl, jobWorkdir, tag);

			for (final OutputEntry entry : filesTable.getEntries()) {
				File localFile;
				try {
					if(entry.isArchive()) {
						entry.createZip(jobWorkdir);
					}
					
					localFile = new File(jobWorkdir + "/" + entry.getName());
					System.out.println("Processing output file: "+localFile);

					if (localFile.exists() && localFile.isFile() && localFile.canRead() && localFile.length() > 0) {

						final long size = localFile.length();
						if (size <= 0)
							System.err.println("Local file has size zero: " + localFile.getAbsolutePath());
						String md5 = null;
						try {
							md5 = IOUtils.getMD5(localFile);
						} catch (final Exception e1) {
							// ignore
						}
						if (md5 == null)
							System.err.println("Could not calculate md5 checksum of the local file: " + localFile.getAbsolutePath());

						LFN lfn = c_api.getLFN(outDir.getCanonicalName() + "/" + entry.getName(), true);
						lfn.size 	= size;
						lfn.md5 	= md5;
						lfn.jobid 	= queueId;
						lfn.type	= 'f';
						GUID guid 	= GUIDUtils.createGuid(localFile, commander.getUser());
						lfn.guid 	= guid.guid;
						ArrayList<String> exses = entry.getSEsDeprioritized();
						
						
						List<PFN> pfns = c_api.getPFNsToWrite(lfn, guid, entry.getSEsPrioritized(), exses, entry.getQoS());

						System.out.println("LFN :"+lfn+ "\npfns: "+pfns.toString());
						
						if (pfns != null && !pfns.isEmpty()) {
							final ArrayList<String> envelopes = new ArrayList<>(pfns.size());
							for (final PFN pfn : pfns) {
								final List<Protocol> protocols = Transfer.getAccessProtocols(pfn);
								for (final Protocol protocol : protocols) {
									envelopes.add(protocol.put(pfn, localFile));
									break;
								}
							}

							// drop the following three lines once put replies
							// correctly
							// with the signed envelope
							envelopes.clear();
							for (final PFN pfn : pfns)
								envelopes.add(pfn.ticket.envelope.getSignedEnvelope());

							final List<PFN> pfnsok = c_api.registerEnvelopes(envelopes);
							if (!pfns.equals(pfnsok)){
								if (pfnsok != null && pfnsok.size() > 0) {
									System.out.println("Only " + pfnsok.size() + " could be uploaded");
									uploadedNotAllCopies = true;
								} else {
									System.err.println("Upload failed, sorry!");
									uploadedAllOutFiles = false;
									break;
								}
							}
						}
						else { System.out.println("Couldn't get write envelopes for output file"); }
					} else
						System.out.println("Can't upload output file " + localFile.getName() + ", does not exist or has zero size.");

				} catch (final IOException e) {
					e.printStackTrace();
					uploadedAllOutFiles = false;
				}
			}
		}	
		
		if(jobStatus != JobStatus.ERROR_E && jobStatus != JobStatus.ERROR_V){
			if (uploadedNotAllCopies){
				TaskQueueApiUtils.setJobStatus(queueId, JobStatus.DONE_WARN);
				jobStatus = JobStatus.DONE_WARN;
			}
			else if (uploadedAllOutFiles){
				TaskQueueApiUtils.setJobStatus(queueId, JobStatus.DONE);
				jobStatus = JobStatus.DONE;
			}
			else{
				TaskQueueApiUtils.setJobStatus(queueId, JobStatus.ERROR_SV);
				jobStatus = JobStatus.ERROR_SV;
			}
		}
			
		return uploadedAllOutFiles;
	}

	
	private boolean createWorkDir() {
		logger.log(Level.INFO, "Creating sandbox and chdir");
		
		jobWorkdir = String.format("%s%s%d", workdir,defaultOutputDirPrefix,queueId);
		
		tempDir = new File(jobWorkdir);
		if (!tempDir.exists()) {
			final boolean created = tempDir.mkdirs();
			if (!created) {
				logger.log(Level.INFO, "Workdir does not exist and can't be created: "+jobWorkdir);
				return false;
			}
		}

		// chdir
		System.setProperty("user.dir", jobWorkdir);

		// TODO: create the extra directories
		
		return true;
	}
	
	private HashMap<String,String> loadJDLEnvironmentVariables() {
		HashMap<String,String> hashret = new HashMap<>();
		
		try {
			HashMap<String,Object> vars = (HashMap<String, Object>) jdl.getJDLVariables();
			
			if(vars != null){
				for (String s:vars.keySet()){
					String value = "";
					Object val = jdl.get(s);
					
					if(val instanceof Collection){
						final Iterator<String> it = ((Collection<String>) val).iterator();
						String sbuff = "";
						boolean isFirst = true;
						
						while (it.hasNext()) {
							if(!isFirst){
								sbuff += "##";
					        }
							String v = it.next().toString();
							sbuff += v;
					        isFirst = false;	
						}
						value = sbuff;
					}
					else{
						value = val.toString();
					}
					
					hashret.put("ALIEN_JDL_"+s.toUpperCase(), value);
				}
			}
		} catch (Exception e){
			System.out.println("There was a problem getting JDLVariables: "+e);
		}
				
		return hashret;
	}

	
	public static void main(final String[] args) throws IOException {
		final JobAgent ja = new JobAgent();
		ja.run();
	}
	
}
