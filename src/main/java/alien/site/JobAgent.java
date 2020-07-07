package alien.site;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.lang.ProcessBuilder.Redirect;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import alien.api.Request;
import alien.api.taskQueue.GetMatchJob;
import alien.api.taskQueue.TaskQueueApiUtils;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.shell.commands.JAliEnCOMMander;
import alien.site.containers.Containerizer;
import alien.site.containers.ContainerizerFactory;
import alien.site.packman.CVMFS;
import alien.taskQueue.JDL;
import alien.taskQueue.Job;
import alien.taskQueue.JobStatus;
import apmon.ApMon;
import apmon.ApMonException;
import apmon.ApMonMonitoringConstants;
import apmon.BkThread;
import apmon.MonitoredJob;
import lazyj.ExtProperties;
import lia.util.process.ExternalProcesses;

/**
 * Gets matched jobs, and launches JobWrapper for executing them
 */
public class JobAgent implements Runnable {

	// Variables passed through VoBox environment
	private final Map<String, String> env = System.getenv();
	private final String ce;
	private final int origTtl;

	// Folders and files
	private File tempDir;
	private static final String defaultOutputDirPrefix = "/jalien-job-";
	private static final String jobWrapperLogName = "jalien-jobwrapper.log";
	private String jobWorkdir;
	private String jobWrapperLogDir;

	// Job variables
	private JDL jdl;
	private long queueId;
	private String username;
	private String tokenCert;
	private String tokenKey;
	private String jobAgentId;
	private String workdir;
	private String legacyToken;
	private HashMap<String, Object> matchedJob;
	private HashMap<String, Object> siteMap = new HashMap<>();
	private int workdirMaxSizeMB;
	private int jobMaxMemoryMB;
	private MonitoredJob mj;
	private Double prevCpuTime;
	private long prevTime = 0;
	private int cpuCores = 1;

	private int totalJobs;
	private final long jobAgentStartTime = System.currentTimeMillis();

	// Other
	private String hostName;
	private final JAliEnCOMMander commander = JAliEnCOMMander.getInstance();
	private String jarPath;
	private String jarName;
	private int wrapperPID;
	private static float lhcbMarks = -1;

	private enum jaStatus {
		REQUESTING_JOB(1), INSTALLING_PKGS(2), JOB_STARTED(3), RUNNING_JOB(4), DONE(5), ERROR_HC(-1), // error in getting host
		ERROR_IP(-2), // error installing packages
		ERROR_GET_JDL(-3), // error getting jdl
		ERROR_JDL(-4), // incorrect jdl
		ERROR_DIRS(-5), // error creating directories, not enough free space in workdir
		ERROR_START(-6); // error forking to start job

		private final int value;

		private jaStatus(int value) {
			this.value = value;
		}

		public int getValue() {
			return value;
		}
	}

	private final int jobagent_requests = 5;

	/**
	 * logger object
	 */
	static final Logger logger = ConfigUtils.getLogger(JobAgent.class.getCanonicalName());

	/**
	 * ML monitor object
	 */
	static final Monitor monitor = MonitorFactory.getMonitor(JobAgent.class.getCanonicalName());
	/**
	 * ApMon sender
	 */
	static final ApMon apmon = MonitorFactory.getApMonSender();

	// _ource monitoring vars

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

	/**
	 */
	public JobAgent() {
		// site = env.get("site"); // or
		// ConfigUtils.getConfig().gets("alice_close_site").trim();

		ce = env.get("CE");

		jobWrapperLogDir = env.getOrDefault("TMPDIR", "/tmp") + "/" + jobWrapperLogName;

		String DN = commander.getUser().getUserCert()[0].getSubjectDN().toString();

		logger.log(Level.INFO, "We have the following DN :" + DN);

		totalJobs = 0;

		siteMap = (new SiteMap()).getSiteParameters(env);

		hostName = (String) siteMap.get("Localhost");
		// alienCm = (String) siteMap.get("alienCm");

		if (env.containsKey("ALIEN_JOBAGENT_ID"))
			jobAgentId = env.get("ALIEN_JOBAGENT_ID");
		else
			jobAgentId = Request.getVMID().toString();

		workdir = (String) siteMap.get("workdir");

		origTtl = ((Integer) siteMap.get("TTL")).intValue();

		Hashtable<Long, String> cpuinfo;
		try {
			cpuinfo = BkThread.getCpuInfo();
			RES_CPUFAMILY = cpuinfo.get(ApMonMonitoringConstants.LGEN_CPU_FAMILY);
			RES_CPUMHZ = cpuinfo.get(ApMonMonitoringConstants.LGEN_CPU_MHZ);
			RES_CPUMHZ = RES_CPUMHZ.substring(0, RES_CPUMHZ.indexOf("."));
			RES_NOCPUS = Integer.valueOf(BkThread.getNumCPUs());

			logger.log(Level.INFO, "CPUFAMILY: " + RES_CPUFAMILY);
			logger.log(Level.INFO, "CPUMHZ: " + RES_CPUMHZ);
			logger.log(Level.INFO, "NOCPUS: " + RES_NOCPUS);
		}
		catch (final IOException e) {
			logger.log(Level.WARNING, "Problem with the monitoring objects IO Exception: " + e.toString());
		}
		catch (final ApMonException e) {
			logger.log(Level.WARNING, "Problem with the monitoring objects ApMon Exception: " + e.toString());
		}

		try {
			File filepath = new java.io.File(JobAgent.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
			jarName = filepath.getName();
			jarPath = filepath.toString().replace(jarName, "");

		}
		catch (final URISyntaxException e) {
			logger.log(Level.SEVERE, "Could not obtain AliEn jar path: " + e.toString());
		}
	}

	@Override
	public void run() {
		logger.log(Level.INFO, "Starting JobAgent in " + hostName);

		int count = jobagent_requests;
		while (count > 0) {
			if (!updateDynamicParameters())

				break;

			logger.log(Level.INFO, siteMap.toString());
			try {
				logger.log(Level.INFO, "Trying to get a match...");

				monitor.sendParameter("ja_status", Integer.valueOf(jaStatus.REQUESTING_JOB.getValue()));
				monitor.sendParameter("TTL", siteMap.get("TTL"));

				final GetMatchJob jobMatch = commander.q_api.getMatchJob(siteMap);

				matchedJob = jobMatch.getMatchJob();

				// TODELETE
				if (matchedJob != null)
					logger.log(Level.INFO, matchedJob.toString());

				if (matchedJob != null && !matchedJob.containsKey("Error")) {
					jdl = new JDL(Job.sanitizeJDL((String) matchedJob.get("JDL")));
					queueId = ((Long) matchedJob.get("queueId")).longValue();
					username = (String) matchedJob.get("User");
					tokenCert = (String) matchedJob.get("TokenCertificate");
					tokenKey = (String) matchedJob.get("TokenKey");
					legacyToken = (String) matchedJob.get("LegacyToken");

					matchedJob.entrySet().forEach(entry -> {
						System.err.println(entry.getKey() + " " + entry.getValue());
					});

					// TODO: commander.setUser(username);
					// commander.setSite(site);

					logger.log(Level.INFO, jdl.getExecutable());
					logger.log(Level.INFO, username);
					logger.log(Level.INFO, Long.toString(queueId));

					// process payload
					handleJob();

					cleanup();
				}
				else { // TODO: Handle matchedJob.containsKey("Error") after all?
					logger.log(Level.INFO, "We didn't get anything back. Nothing to run right now.");
				}
			}
			catch (final Exception e) {
				logger.log(Level.INFO, "Error getting a matching job: ", e);
			}
			count--;
			if (count != 0) {
				logger.log(Level.INFO, "Idling 20secs zZz...");
				try {
					// TODO?: monitor.sendBgMonitoring
					Thread.sleep(20 * 1000);
				}
				catch (final InterruptedException e) {
					logger.log(Level.WARNING, "Interrupt received", e);
				}
			}
		}

		logger.log(Level.INFO, "JobAgent finished, id: " + jobAgentId + " totalJobs: " + totalJobs);
		System.exit(0);
	}

	private void handleJob() {

		totalJobs++;
		try {

			if (!createWorkDir()) {
				// changeStatus(JobStatus.ERROR_IB);
				logger.log(Level.INFO, "Error. Workdir for job could not be created");
				return;
			}

			logger.log(Level.INFO, "Started JA with: " + jdl);
			commander.q_api.putJobLog(queueId, "trace", "Running JAliEn JobAgent " + CVMFS.getJAliEnVersion());
			commander.q_api.putJobLog(queueId, "trace", "Job preparing to run in: " + hostName);

			// Set up constraints
			getMemoryRequirements();

			final List<String> launchCommand = generateLaunchCommand();

			setupJobWrapperLogging();

			commander.q_api.putJobLog(queueId, "trace", "Starting JobWrapper");

			launchJobWrapper(launchCommand, true);

		}
		catch (final Exception e) {
			logger.log(Level.SEVERE, "Unable to handle job", e);
		}
	}

	private void cleanup() {
		logger.log(Level.INFO, "Sending monitoring values...");

		monitor.sendParameter("job_id", Integer.valueOf(0));
		monitor.sendParameter("ja_status", Integer.valueOf(jaStatus.DONE.getValue()));

		logger.log(Level.INFO, "Cleaning up after execution...");

		try {
			Files.walk(tempDir.toPath())
					.map(Path::toFile)
					.sorted(Comparator.reverseOrder()) // or else dir will appear before its contents
					.forEach(File::delete);
		}
		catch (IOException e) {
			logger.log(Level.WARNING, "Error deleting the job workdir: " + e.toString());
		}

		RES_WORKDIR_SIZE = ZERO;
		RES_VMEM = ZERO;
		RES_RMEM = ZERO;
		RES_VMEMMAX = ZERO;
		RES_RMEMMAX = ZERO;
		RES_MEMUSAGE = ZERO;
		RES_CPUTIME = ZERO;
		RES_CPUUSAGE = ZERO;
		RES_RESOURCEUSAGE = "";
		RES_RUNTIME = Long.valueOf(0);
		RES_FRUNTIME = "";

		logger.log(Level.INFO, "Done!");
	}

	/**
	 * @param folder
	 * @return amount of free space (in bytes) in the given folder. Or zero if there was a problem (or no free space).
	 */
	public static long getFreeSpace(final String folder) {
		long space = new File(folder).getFreeSpace();

		if (space <= 0) {
			// 32b JRE returns 0 when too much space is available

			try {
				final String output = ExternalProcesses.getCmdOutput(Arrays.asList("df", "-P", "-B", "1024", folder), true, 30L, TimeUnit.SECONDS);

				try (BufferedReader br = new BufferedReader(new StringReader(output))) {
					String sLine = br.readLine();

					if (sLine != null) {
						sLine = br.readLine();

						if (sLine != null) {
							final StringTokenizer st = new StringTokenizer(sLine);

							st.nextToken();
							st.nextToken();
							st.nextToken();

							space = Long.parseLong(st.nextToken());
						}
					}
				}
			}
			catch (IOException | InterruptedException ioe) {
				logger.log(Level.WARNING, "Could not extract the space information from `df`", ioe);
			}
		}

		return space;
	}

	/**
	 * updates jobagent parameters that change between job requests
	 *
	 * @return false if we can't run because of current conditions, true if positive
	 */
	private boolean updateDynamicParameters() {
		logger.log(Level.INFO, "Updating dynamic parameters of jobAgent map");

		// free disk recalculation
		final long space = getFreeSpace(workdir) / 1024;

		// ttl recalculation
		final long jobAgentCurrentTime = System.currentTimeMillis();
		final int time_subs = (int) (jobAgentCurrentTime - jobAgentStartTime) / 1000; // convert to seconds
		int timeleft = origTtl - time_subs;

		logger.log(Level.INFO, "Still have " + timeleft + " seconds to live (" + jobAgentCurrentTime + "-" + jobAgentStartTime + "=" + time_subs + ")");

		// we check if the cert timeleft is smaller than the ttl itself
		final int certTime = getCertTime();
		logger.log(Level.INFO, "Certificate timeleft is " + certTime);
		if (certTime > 0 && certTime < timeleft)
			timeleft = certTime - 900; // (-15min)

		// safety time for saving, etc
		timeleft -= 600;

		// what is the minimum we want to run with? (100MB?)
		if (space <= 100 * 1024 * 1024) {
			logger.log(Level.INFO, "There is not enough space left: " + space);
			if (!System.getenv().containsKey("JALIEN_IGNORE_STORAGE")) {
				return false;
			}
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
	 * @return the time in seconds that the certificate is still valid for
	 */
	private int getCertTime() {
		return (int) TimeUnit.MILLISECONDS.toSeconds(commander.getUser().getUserCert()[0].getNotAfter().getTime() - System.currentTimeMillis());
	}

	private void getMemoryRequirements() {
		// Sandbox size
		final String workdirMaxSize = jdl.gets("Workdirectorysize");

		// By default the jobs are allowed to use up to 10GB of disk space in the sandbox
		workdirMaxSizeMB = 10 * 1024;

		final Pattern p = Pattern.compile("\\p{L}");

		if (workdirMaxSize != null) {
			final Matcher m = p.matcher(workdirMaxSize);
			try {
				if (m.find()) {
					final String number = workdirMaxSize.substring(0, m.start());
					final String unit = workdirMaxSize.substring(m.start());

					workdirMaxSizeMB = convertStringUnitToIntegerMB(unit, number);
				}
				else
					workdirMaxSizeMB = Integer.parseInt(workdirMaxSize);

				commander.q_api.putJobLog(queueId, "trace", "Local disk space limit (JDL): " + workdirMaxSizeMB + "MB");
			}
			catch (@SuppressWarnings("unused") NumberFormatException nfe) {
				commander.q_api.putJobLog(queueId, "trace", "Local disk space specs are invalid: '" + workdirMaxSize + "', using the default " + workdirMaxSizeMB + "MB");
			}
		}
		else
			commander.q_api.putJobLog(queueId, "trace", "Local disk space limit (default): " + jobMaxMemoryMB + "MB");

		Integer requestedCPUCores = jdl.getInteger("CPUCores");

		if (requestedCPUCores != null && requestedCPUCores.intValue() > 0)
			cpuCores = requestedCPUCores.intValue();

		// Memory use
		final String maxmemory = jdl.gets("Memorysize");

		// By default the job is limited to using 8GB of virtual memory per allocated CPU core
		jobMaxMemoryMB = cpuCores * 8 * 1024;

		if (maxmemory != null) {
			final Matcher m = p.matcher(maxmemory);
			try {
				if (m.find()) {
					final String number = maxmemory.substring(0, m.start());
					final String unit = maxmemory.substring(m.start()).toUpperCase();

					jobMaxMemoryMB = convertStringUnitToIntegerMB(unit, number);
				}
				else
					jobMaxMemoryMB = Integer.parseInt(maxmemory);

				commander.q_api.putJobLog(queueId, "trace", "Virtual memory limit (JDL): " + jobMaxMemoryMB + "MB");
			}
			catch (@SuppressWarnings("unused") NumberFormatException nfe) {
				commander.q_api.putJobLog(queueId, "trace", "Virtual memory limit specs are invalid: '" + maxmemory + "', using the default " + jobMaxMemoryMB + "MB");
			}
		}
		else
			commander.q_api.putJobLog(queueId, "trace", "Virtual memory limit (default): " + jobMaxMemoryMB + "MB");
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {
		ConfigUtils.setApplicationName("JobAgent");
		ConfigUtils.switchToForkProcessLaunching();

		final JobAgent jao = new JobAgent();
		jao.run();
	}

	/**
	 * @return Command w/arguments for starting the JobWrapper, based on the command used for the JobAgent
	 * @throws InterruptedException
	 */
	public List<String> generateLaunchCommand() throws InterruptedException {
		try {
			// Main cmd for starting the JobWrapper
			final List<String> launchCmd = new ArrayList<>();

			final Process cmdChecker = Runtime.getRuntime().exec("ps -p " + MonitorFactory.getSelfProcessID() + " -o command=");
			cmdChecker.waitFor();
			try (Scanner cmdScanner = new Scanner(cmdChecker.getInputStream())) {
				String readArg;
				while (cmdScanner.hasNext()) {
					readArg = (cmdScanner.next());
					switch (readArg) {
						case "-cp":
							cmdScanner.next();
							break;
						case "alien.site.JobAgent":
							launchCmd.add("-DAliEnConfig=" + jobWorkdir);
							launchCmd.add("-cp");
							launchCmd.add(jarPath + jarName);
							launchCmd.add("alien.site.JobWrapper");
							break;
						default:
							launchCmd.add(readArg);
					}
				}
			}

			// Check if there is container support at present on site. If yes, add to launchCmd
			Containerizer cont = ContainerizerFactory.getContainerizer();
			if (cont != null) {
				monitor.sendParameter("canRunContainers", Integer.valueOf(1));
				monitor.sendParameter("containerLayer", Integer.valueOf(1));
				cont.setWorkdir(jobWorkdir);
				return cont.containerize(String.join(" ", launchCmd));
			}
			monitor.sendParameter("canRunContainers", Integer.valueOf(0));
			return launchCmd;
		}
		catch (final IOException e) {
			logger.log(Level.SEVERE, "Could not generate JobWrapper launch command: " + e.toString());
			return null;
		}
	}

	private int launchJobWrapper(List<String> launchCommand, boolean monitorJob) {
		logger.log(Level.INFO, "Launching jobwrapper using the command: " + launchCommand.toString());

		final ProcessBuilder pBuilder = new ProcessBuilder(launchCommand);
		pBuilder.environment().remove("JALIEN_TOKEN_CERT");
		pBuilder.environment().remove("JALIEN_TOKEN_KEY");
		pBuilder.environment().put("TMPDIR", jobWorkdir + "/tmp");
		pBuilder.redirectError(Redirect.INHERIT);
		pBuilder.directory(tempDir);

		final Process p;

		// stdin from the viewpoint of the wrapper
		final OutputStream stdin;

		try {
			p = pBuilder.start();

			stdin = p.getOutputStream();
			try (ObjectOutputStream stdinObj = new ObjectOutputStream(stdin)) {
				stdinObj.writeObject(jdl);
				stdinObj.writeObject(username);
				stdinObj.writeObject(Long.valueOf(queueId));
				stdinObj.writeObject(tokenCert);
				stdinObj.writeObject(tokenKey);
				stdinObj.writeObject(ce);
				stdinObj.writeObject(siteMap);
				stdinObj.writeObject(defaultOutputDirPrefix);
				stdinObj.writeObject(legacyToken);

				stdinObj.flush();
			}

			logger.log(Level.INFO, "JDL info sent to JobWrapper");
			commander.q_api.putJobLog(queueId, "trace", "JobWrapper started");

			// Wait for JobWrapper to start
			try (InputStream stdout = p.getInputStream()) {
				stdout.read();
			}

		}
		catch (final Exception ioe) {
			logger.log(Level.SEVERE, "Exception running " + launchCommand + " : " + ioe.getMessage());
			return 1;
		}

		if (monitorJob) {
			wrapperPID = (int) p.pid();

			apmon.addJobToMonitor(wrapperPID, jobWorkdir, ce, hostName);
			mj = new MonitoredJob(wrapperPID, jobWorkdir, ce, hostName);

			final String fs = checkProcessResources();
			if (fs == null)
				sendProcessResources();
		}

		TimerTask killPayload = new TimerTask() {
			@Override
			public void run() {
				final Vector<Integer> childProcs = mj.getChildren();
				if (childProcs != null || childProcs.size() > 1) {
					try {
						Runtime.getRuntime().exec("kill -9 " + getPayloadPid(childProcs));
						Thread.sleep(60 * 1000); //Give the JobWrapper 60s to clean things up
					} catch (Exception e) {}
				}
				//If still alive, kill everything, including the JW
				if (p.isAlive()) {
					p.destroyForcibly();
				}
			}
		};

		final Timer t = new Timer();
		t.schedule(killPayload, TimeUnit.MILLISECONDS.convert(ttlForJob(), TimeUnit.SECONDS)); // TODO: ttlForJob

		int code = 0;

		logger.log(Level.INFO, "About to enter monitor loop. Is the JobWrapper process alive?: " + p.isAlive());

		int monitor_loops = 0;
		try {
			while (p.isAlive()) {
				logger.log(Level.INFO, "Waiting for the JobWrapper process to finish");

				if (monitorJob) {
					monitor_loops++;
					final String error = checkProcessResources();
					if (error != null) {
						// killProcess.run(); //TODO: Temporarily disabled
						logger.log(Level.SEVERE, "Process overusing resources: " + error);
						// return 1;
					}
					if (monitor_loops == 10) {
						monitor_loops = 0;
						sendProcessResources();
					}
				}
				try {
					Thread.sleep(5 * 1000);
				}
				catch (final InterruptedException ie) {
					logger.log(Level.WARNING, "Interrupted while waiting for the JobWrapper to finish execution: " + ie.getMessage());
					return 1;
				}
			}
			code = p.exitValue();

			logger.log(Level.INFO, "JobWrapper has finished execution. Exit code: " + code);
			if (code != 0)
				logger.log(Level.WARNING, "Error encountered: see the JobWrapper logs in: " + env.getOrDefault("TMPDIR", "/tmp") + "/jalien-jobwrapper.log " + " for more details");

			return code;
		}
		finally {
			try {
				t.cancel();
				stdin.close();
			}
			catch (final Exception e) {
				logger.log(Level.WARNING, "Not all resources from the current job could be cleared: " + e);
			}
			apmon.removeJobToMonitor(wrapperPID);
			if (code != 0) {
				// Looks like something went wrong. Let's check the last reported status
				final String lastStatus = readWrapperStatus();
				if (lastStatus.equals("STARTED") || lastStatus.equals("RUNNING"))
					changeJobStatus(JobStatus.ERROR_E, null); // JobWrapper was killed before the job could be completed
				else if (lastStatus.equals("SAVING")) {
					changeJobStatus(JobStatus.ERROR_SV, null); // JobWrapper was killed during saving
				}
			}
		}
	}

	private void sendProcessResources() {
		// runtime(date formatted) start cpu(%) mem cputime rsz vsize ncpu
		// cpufamily cpuspeed resourcecost maxrss maxvss ksi2k
		final String procinfo = String.format("%s %d %.2f %.2f %.2f %.2f %.2f %d %s %s %s %.2f %.2f 1000", RES_FRUNTIME, RES_RUNTIME, RES_CPUUSAGE, RES_MEMUSAGE, RES_CPUTIME, RES_RMEM, RES_VMEM,
				RES_NOCPUS, RES_CPUFAMILY, RES_CPUMHZ, RES_RESOURCEUSAGE, RES_RMEMMAX, RES_VMEMMAX);
		logger.log(Level.INFO, "+++++ Sending resources info +++++");
		logger.log(Level.INFO, procinfo);

		commander.q_api.putJobLog(queueId, "proc", procinfo);
	}

	private String checkProcessResources() { // checks and maintains sandbox
		String error = null;
		logger.log(Level.INFO, "Checking resources usage");

		try {

			final HashMap<Long, Double> jobinfo = mj.readJobInfo();

			final HashMap<Long, Double> diskinfo = mj.readJobDiskUsage();

			if (jobinfo == null || diskinfo == null) {

				logger.log(Level.WARNING, "JobInfo or DiskInfo monitor null");
				// return "Not available"; TODO: Adjust and put back again
			}

			// getting cpu, memory and runtime info
			if (diskinfo != null)
				RES_WORKDIR_SIZE = diskinfo.get(ApMonMonitoringConstants.LJOB_WORKDIR_SIZE);

			if (jobinfo != null) {
				RES_VMEM = Double.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_VIRTUALMEM).doubleValue() / 1024);
				RES_RMEM = Double.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_RSS).doubleValue() / 1024);
				RES_CPUTIME = jobinfo.get(ApMonMonitoringConstants.LJOB_CPU_TIME);
				RES_CPUUSAGE = Double.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_CPU_USAGE).doubleValue());
				RES_RUNTIME = Long.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_RUN_TIME).longValue());
				RES_MEMUSAGE = jobinfo.get(ApMonMonitoringConstants.LJOB_MEM_USAGE);
			}

			// RES_RESOURCEUSAGE =
			// Format.showDottedDouble(RES_CPUTIME.doubleValue() *
			// Double.parseDouble(RES_CPUMHZ) / 1000, 2);
			RES_RESOURCEUSAGE = String.format("%.02f", Double.valueOf(RES_CPUTIME.doubleValue() * Double.parseDouble(RES_CPUMHZ) / 1000));

			// max memory consumption
			if (RES_RMEM.doubleValue() > RES_RMEMMAX.doubleValue())
				RES_RMEMMAX = RES_RMEM;

			if (RES_VMEM.doubleValue() > RES_VMEMMAX.doubleValue())
				RES_VMEMMAX = RES_VMEM;

			// formatted runtime
			if (RES_RUNTIME.longValue() < 60)
				RES_FRUNTIME = String.format("00:00:%02d", Long.valueOf(RES_RUNTIME.longValue()));
			else if (RES_RUNTIME.longValue() < 3600)
				RES_FRUNTIME = String.format("00:%02d:%02d", Long.valueOf(RES_RUNTIME.longValue() / 60), Long.valueOf(RES_RUNTIME.longValue() % 60));
			else
				RES_FRUNTIME = String.format("%02d:%02d:%02d", Long.valueOf(RES_RUNTIME.longValue() / 3600), Long.valueOf((RES_RUNTIME.longValue() - (RES_RUNTIME.longValue() / 3600) * 3600) / 60),
						Long.valueOf((RES_RUNTIME.longValue() - (RES_RUNTIME.longValue() / 3600) * 3600) % 60));

			// check disk usage
			if (workdirMaxSizeMB != 0 && RES_WORKDIR_SIZE.doubleValue() > workdirMaxSizeMB)
				error = "Disk space limit is " + workdirMaxSizeMB + ", using " + RES_WORKDIR_SIZE;

			// check memory usage
			if (jobMaxMemoryMB != 0 && RES_VMEM.doubleValue() > jobMaxMemoryMB)
				error = "Memory usage limit is " + jobMaxMemoryMB + ", using " + RES_VMEM;

			// cpu
			final long time = System.currentTimeMillis();

			if (prevTime != 0 && prevTime + (20 * 60 * 1000) < time && RES_CPUTIME.equals(prevCpuTime))
				error = "The job hasn't used the CPU for 20 minutes";
			else {
				prevCpuTime = RES_CPUTIME;
				prevTime = time;
			}

		}
		catch (final IOException e) {
			logger.log(Level.WARNING, "Problem with the monitoring objects: " + e.toString());
		}
		catch (final NoSuchElementException e) {
			logger.log(Level.WARNING, "Warning: an error occurred reading monitoring data:  " + e.toString());
		}
		catch (final NullPointerException e) {
			logger.log(Level.WARNING, "JobInfo or DiskInfo monitor are now null. Did the JobWrapper terminate?: " + e.toString());
		}
		catch (final NumberFormatException e) {
			logger.log(Level.WARNING, "Unable to continue monitoring: " + e.toString());
		}

		return error;
	}

	private long ttlForJob() {
		final Integer iTTL = jdl.getInteger("TTL");

		int ttl = (iTTL != null ? iTTL.intValue() : 3600);
		commander.q_api.putJobLog(queueId, "trace", "Job asks to run for " + ttl + " seconds");
		ttl += 300; // extra time (saving)

		final String proxyttl = jdl.gets("ProxyTTL");
		if (proxyttl != null) {
			ttl = ((Integer) siteMap.get("TTL")).intValue() - 600;
			commander.q_api.putJobLog(queueId, "trace", "ProxyTTL enabled, running for " + ttl + " seconds");
		}

		return ttl;
	}

	private boolean createWorkDir() {
		logger.log(Level.INFO, "Creating sandbox directory");

		jobWorkdir = String.format("%s%s%d", workdir, defaultOutputDirPrefix, Long.valueOf(queueId));

		tempDir = new File(jobWorkdir);
		if (!tempDir.exists()) {
			final boolean created = tempDir.mkdirs();
			if (!created) {
				logger.log(Level.INFO, "Workdir does not exist and can't be created: " + jobWorkdir);
				return false;
			}
			File tempTmpDir = new File(jobWorkdir + "/tmp");
			tempTmpDir.mkdir();
		}

		commander.q_api.putJobLog(queueId, "trace", "Created workdir: " + jobWorkdir);

		// chdir
		System.setProperty("user.dir", jobWorkdir);

		return true;
	}

	private void setupJobWrapperLogging() {
		Properties props = new Properties();
		try {
			ExtProperties ep = ConfigUtils.getConfiguration("logging");

			props = ep.getProperties();

			props.setProperty("java.util.logging.FileHandler.pattern", jobWrapperLogDir);

			logger.log(Level.INFO, "Logging properties loaded for the JobWrapper");
		}
		catch (@SuppressWarnings("unused") final Exception e) {

			logger.log(Level.INFO, "Logging properties for JobWrapper not found.");
			logger.log(Level.INFO, "Using fallback logging configurations for JobWrapper");

			props.put("handlers", "java.util.logging.FileHandler");
			props.put("java.util.logging.FileHandler.pattern", jobWrapperLogDir);
			props.put("java.util.logging.FileHandler.limit", "0");
			props.put("java.util.logging.FileHandler.count", "1");
			props.put("alien.log.WarningFileHandler.append", "true");
			props.put("java.util.logging.FileHandler.formatter", "java.util.logging.SimpleFormatter");
			props.put(".level", "FINEST");
			props.put("lia.level", "WARNING");
			props.put("lazyj.level", "WARNING");
			props.put("apmon.level", "WARNING");
			props.put("alien.level", "FINEST");
			props.put("alien.monitoring.Monitor.level", "WARNING");
			props.put("use_java_logger", "true");
		}

		try (FileOutputStream str = new FileOutputStream(jobWorkdir + "/logging.properties")) {
			props.store(str, null);
		}
		catch (IOException e1) {
			logger.log(Level.WARNING, "Failed to configure JobWrapper logging", e1);
		}
	}

	private void changeJobStatus(final JobStatus newStatus, HashMap<String, Object> extrafields) {
		TaskQueueApiUtils.setJobStatus(queueId, newStatus, extrafields);
		return;
	}

	private String readWrapperStatus() {
		try {
			return Files.readString(Paths.get(jobWorkdir + "/.jobstatus"));
		}
		catch (IOException e) {
			logger.log(Level.WARNING, "Attempt to read job status failed. Ignoring: " + e.toString());
			return "";
		}
	}

	private static int convertStringUnitToIntegerMB(String unit, String number) {
		switch (unit) {
			case "KB":
				return Integer.parseInt(number) / 1024;
			case "GB":
				return Integer.parseInt(number) * 1024;
			default: // MB
				return Integer.parseInt(number);
		}
	}

	/**
	 * Get LhcbMarks, using a specialized script in CVMFS
	 * 
	 * @return script output, or null in case of error
	 */
	public static Float getLhcbMarks() {
		if (lhcbMarks > 0)
			return Float.valueOf(lhcbMarks);

		final File lhcbMarksScript = new File(CVMFS.getLhcbMarksScript());

		if (!lhcbMarksScript.exists()) {
			logger.log(Level.WARNING, "Script for lhcbMarksScript not found in: " + lhcbMarksScript.getAbsolutePath());
			return null;
		}

		try {
			String out = ExternalProcesses.getCmdOutput(lhcbMarksScript.getAbsolutePath(), true, 300L, TimeUnit.SECONDS);
			out = out.substring(out.lastIndexOf(":") + 1);
			lhcbMarks = Float.parseFloat(out);
			return Float.valueOf(lhcbMarks);
		}
		catch (Exception e) {
			logger.log(Level.WARNING, "An error occurred while attempting to run process cleanup: ", e);
			return null;
		}
	}
	
	/**
	 * 
	 * Identifies job payload in list of child PIDs
	 * 
	 * @param childPIDs
	 * @return job payload PID
	 */
	private int getPayloadPid(Vector<Integer> childPIDs) {
		ArrayList<Integer> javaProcs = new ArrayList<Integer>();

		try {
			final Process getJavaProcs = Runtime.getRuntime().exec("pgrep java");
			getJavaProcs.waitFor();
			Scanner cmdScanner = new Scanner(getJavaProcs.getInputStream());
			while (cmdScanner.hasNext()) {
				javaProcs.add(Integer.parseInt(cmdScanner.next()));
			}
			cmdScanner.close();
		}
		catch (Exception e) {
			logger.log(Level.WARNING, "Could not get PIDs to identify payload");
		}

		for(int i=0; i < childPIDs.size(); i++) {
			for(int j=0; j < javaProcs.size(); j++) {
				if(childPIDs.get(i).equals(javaProcs.get(j)))
						return childPIDs.get(i+1); //the first java PID encountered will be the wrapper, the next will be the payload
			}
		}
		return 0;
	}
}
