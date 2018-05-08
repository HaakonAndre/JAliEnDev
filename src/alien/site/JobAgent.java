package alien.site;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.ProcessBuilder.Redirect;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import alien.api.JBoxServer;
import alien.api.aaa.TokenCertificateType;
import alien.api.catalogue.CatalogueApiUtils;
import alien.api.taskQueue.GetMatchJob;
import alien.api.taskQueue.TaskQueueApiUtils;
import alien.catalogue.FileSystemUtils;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.MonitoringObject;
import alien.shell.commands.JAliEnCOMMander;
import alien.site.packman.PackMan;
import alien.taskQueue.JDL;
import alien.taskQueue.Job;
import alien.taskQueue.JobStatus;
import alien.user.AliEnPrincipal;
import alien.user.JAKeyStore;
import alien.user.UserFactory;
import apmon.ApMon;
import apmon.ApMonException;
import apmon.ApMonMonitoringConstants;
import apmon.BkThread;
import apmon.MonitoredJob;

/**
 * Gets matched jobs, and launches JobWrapper for executing them
 */
public class JobAgent implements MonitoringObject, Runnable {

  // Folders and files
  private static final String defaultOutputDirPrefix = "/jalien-job-";
  private String jobWorkdir = "";

  // Variables passed through VoBox environment
  private final Map<String, String> env = System.getenv();
  private final String ce;
  private int origTtl;

  // Job variables
  private JDL jdl = null;
  private long queueId;
  private String jobToken;
  private String username;
  private String tokenCert;
  private String tokenKey;
  private String jobAgentId = "";
  private String workdir = null;
  private HashMap<String, Object> matchedJob = null;
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
  private final int pid;
  private final JAliEnCOMMander commander = JAliEnCOMMander.getInstance();
  private static final HashMap<String, Integer> jaStatus = new HashMap<>();
  private final String containerPlatform;

  static {
    jaStatus.put("REQUESTING_JOB", Integer.valueOf(1));
    jaStatus.put("INSTALLING_PKGS", Integer.valueOf(2));
    jaStatus.put("JOB_STARTED", Integer.valueOf(3));
    jaStatus.put("RUNNING_JOB", Integer.valueOf(4));
    jaStatus.put("DONE", Integer.valueOf(5));
    jaStatus.put("ERROR_HC", Integer.valueOf(-1)); // error in getting host
    // classad
    jaStatus.put("ERROR_IP", Integer.valueOf(-2)); // error installing
    // packages
    jaStatus.put("ERROR_GET_JDL", Integer.valueOf(-3)); // error getting jdl
    jaStatus.put("ERROR_JDL", Integer.valueOf(-4)); // incorrect jdl
    jaStatus.put("ERROR_DIRS", Integer.valueOf(-5)); // error creating
    // directories, not
    // enough free space
    // in workdir
    jaStatus.put("ERROR_START", Integer.valueOf(-6)); // error forking to
    // start job
  }

  private final int jobagent_requests = 1; // TODO: restore to 5

  /**
   * logger object
   */
  static transient final Logger logger = ConfigUtils.getLogger(JobAgent.class.getCanonicalName());

  /**
   * ML monitor object
   */
  static transient final Monitor monitor = MonitorFactory.getMonitor(JobAgent.class.getCanonicalName());
  /**
   * ApMon sender
   */
  static transient final ApMon apmon = MonitorFactory.getApMonSender();

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
    containerPlatform = env.get("USE_CONTAINERS");

    String DN = commander.getUser().getUserCert()[0].getSubjectDN().toString();

    System.err.println("We have the following DN :" + DN);

    totalJobs = 0;

    siteMap = (new SiteMap()).getSiteParameters(env);

    hostName = (String) siteMap.get("Host");
    // alienCm = (String) siteMap.get("alienCm");
    packMan = (PackMan) siteMap.get("PackMan");

    if (env.containsKey("ALIEN_JOBAGENT_ID"))
      jobAgentId = env.get("ALIEN_JOBAGENT_ID");
    pid = Integer.parseInt(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);

    workdir = (String) siteMap.get("workdir");

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
  }

  @Override
  public void run() {

    logger.log(Level.INFO, "Starting JobAgent in " + hostName);

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
        if (matchedJob != null)
          System.out.println(matchedJob.toString());

        if (matchedJob != null && !matchedJob.containsKey("Error")) {
          jdl = new JDL(Job.sanitizeJDL((String) matchedJob.get("JDL")));
          queueId = ((Long) matchedJob.get("queueId")).longValue();
          username = (String) matchedJob.get("User");
          jobToken = (String) matchedJob.get("jobToken"); //TokenCertificate TokenKey 
          tokenCert = (String) matchedJob.get("TokenCertificate");
          tokenKey = (String) matchedJob.get("TokenKey");

          // TODO: commander.setUser(username);
          // commander.setSite(site);

          System.out.println(jdl.getExecutable());
          System.out.println(username);
          System.out.println(queueId);
          System.out.println(jobToken);       

          // process payload
          handleJob();

          cleanup();
        }
        else {
          if (matchedJob != null && matchedJob.containsKey("Error")) {
            logger.log(Level.INFO, (String) matchedJob.get("Error"));

            if (Integer.valueOf(3).equals(matchedJob.get("Code"))) {
              @SuppressWarnings("unchecked")
              final ArrayList<String> packToInstall = (ArrayList<String>) matchedJob.get("Packages");
              monitor.sendParameter("ja_status", getJaStatusForML("INSTALLING_PKGS"));
              installPackages(packToInstall);
            }
          }
          else
            logger.log(Level.INFO, "We didn't get anything back. Nothing to run right now. Idling 20secs zZz...");

          try {
            // TODO?: monitor.sendBgMonitoring
            Thread.sleep(20000);
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

  private void handleJob() {

    totalJobs++;
    try {
      logger.log(Level.INFO, "Started JA with: " + jdl);

      commander.q_api.putJobLog(queueId, "trace", "Job preparing to run in: " + hostName);

      changeStatus(JobStatus.STARTED);      

      //Set up constraints
      getMemoryRequirements();      

      final int selfProcessID = MonitorFactory.getSelfProcessID();      
      final List<String> launchCommand = generateLaunchCommand(selfProcessID);

      commander.q_api.putJobLog(queueId, "trace", "Starting JobWrapper");

      launchJobWrapper(launchCommand,true);   

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
  private void cleanup() {
    //    System.out.println("Cleaning up after execution...Removing sandbox: " + jobWorkdir);
    // Remove sandbox, TODO: use Java builtin
    //    SystemCommand.bash("rm -rf " + jobWorkdir); //TODO: Remove after testing
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
  }

  private Map<String, String> installPackages(final ArrayList<String> packToInstall) {
    Map<String, String> ok = null;

    for (final String pack : packToInstall) {
      ok = packMan.installPackage(username, pack, null);
      if (ok == null) {
        logger.log(Level.INFO, "Error installing the package " + pack);
        monitor.sendParameter("ja_status", "ERROR_IP");
        System.out.println("Error installing " + pack);
        System.exit(1);
      }
    }
    return ok;
  }

  private static Integer getJaStatusForML(final String status) {
    final Integer value = jaStatus.get(status);

    return value != null ? value : Integer.valueOf(0);
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
    int timeleft = origTtl - time_subs - 300;

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

    timeleft=87000; //TODO: Remove
    if (timeleft <= 0) {
      logger.log(Level.INFO, "There is not enough time left: " + timeleft);
      return false; //TODO: Put back
    }

    siteMap.put("Disk", Long.valueOf(space));
    siteMap.put("TTL", Integer.valueOf(timeleft));

    return true;
  }

  /**
   * @return the time in seconds that proxy is still valid for
   */
  private int getRemainingProxyTime() {
    // TODO: to be modified!
    return origTtl;
  }

  private void getMemoryRequirements() {
    // Sandbox size
    final String workdirMaxSize = jdl.gets("Workdirectorysize");

    if (workdirMaxSize != null) {
      final Pattern p = Pattern.compile("\\p{L}");
      final Matcher m = p.matcher(workdirMaxSize);
      if (m.find()) {
        final String number = workdirMaxSize.substring(0, m.start());
        final String unit = workdirMaxSize.substring(m.start());

        switch (unit) {
        case "KB":
          workdirMaxSizeMB = Integer.parseInt(number) / 1024;
          break;
        case "GB":
          workdirMaxSizeMB = Integer.parseInt(number) * 1024;
          break;
        default: // MB
          workdirMaxSizeMB = Integer.parseInt(number);
        }
      }
      else
        workdirMaxSizeMB = Integer.parseInt(workdirMaxSize);
      commander.q_api.putJobLog(queueId, "trace", "Disk requested: " + workdirMaxSizeMB);
    }
    else
      workdirMaxSizeMB = 0;

    // Memory use
    final String maxmemory = jdl.gets("Memorysize");

    if (maxmemory != null) {
      final Pattern p = Pattern.compile("\\p{L}");
      final Matcher m = p.matcher(maxmemory);
      if (m.find()) {
        final String number = maxmemory.substring(0, m.start());
        final String unit = maxmemory.substring(m.start());

        switch (unit) {
        case "KB":
          jobMaxMemoryMB = Integer.parseInt(number) / 1024;
          break;
        case "GB":
          jobMaxMemoryMB = Integer.parseInt(number) * 1024;
          break;
        default: // MB
          jobMaxMemoryMB = Integer.parseInt(number);
        }
      }
      else
        jobMaxMemoryMB = Integer.parseInt(maxmemory);
      commander.q_api.putJobLog(queueId, "trace", "Memory requested: " + jobMaxMemoryMB);
    }
    else
      jobMaxMemoryMB = 0;

  }

  /**
   * @param args
   * @throws IOException
   */
  public static void main(final String[] args) throws IOException {
    final JobAgent jao = new JobAgent();
    jao.run();    

  }

  /**
   * @param newStatus
   */
  public void changeStatus(final JobStatus newStatus) {
    final HashMap<String, Object> extrafields = new HashMap<>();
    extrafields.put("exechost", this.ce);
    // if final status with saved files, we set the path
    if (newStatus == JobStatus.DONE || newStatus == JobStatus.DONE_WARN || newStatus == JobStatus.ERROR_E || newStatus == JobStatus.ERROR_V) {
      extrafields.put("path", getJobOutputDir());

      TaskQueueApiUtils.setJobStatus(queueId, newStatus, extrafields);
    }
    else
      if (newStatus == JobStatus.RUNNING) {
        extrafields.put("spyurl", hostName + ":" + JBoxServer.getPort());
        extrafields.put("node", hostName);

        TaskQueueApiUtils.setJobStatus(queueId, newStatus, extrafields);
      }
      else
        TaskQueueApiUtils.setJobStatus(queueId, newStatus);

    jobStatus = newStatus;

    return;
  }
  
  /**
   * @return job output dir (as indicated in the JDL if OK, or the recycle path if not)
   */
  public String getJobOutputDir() {
    String outputDir = jdl.getOutputDir();

    if (jobStatus == JobStatus.ERROR_V || jobStatus == JobStatus.ERROR_E)
      outputDir = FileSystemUtils.getAbsolutePath(username, null, "~" + "recycle/" + defaultOutputDirPrefix + queueId);
    else
      if (outputDir == null)
        outputDir = FileSystemUtils.getAbsolutePath(username, null, "~" + defaultOutputDirPrefix + queueId);

    return outputDir;
  }

  /**
   * 
   * @param processID
   * @return Command w/arguments for starting the JobWrapper, based on the command used for the JobAgent
   */
  public List<String> generateLaunchCommand(int processID) throws InterruptedException {

    try {
      final Scanner scanner = new Scanner(new File("/proc/" + processID + "/cmdline"));
      List<String> cmd = new ArrayList<String>();

      while (scanner.hasNext()) {
        cmd.add(scanner.useDelimiter("\0").next());

        if (cmd.get(cmd.size() - 1).contains("JobAgent"))
          cmd.set(cmd.size() - 1, "alien.site.JobWrapper");
      }
      scanner.close();

      return cmd;
    } catch (final IOException e) {
      System.err.println("Could not generate JobWrapper launch command: " + e.getStackTrace());
      return null;
    }
  } 

  /* 
  /**
   * 
   * For testing purposes only. DO NOT USE. 
   * 
   * @param processID
   * @return Command w/arguments for starting the JobWrapper, based on the command used for the JobAgent
   */
  /* public List<String> generateLaunchCommand(int processID) throws InterruptedException {

    List<String> cmd = new ArrayList<String>();

    switch(containerPlatform){
    case "SINGULARITY": Collections.addAll(cmd, "singularity", "exec", "/tmp/mycontainer", "java", "-cp");
    case "DOCKER": Collections.addAll(cmd, "docker", "run", "mycontainer", "java", "-cp");

    }

    try {
      final Scanner scanner = new Scanner(new File("/proc/" + processID + "/cmdline"));

      while (scanner.hasNext()) {
        cmd.add(scanner.useDelimiter("\0").next());

        if (cmd.get(cmd.size() - 1).contains("JobAgent"))
          cmd.set(cmd.size() - 1, "alien.site.JobWrapper");
      }
      scanner.close();

      return cmd;
    } catch (final IOException e) {
      System.err.println("Could not generate JobWrapper launch command: " + e.getStackTrace());
      return null;
    }
  } */

  public int launchJobWrapper(List<String> launchCommand, boolean monitorJob){

    System.err.println("Launching jobwrapper using the command: " + launchCommand.toString());

    final ProcessBuilder pBuilder = new ProcessBuilder(launchCommand);
    pBuilder.redirectError(Redirect.appendTo(new File("/tmp", "stderr"))); //TODO: Remove after testing

    final Process p;

    // stdin from the viewpoint of the wrapper
    OutputStream stdin;
    OutputStream stdout;

    ObjectOutputStream stdinObj;
    ObjectOutputStream stdoutObj;

    System.out.println("We want to change the current user \"" + commander.getUsername() + "\" to: " + username);

    // TODO: Cleanup after testing
    try {

      p = pBuilder.start();

      stdin = p.getOutputStream();
      stdinObj = new ObjectOutputStream(stdin);

      stdinObj.writeObject(jdl);
      stdinObj.flush();
      stdinObj.reset();

      stdinObj.writeObject(username);
      stdinObj.flush();
      stdinObj.reset();

      stdinObj.writeObject(queueId);
      stdinObj.flush();
      stdinObj.reset();

      stdinObj.writeObject(tokenCert);
      stdinObj.flush();
      stdinObj.reset();

      stdinObj.writeObject(tokenKey);
      stdinObj.flush();
      stdinObj.reset();

      stdinObj.close();
      stdin.close();

    } catch (final IOException ioe) {
      System.out.println("Exception running " + launchCommand + " : " + ioe.getMessage());
      return -2;
    }

    jobWorkdir = String.format("%s%s%d", workdir, defaultOutputDirPrefix, Long.valueOf(queueId)); // not
    // ideal

    mj = new MonitoredJob(pid, jobWorkdir, ce, hostName);
    final Vector<Integer> child = mj.getChildren();
    if (child == null || child.size() <= 1) {
      System.err.println("Can't get children. Failed to execute? " + launchCommand.toString()
      + " child: " + child);
      return -1;
    }
    System.out.println("Child: " + child.get(1).toString());

    //TODO: Creates an exception when JobWrapper has terminated. This is caught, but perhaps provide more info than just the exit code?  
    if (monitorJob) {
      payloadPID = child.get(1).intValue();
      apmon.addJobToMonitor(payloadPID, jobWorkdir, ce, hostName); // Will also keep track of the sub-processes (payload also, not just jobwrapper)
      mj = new MonitoredJob(payloadPID, jobWorkdir, ce, hostName);
      final String fs = checkProcessResources();
      if (fs == null)
        sendProcessResources();
    }

    final Timer t = new Timer();
    t.schedule(new TimerTask() {
      @Override
      public void run() {
        p.destroy();
      }
    }, TimeUnit.MILLISECONDS.convert(ttlForJob(), TimeUnit.SECONDS)); // TODO: ttlForJob

    boolean processNotFinished = true;
    int code = 0;

    int monitor_loops = 0;
    try {
      while (processNotFinished)
        try {
          Thread.sleep(5 * 1000); // TODO: Change to 60
          code = p.exitValue();
          processNotFinished = false;
          System.out.println("JobWrapper exited with code: " + code);
        } catch (final IllegalThreadStateException e) {
          logger.log(Level.WARNING, "Exception waiting for the process to finish", e);
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
      System.out.println(
          "Interrupted while waiting for the JobWrapper to finish execution: " + ie.getMessage());
      return -2;
    } finally {
      t.cancel();
    }
  }  

  private void sendProcessResources() {
    // runtime(date formatted) start cpu(%) mem cputime rsz vsize ncpu
    // cpufamily cpuspeed resourcecost maxrss maxvss ksi2k
    final String procinfo = String.format("%s %d %.2f %.2f %.2f %.2f %.2f %d %s %s %s %.2f %.2f 1000", RES_FRUNTIME, RES_RUNTIME, RES_CPUUSAGE, RES_MEMUSAGE, RES_CPUTIME, RES_RMEM, RES_VMEM,
        RES_NOCPUS, RES_CPUFAMILY, RES_CPUMHZ, RES_RESOURCEUSAGE, RES_RMEMMAX, RES_VMEMMAX);
    System.out.println("+++++ Sending resources info +++++");
    System.out.println(procinfo);

    commander.q_api.putJobLog(queueId, "proc", procinfo); //Same commander - Yes! 
  }

  private String checkProcessResources() {
    String error = null;
    System.out.println("Checking resources usage");

    try {
      final HashMap<Long, Double> jobinfo = mj.readJobInfo();
      final HashMap<Long, Double> diskinfo = mj.readJobDiskUsage();

      if (jobinfo == null || diskinfo == null) {
        System.err.println("JobInfo or DiskInfo monitor null");
        return "Not available";
      }

      // getting cpu, memory and runtime info
      RES_WORKDIR_SIZE = diskinfo.get(ApMonMonitoringConstants.LJOB_WORKDIR_SIZE);
      RES_VMEM = Double.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_VIRTUALMEM).doubleValue() / 1024);
      RES_RMEM = Double.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_RSS).doubleValue() / 1024);
      RES_CPUTIME = jobinfo.get(ApMonMonitoringConstants.LJOB_CPU_TIME);
      RES_CPUUSAGE = Double.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_CPU_USAGE).doubleValue());
      RES_RUNTIME = Long.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_RUN_TIME).longValue());
      RES_MEMUSAGE = jobinfo.get(ApMonMonitoringConstants.LJOB_MEM_USAGE);
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
      else
        if (RES_RUNTIME.longValue() < 3600)
          RES_FRUNTIME = String.format("00:%02d:%02d", Long.valueOf(RES_RUNTIME.longValue() / 60), Long.valueOf(RES_RUNTIME.longValue() % 60));
        else
          RES_FRUNTIME = String.format("%02d:%02d:%02d", Long.valueOf(RES_RUNTIME.longValue() / 3600), Long.valueOf((RES_RUNTIME.longValue() - (RES_RUNTIME.longValue() / 3600) * 3600) / 60),
              Long.valueOf((RES_RUNTIME.longValue() - (RES_RUNTIME.longValue() / 3600) * 3600) % 60));

      // check disk usage
      if (workdirMaxSizeMB != 0 && RES_WORKDIR_SIZE.doubleValue() > workdirMaxSizeMB)
        error = "Disk space limit is " + workdirMaxSizeMB + ", using " + RES_WORKDIR_SIZE;

      // check disk usage
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

    } catch (final IOException e) {
      System.out.println("Problem with the monitoring objects: " + e.toString());
    } catch (final NoSuchElementException e){
      System.out.println("JobWrapper has terminated. Ending monitoring");
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

  @Override
  public void fillValues(final Vector<String> paramNames, final Vector<Object> paramValues) {
    if (queueId > 0) {
      paramNames.add("jobID");
      paramValues.add(Double.valueOf(queueId));

      paramNames.add("statusID");
      paramValues.add(Double.valueOf(jobStatus.getAliEnLevel()));
    }
  }

}