package alien.site;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.security.cert.X509Certificate;

import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcessBuilder;
import alien.api.JBoxServer;
import alien.api.catalogue.CatalogueApiUtils;
import alien.api.taskQueue.GetMatchJob;
import alien.api.taskQueue.TaskQueueApiUtils;
import alien.catalogue.FileSystemUtils;
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
import alien.taskQueue.JobSigner;
import alien.taskQueue.JobStatus;
import alien.taskQueue.JobSubmissionException;
import alien.user.AliEnPrincipal;

/**
 * @author mmmartin, ron
 * @since Apr 1, 2015
 */
public class JobAgent extends Thread {

	// Folders and files
	private static final String tempDirPrefix = "jAliEn.JobAgent.tmp";
	private File tempDir = null;
	private static final String defaultOutputDirPrefix = "~/jalien-job-";

	// Variables passed through VoBox environment
	private final Map<String, String> env = System.getenv();
	private final String site;
	private final String ce;
	private int origTtl;

	// Job variables
	private JDL jdl = null;
	private String sjdl = null;
	private Job job = null;
	private final AliEnPrincipal user = null;
	private int queueId;
	private int jobToken;
	private String username;
	private String jobAgentId = "";
	private String workdir = null;
	private HashMap<String, Object> matchedJob = null;
	private String partition;
	private String ceRequirements = "";
	private List<String> packages;
	private List<String> installedPackages;
	private HashMap<String, Object> siteMap = new HashMap<>();

	private int totalJobs;
	private final long jobAgentStartTime = new java.util.Date().getTime();

	// Other
	private PackMan packMan = null;
	private final CatalogueApiUtils c_api = null;
	private String hostName = null;
	private String alienCm = null;
	JAliEnCOMMander commander = JAliEnCOMMander.getInstance();

	static transient final Logger logger = ConfigUtils.getLogger(JobAgent.class.getCanonicalName());

	static transient final Monitor monitor = MonitorFactory.getMonitor(JobAgent.class.getCanonicalName());

	/**
	 */
	public JobAgent() {
		site = env.get("site"); // or ConfigUtils.getConfig().gets("alice_close_site").trim();
		ce = env.get("CE");

		totalJobs = 0;

		partition = "";
		if (env.containsKey("partition"))
			partition = env.get("partition");

		if (env.containsKey("TTL")) {
			origTtl = Integer.parseInt(env.get("TTL"));
		} else {
			origTtl = 12 * 3600;
		}

		if (env.containsKey("cerequirements")) {
			ceRequirements = env.get("cerequirements");
		}

		try {
			hostName = InetAddress.getLocalHost().getCanonicalHostName();
		} catch (UnknownHostException e) {
			System.err.println("Couldn't get hostname");
			e.printStackTrace();
		}

		alienCm = hostName;
		if (env.containsKey("ALIEN_CM_AS_LDAP_PROXY")) {
			alienCm = env.get("ALIEN_CM_AS_LDAP_PROXY");
		}

		if (env.containsKey("ALIEN_JOBAGENT_ID")) {
			jobAgentId = env.get("ALIEN_JOBAGENT_ID");
		}
		jobAgentId = jobAgentId + "_" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0];

		workdir = env.get("HOME");
		if (env.containsKey("WORKDIR"))
			workdir = env.get("WORKDIR");
		if (env.containsKey("TMPBATCH"))
			workdir = env.get("TMPBATCH");

		siteMap = getSiteParameters();
	}

	@Override
	public void run() {

		logger.log(Level.INFO, "Starting JobAgent in " + hostName);

		// We start, if needed, the node JBox
		// Does it check a previous one is already running?
		try {
			System.out.println("Trying to start JBox");
			JBoxServer.startJBoxService(0);
		} catch (Exception e) {
			System.err.println("Unable to start JBox.");
			e.printStackTrace();
		}

		int count = 1; // to modify
		while (count > 0) {
			if (!updateDynamicParameters())
				break;

			System.out.println(siteMap.toString());

			try {
				logger.log(Level.INFO, "Trying to get a match...");

				GetMatchJob jobMatch = commander.q_api.getMatchJob(siteMap);
				matchedJob = jobMatch.getMatchJob();

				if (matchedJob != null && !matchedJob.containsKey("Error")) {
					// jdl = new JDL((String) matchedJob.get("JDL"));
					// queueId = (int) matchedJob.get("queueId");
					// jobToken = (int) matchedJob.get("jobToken");
					// username = (String) matchedJob.get("User");
					//
					// System.out.println("Matched job received with:");
					// System.out.println("JDL:\n"+jdl.toString());
					// System.out.println("queueId:\n"+queueId);
					// System.out.println("jobToken:\n"+jobToken);
					// System.out.println("User:\n"+username);
					System.out.println(matchedJob.toString());
					System.exit(0);

					// handleJob(j);
					totalJobs++;
				} else {
					if (matchedJob != null && matchedJob.containsKey("Error"))
						logger.log(Level.INFO, (String) matchedJob.get("Error"));
					else
						logger.log(Level.INFO, "Nothing to run right now. Idling 10secs zZz...");

					try {
						sleep(10000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			} catch (Exception e) {
				logger.log(Level.INFO, "Error getting a matching job: " + e);
			}
			count--;
		}

		logger.log(Level.INFO, "JobAgent finished, id: " + jobAgentId + " totalJobs: " + totalJobs);
		System.exit(0);
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
		ArrayList<String> users = new ArrayList<>();
		if (!ceRequirements.equals("")) {
			Pattern p = Pattern.compile("\\s*other.user\\s*==\\s*\"(\\w+)\"");
			Matcher m = p.matcher(ceRequirements);
			while (m.find()) {
				users.add(m.group(1));
			}
		}
		// setting entries for the map object
		siteMap.put("TTL", Integer.valueOf(origTtl));

		// We prepare the packages for direct matching
		String packs = ",";
		Collections.sort(packages);
		for (String pack : packages) {
			packs += pack + ",,";
		}
		
		packs = packs.substring(0, packs.length() - 1);

		String instpacks = ",";
		Collections.sort(installedPackages);
		for (String pack : installedPackages) {
			instpacks += pack + ",,";
		}

		instpacks = instpacks.substring(0, instpacks.length() - 1);

		siteMap.put("Platform", ConfigUtils.getPlatform());
		siteMap.put("Packages", packs);
		siteMap.put("InstalledPackages", instpacks);
		siteMap.put("CE", ce);
		siteMap.put("Site", site);
		siteMap.put("Partition", partition);
		siteMap.put("Users", users);
		siteMap.put("Host", alienCm);
		siteMap.put("Disk", Long.valueOf(new File(workdir).getFreeSpace() / 1024));

		return siteMap;
	}

	private static PackMan getPackman() {
		return new CVMFS();
	}

	/**
	 * updates jobagent parameters that change between job requests
	 * 
	 * @return false if we can't run because of current conditions, true if positive
	 */
	private boolean updateDynamicParameters() {
		logger.log(Level.INFO, "Updating dynamic parameters of jobAgent map");

		// free disk recalculation
		long space = new File(workdir).getFreeSpace() / 1024;

		// ttl recalculation
		long jobAgentCurrentTime = new java.util.Date().getTime();
		int time_subs = (int) (jobAgentCurrentTime - jobAgentStartTime);
		int timeleft = origTtl - time_subs;

		logger.log(Level.INFO, "Still have " + timeleft + " seconds to live (" + jobAgentCurrentTime + "-" + jobAgentStartTime + "=" + time_subs + ")");

		// we check if the proxy timeleft is smaller than the ttl itself
		int proxy = getRemainingProxyTime();
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

	private void handleJob(final Job thejob) {
		this.job = thejob;
		try {
			sjdl = thejob.getOriginalJDL();
			System.out.println("started JA with: " + sjdl);
			jdl = new JDL(thejob.getJDL());

			if (verifiedJob()) {
				TaskQueueApiUtils.setJobStatus(thejob.queueId, JobStatus.STARTED);
				if (createTempDir())
					if (getInputFiles()) {
						if (execute())
							if (uploadOutputFiles())
								System.out.println("Job sucessfully executed.");
					} else {
						System.out.println("Could not get input files.");
						TaskQueueApiUtils.setJobStatus(thejob.queueId, JobStatus.ERROR_IB);
					}
			} else
				TaskQueueApiUtils.setJobStatus(thejob.queueId, JobStatus.ERROR_VER);
		} catch (final IOException e) {
			System.err.println("Unable to get JDL from Job.");
			e.printStackTrace();
		}
	}

	private boolean verifiedJob() {
		try {
			return JobSigner.verifyJobToRun(new X509Certificate[] { job.userCertificate }, sjdl);

		} catch (final InvalidKeyException e) {
			e.printStackTrace();
		} catch (final NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (final SignatureException e) {
			e.printStackTrace();
		} catch (final KeyStoreException e) {
			e.printStackTrace();
		} catch (final JobSubmissionException e) {
			e.printStackTrace();
		}
		return false;
	}

	private boolean getInputFiles() {

		boolean gotAllInputFiles = true;
		if (jdl.getInputFiles() != null && jdl.getInputFiles().size() > 0)
			for (final String slfn : jdl.getInputFiles()) {
				File localFile;
				try {
					localFile = new File(tempDir.getCanonicalFile() + "/" + slfn.substring(slfn.lastIndexOf('/') + 1));

					System.out.println("Getting input file into local file: " + tempDir.getCanonicalFile() + "/" + slfn.substring(slfn.lastIndexOf('/') + 1));

					System.out.println("Getting input file: " + slfn);
					final LFN lfn = c_api.getLFN(slfn);
					System.out.println("Getting input file lfn: " + lfn);
					final List<PFN> pfns = c_api.getPFNsToRead(lfn, null, null);
					System.out.println("Getting input file pfns: " + pfns);

					for (final PFN pfn : pfns) {

						final List<Protocol> protocols = Transfer.getAccessProtocols(pfn);
						for (final Protocol protocol : protocols) {

							localFile = protocol.get(pfn, localFile);
							break;

						}
						System.out.println("Suppossed to have input file: " + localFile.getCanonicalPath());
					}
					if (!localFile.exists())
						gotAllInputFiles = false;
				} catch (final IOException e) {
					e.printStackTrace();
					gotAllInputFiles = false;
				}
			}
		return gotAllInputFiles;

	}

	private boolean execute() {

		boolean ran = true;

		final LinkedList<String> command = new LinkedList<>();

		command.add(jdl.getExecutable());

		if (jdl.getArguments() != null)
			command.addAll(jdl.getArguments());

		System.out.println("we will run: " + command.toString());
		final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(command);

		pBuilder.returnOutputOnExit(true);

		pBuilder.directory(tempDir);

		pBuilder.timeout(24, TimeUnit.HOURS);

		pBuilder.redirectErrorStream(true);

		try {
			final ExitStatus exitStatus;

			TaskQueueApiUtils.setJobStatus(job.queueId, JobStatus.RUNNING);

			exitStatus = pBuilder.start().waitFor();

			if (exitStatus.getExtProcExitStatus() == 0) {

				final BufferedWriter out = new BufferedWriter(new FileWriter(tempDir.getCanonicalFile() + "/stdout"));
				out.write(exitStatus.getStdOut());
				out.close();
				final BufferedWriter err = new BufferedWriter(new FileWriter(tempDir.getCanonicalFile() + "/stderr"));
				err.write(exitStatus.getStdErr());
				err.close();

				System.out.println("we ran, stdout+stderr should be there now.");
			}

			System.out.println("A local cat on stdout: " + exitStatus.getStdOut());
			System.out.println("A local cat on stderr: " + exitStatus.getStdErr());

		} catch (final InterruptedException ie) {
			System.err.println("Interrupted while waiting for the following command to finish : " + command.toString());
			ran = false;
		} catch (final IOException e) {
			ran = false;
		}
		return ran;
	}

	private boolean uploadOutputFiles() {

		boolean uploadedAllOutFiles = true;
		boolean uploadedNotAllCopies = false;
		TaskQueueApiUtils.setJobStatus(job.queueId, JobStatus.SAVING);

		String outputDir = jdl.getOutputDir();

		if (outputDir == null)
			outputDir = defaultOutputDirPrefix + job.queueId;

		System.out.println("QueueID: " + job.queueId);

		System.out.println("Full catpath of outDir is: " + FileSystemUtils.getAbsolutePath(user.getName(), "~", outputDir));

		if (c_api.getLFN(FileSystemUtils.getAbsolutePath(user.getName(), null, outputDir)) != null) {
			System.err.println("OutputDir [" + outputDir + "] already exists.");
			return false;
		}

		final LFN outDir = c_api.createCatalogueDirectory(outputDir);

		if (outDir == null) {
			System.err.println("Error creating the OutputDir [" + outputDir + "].");
			uploadedAllOutFiles = false;
		} else
			for (final String slfn : jdl.getOutputFiles()) {
				File localFile;
				try {
					localFile = new File(tempDir.getCanonicalFile() + "/" + slfn);

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

						List<PFN> pfns = null;

						LFN lfn = null;
						lfn = c_api.getLFN(outDir.getCanonicalName() + slfn, true);

						lfn.size = size;
						lfn.md5 = md5;

						pfns = c_api.getPFNsToWrite(lfn, null, null, null, null);

						if (pfns != null) {
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
							if (!pfns.equals(pfnsok))
								if (pfnsok != null && pfnsok.size() > 0) {
									System.out.println("Only " + pfnsok.size() + " could be uploaded");
									uploadedNotAllCopies = true;
								} else {

									System.err.println("Upload failed, sorry!");
									uploadedAllOutFiles = false;
									break;
								}
						} else
							System.out.println("Couldn't get write envelopes for output file");
					} else
						System.out.println("Can't upload output file " + localFile.getName() + ", does not exist or has zero size.");

				} catch (final IOException e) {
					e.printStackTrace();
					uploadedAllOutFiles = false;
				}
			}
		if (uploadedNotAllCopies)
			TaskQueueApiUtils.setJobStatus(job.queueId, JobStatus.DONE_WARN);
		else if (uploadedAllOutFiles)
			TaskQueueApiUtils.setJobStatus(job.queueId, JobStatus.DONE);
		else
			TaskQueueApiUtils.setJobStatus(job.queueId, JobStatus.ERROR_SV);

		return uploadedAllOutFiles;
	}

	private boolean createTempDir() {

		final String tmpDirStr = System.getProperty("java.io.tmpdir");
		if (tmpDirStr == null) {
			System.err.println("System temp dir config [java.io.tmpdir] does not exist.");
			return false;
		}

		final File tmpDir = new File(tmpDirStr);
		if (!tmpDir.exists()) {
			final boolean created = tmpDir.mkdirs();
			if (!created) {
				System.err.println("System temp dir [java.io.tmpdir] does not exist and can't be created.");
				return false;
			}
		}

		int suffix = (int) System.currentTimeMillis();
		int failureCount = 0;
		do {
			tempDir = new File(tmpDir, tempDirPrefix + suffix % 10000);
			suffix++;
			failureCount++;
		} while (tempDir.exists() && failureCount < 50);

		if (tempDir.exists()) {
			System.err.println("Could not create temporary directory.");
			return false;
		}
		final boolean created = tempDir.mkdir();
		if (!created) {
			System.err.println("Could not create temporary directory.");
			return false;
		}

		return true;
	}

}
