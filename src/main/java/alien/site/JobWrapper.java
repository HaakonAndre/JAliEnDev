package alien.site;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.TomcatServer;
import alien.api.catalogue.CatalogueApiUtils;
import alien.api.taskQueue.TaskQueueApiUtils;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.XmlCollection;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.monitoring.MonitorFactory;
import alien.monitoring.MonitoringObject;
import alien.shell.commands.JAliEnCOMMander;
import alien.site.containers.Containerizer;
import alien.site.containers.ContainerizerFactory;
import alien.site.packman.CVMFS;
import alien.site.packman.PackMan;
import alien.taskQueue.JDL;
import alien.taskQueue.JobStatus;
import alien.user.JAKeyStore;
import alien.user.UserFactory;

/**
 * Job execution wrapper, running an embedded Tomcat server for in/out-bound communications
 */
public class JobWrapper implements MonitoringObject, Runnable {

	// Folders and files
	private final File currentDir = new File(Paths.get(".").toAbsolutePath().normalize().toString());
	private String defaultOutputDirPrefix;

	// Job variables
	/**
	 * @uml.property name="jdl"
	 * @uml.associationEnd
	 */
	private JDL jdl;
	private long queueId;
	private String username;
	private String tokenCert;
	private String tokenKey;
	private HashMap<String, Object> siteMap;
	private String ce;
	private String legacyToken;
	/**
	 * @uml.property name="jobStatus"
	 * @uml.associationEnd
	 */
	private JobStatus jobStatus;

	// Other
	/**
	 * @uml.property name="packMan"
	 * @uml.associationEnd
	 */
	private PackMan packMan;
	private String hostName;
	private final int pid;
	private String ceHost;
	/**
	 * @uml.property name="commander"
	 * @uml.associationEnd
	 */
	private final JAliEnCOMMander commander;

	/**
	 * @uml.property name="c_api"
	 * @uml.associationEnd
	 */
	private final CatalogueApiUtils c_api;

	/**
	 * logger object
	 */
	static final Logger logger = ConfigUtils.getLogger(JobWrapper.class.getCanonicalName());

	/**
	 * Streams for data transfer
	 */
	private ObjectInputStream inputFromJobAgent;

	/**
	 */
	@SuppressWarnings("unchecked")
	public JobWrapper() {

		pid = MonitorFactory.getSelfProcessID();

		try {
			inputFromJobAgent = new ObjectInputStream(System.in);
			jdl = (JDL) inputFromJobAgent.readObject();
			username = (String) inputFromJobAgent.readObject();
			queueId = ((Long) inputFromJobAgent.readObject()).longValue();
			tokenCert = (String) inputFromJobAgent.readObject();
			tokenKey = (String) inputFromJobAgent.readObject();
			ce = (String) inputFromJobAgent.readObject();
			siteMap = (HashMap<String, Object>) inputFromJobAgent.readObject();
			defaultOutputDirPrefix = (String) inputFromJobAgent.readObject();
			legacyToken = (String) inputFromJobAgent.readObject();

			logger.log(Level.INFO, "We received the following tokenCert: " + tokenCert);
			logger.log(Level.INFO, "We received the following tokenKey: " + tokenKey);
			logger.log(Level.INFO, "We received the following username: " + username);
			logger.log(Level.INFO, "We received the following CE " + ce);
		}
		catch (final IOException | ClassNotFoundException e) {
			logger.log(Level.SEVERE, "Error: Could not receive data from JobAgent" + e);
		}

		if ((tokenCert != null) && (tokenKey != null)) {
			try {
				JAKeyStore.createTokenFromString(tokenCert, tokenKey);
				logger.log(Level.INFO, "Token successfully created");
				JAKeyStore.loadKeyStore();
			}
			catch (final Exception e) {
				logger.log(Level.SEVERE, "Error. Could not load tokenCert and/or tokenKey" + e);
			}
		}

		hostName = (String) Objects.requireNonNullElse(siteMap.get("Host"), "");
		ceHost = (String) Objects.requireNonNullElse(siteMap.get("CEhost"), hostName);
		packMan = (PackMan) Objects.requireNonNullElse(siteMap.get("PackMan"), new CVMFS(""));

		commander = JAliEnCOMMander.getInstance();
		c_api = new CatalogueApiUtils(commander);

		logger.log(Level.INFO, "JobWrapper initialised. Running as the following user: " + commander.getUser().getName());
	}

	@Override
	public void run() {

		logger.log(Level.INFO, "Starting JobWrapper in " + hostName);

		// We start, if needed, the node Tomcat server
		// Does it check a previous one is already running?
		try {
			logger.log(Level.INFO, "Trying to start Tomcat");
			TomcatServer.startTomcatServer();
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Unable to start Tomcat." + e);
		}

		logger.log(Level.INFO, "Tomcat started");

		// process payload
		final int runCode = runJob();

		logger.log(Level.INFO, "JobWrapper has finished execution");

		if (runCode > 0)
			System.exit(0); // Positive runCodes originate from the payload. Ignore. All OK here as far as we're concerned.
		else
			System.exit(Math.abs(runCode));
	}

	private Map<String, String> installPackages(final ArrayList<String> packToInstall) {
		Map<String, String> ok = null;

		if (packMan == null) {
			logger.log(Level.WARNING, "Packman is null!");
			return ok;
		}

		for (final String pack : packToInstall) {
			ok = packMan.installPackage(username, pack, null);
			if (ok == null) {
				logger.log(Level.INFO, "Error installing the package " + pack);
				// monitor.sendParameter("ja_status", "ERROR_IP");
				logger.log(Level.SEVERE, "Error installing " + pack);
				System.exit(1);
			}
		}

		return ok;
	}

	private int runJob() {
		try {
			logger.log(Level.INFO, "Started JobWrapper for: " + jdl);

			changeStatus(JobStatus.STARTED);

			if (!getInputFiles()) {
				logger.log(Level.SEVERE, "Failed to get inputfiles");
				changeStatus(JobStatus.ERROR_IB);
				return -1;
			}

			// run payload
			final int execExitCode = execute();
			if (execExitCode != 0) {
				logger.log(Level.SEVERE, "Failed to run payload");

				if (execExitCode < 0)
					commander.q_api.putJobLog(queueId, "trace", "Failed to start execution of payload. Exit code: " + Math.abs(execExitCode));
				else
					commander.q_api.putJobLog(queueId, "trace", "Job started, but did not execute correctly: " + execExitCode);

				if (jdl.gets("OutputErrorE") != null)
					return uploadOutputFiles(true) ? execExitCode : -1;

				changeStatus(jobStatus);
				return execExitCode;
			}

			final int valExitCode = validate();
			if (valExitCode != 0) {
				logger.log(Level.SEVERE, "Validation failed");

				if (valExitCode < 0)
					commander.q_api.putJobLog(queueId, "trace", "Failed to start validation. Exit code: " + Math.abs(valExitCode));
				else
					commander.q_api.putJobLog(queueId, "trace", "Validation failed. Exit code: " + valExitCode);

				final String fileTrace = getTraceFromFile();
				if (fileTrace != null)
					commander.q_api.putJobLog(queueId, "trace", fileTrace);

				changeStatus(JobStatus.ERROR_V);
				return valExitCode;
			}

			if (!uploadOutputFiles(false)) {
				logger.log(Level.SEVERE, "Failed to upload output files");
				return -1;
			}

			return 0;
		}
		catch (final Exception e) {
			logger.log(Level.SEVERE, "Unable to handle job" + e);
			return -1;
		}
	}

	/**
	 * @param command
	 * @param arguments
	 * @param timeout
	 * @return <code>0</code> if everything went fine, a positive number with the process exit code (which would mean a problem) and a negative error code in case of timeout or other supervised
	 *         execution errors
	 */
	private int executeCommand(final String command, final List<String> arguments) {

		logger.log(Level.INFO, "Starting execution of command: " + command);

		List<String> cmd = new LinkedList<>();

		final int idx = command.lastIndexOf('/');

		final String cmdStrip = idx < 0 ? command : command.substring(idx + 1);

		final File fExe = new File(currentDir, cmdStrip);

		if (!fExe.exists()) {
			logger.log(Level.SEVERE, "ERROR. Executable was not found");
			return -2;
		}

		fExe.setExecutable(true);

		cmd.add(fExe.getAbsolutePath());

		if (arguments != null && arguments.size() > 0)
			for (final String argument : arguments)
				if (argument.trim().length() > 0) {
					final StringTokenizer st = new StringTokenizer(argument);

					while (st.hasMoreTokens())
						cmd.add(st.nextToken());
				}

		//Check if we can put the payload in its own container
		Containerizer cont = ContainerizerFactory.getContainerizer();
		if (cont != null) {
			cmd =  cont.containerize(String.join(" ", cmd));
		}
		
		logger.log(Level.INFO, "Executing: " + cmd + ", arguments is " + arguments + " pid: " + pid);

		final ProcessBuilder pBuilder = new ProcessBuilder(cmd);

		final HashMap<String, String> environment_packages = getJobPackagesEnvironment();
		final Map<String, String> processEnv = pBuilder.environment();
		final HashMap<String, String> jBoxEnv = ConfigUtils.exportJBoxVariables();

		processEnv.putAll(environment_packages);
		processEnv.putAll(loadJDLEnvironmentVariables());
		processEnv.putAll(jBoxEnv);
		processEnv.put("JALIEN_TOKEN_CERT", tokenCert);
		processEnv.put("JALIEN_TOKEN_KEY", tokenKey);
		processEnv.put("ALIEN_JOB_TOKEN", legacyToken); // add legacy token
		processEnv.put("ALIEN_PROC_ID", String.valueOf(queueId));

		pBuilder.redirectOutput(Redirect.appendTo(new File(currentDir, "stdout")));
		pBuilder.redirectError(Redirect.appendTo(new File(currentDir, "stderr")));

		final Process p;

		try {
			p = pBuilder.start();

		}
		catch (final IOException ioe) {
			logger.log(Level.INFO, "Exception running " + cmd + " : " + ioe.getMessage());
			return -5;
		}

		if (!p.isAlive()) {
			logger.log(Level.INFO, "The process for: " + cmd + " has terminated. Failed to execute?");
			return p.exitValue();
		}

		try {
			p.waitFor();
		}
		catch (final InterruptedException e) {
			logger.log(Level.INFO, "Interrupted while waiting for process to finish execution" + e);
		}

		return p.exitValue();

	}

	private int execute() {
		commander.q_api.putJobLog(queueId, "trace", "Starting execution");

		changeStatus(JobStatus.RUNNING);
		final int code = executeCommand(jdl.gets("Executable"), jdl.getArguments());

		return code;
	}

	private int validate() {
		int code = 0;

		final String validation = jdl.gets("ValidationCommand");

		if (validation != null) {
			commander.q_api.putJobLog(queueId, "trace", "Starting validation");
			code = executeCommand(validation, null);
		}

		return code;
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
			logger.log(Level.WARNING, "Not all requested files could be located");
			return false;
		}

		final Map<LFN, File> localFiles = new HashMap<>();

		for (final LFN l : iFiles) {
			File localFile = new File(currentDir, l.getFileName());

			int i = 0;

			while (localFile.exists() && i < 100000) {
				localFile = new File(currentDir, l.getFileName() + "." + i);
				i++;
			}

			if (localFile.exists()) {
				logger.log(Level.WARNING, "Too many occurences of " + l.getFileName() + " in " + currentDir.getAbsolutePath());
				return false;
			}

			localFiles.put(l, localFile);
		}

		for (final Map.Entry<LFN, File> entry : localFiles.entrySet()) {
			final List<PFN> pfns = c_api.getPFNsToRead(entry.getKey(), null, null);

			if (pfns == null || pfns.size() == 0) {
				logger.log(Level.WARNING, "No replicas of " + entry.getKey().getCanonicalName() + " to read from");
				return false;
			}

			final GUID g = pfns.iterator().next().getGuid();

			commander.q_api.putJobLog(queueId, "trace", "Getting InputFile: " + entry.getKey().getCanonicalName());

			logger.log(Level.INFO, "GUID g: " + g + " entry.getvalue(): " + entry.getValue());

			final File f = IOUtils.get(g, entry.getValue());

			if (f == null) {
				logger.log(Level.WARNING, "Could not download " + entry.getKey().getCanonicalName() + " to " + entry.getValue().getAbsolutePath());
				return false;
			}

		}

		dumpInputDataList();

		logger.log(Level.INFO, "Sandbox populated: " + currentDir.getAbsolutePath());

		return true;
	}

	private void dumpInputDataList() {
		// creates xml file with the InputData
		try {
			final String list = jdl.gets("InputDataList");

			if (list == null)
				return;

			logger.log(Level.INFO, "Going to create XML: " + list);

			final String format = jdl.gets("InputDataListFormat");
			if (format == null || !format.equals("xml-single")) {
				logger.log(Level.WARNING, "XML format not understood");
				return;
			}

			final XmlCollection c = new XmlCollection();
			c.setName("jobinputdata");
			final List<String> datalist = jdl.getInputData(true);

			// TODO: Change
			for (final String s : datalist) {
				final LFN l = c_api.getLFN(s);
				if (l == null)
					continue;
				c.add(l);
			}

			final String content = c.toString();

			Files.write(Paths.get(currentDir.getAbsolutePath() + "/" + list), content.getBytes());

		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Problem dumping XML: " + e.toString());
		}

	}

	private HashMap<String, String> getJobPackagesEnvironment() {
		final String voalice = "VO_ALICE@";
		String packagestring = "";
		final HashMap<String, String> packs = (HashMap<String, String>) jdl.getPackages();
		HashMap<String, String> envmap = new HashMap<>();

		logger.log(Level.INFO, "Preparing to install packages");
		if (packs != null) {
			for (final String pack : packs.keySet())
				packagestring += voalice + pack + "::" + packs.get(pack) + ",";

			if (!packs.containsKey("APISCONFIG"))
				packagestring += voalice + "APISCONFIG,";

			packagestring = packagestring.substring(0, packagestring.length() - 1);

			final ArrayList<String> packagesList = new ArrayList<>();
			packagesList.add(packagestring);

			logger.log(Level.INFO, packagestring);

			envmap = (HashMap<String, String>) installPackages(packagesList);
		}

		logger.log(Level.INFO, envmap.toString());
		return envmap;
	}

	private boolean uploadOutputFiles(boolean ERROR_E) {
		boolean uploadedAllOutFiles = true;
		boolean uploadedNotAllCopies = false;

		commander.q_api.putJobLog(queueId, "trace", "Going to uploadOutputFiles");
		logger.log(Level.INFO, "Uploading output for: " + jdl);

		final String outputDir = getJobOutputDir();

		String tag = "Output";
		if (ERROR_E)
			tag = "OutputErrorE";

		changeStatus(JobStatus.SAVING);

		logger.log(Level.INFO, "queueId: " + queueId);
		logger.log(Level.INFO, "outputDir: " + outputDir);
		logger.log(Level.INFO, "We are the current user: " + commander.getUser().getName());

		if (c_api.getLFN(outputDir) == null) {
			final LFN outDir = c_api.createCatalogueDirectory(outputDir);
			if (outDir == null) {
				logger.log(Level.SEVERE, "Error creating the OutputDir [" + outputDir + "].");
				changeStatus(JobStatus.ERROR_SV);
				return false;
			}
		}

		final ParsedOutput filesTable = new ParsedOutput(queueId, jdl, currentDir.getAbsolutePath(), tag);

		for (final OutputEntry entry : filesTable.getEntries()) {
			File localFile;
			try {
				localFile = new File(currentDir.getAbsolutePath() + "/" + entry.getName());
				logger.log(Level.INFO, "Processing output file: " + localFile);

				if (entry.isArchive())
					entry.createZip(currentDir.getAbsolutePath());

				if (localFile.exists() && localFile.isFile() && localFile.canRead() && localFile.length() > 0) {
					// Use upload instead
					commander.q_api.putJobLog(queueId, "trace", "Uploading: " + entry.getName());

					String args = "-w,-S," +
							(entry.getOptions() != null && entry.getOptions().length() > 0 ? entry.getOptions().replace('=', ':') : "disk:2") +
							",-j," + String.valueOf(queueId) + "";

					// Don't commit in case of ERROR_E
					if (ERROR_E)
						args += ",-nc";

					final ByteArrayOutputStream out = new ByteArrayOutputStream();
					IOUtils.upload(localFile, outputDir + "/" + entry.getName(), UserFactory.getByUsername(username), out, args.split(","));
					final String output_upload = out.toString("UTF-8");
					final String lower_output = output_upload.toLowerCase();

					logger.log(Level.INFO, "Output upload: " + output_upload);

					if (lower_output.contains("only")) {
						uploadedNotAllCopies = true;
						commander.q_api.putJobLog(queueId, "trace", output_upload);
						break;
					}
					else if (lower_output.contains("failed")) {
						uploadedAllOutFiles = false;
						commander.q_api.putJobLog(queueId, "trace", output_upload);
						break;
					}

					if (!ERROR_E) {
						// Register lfn links to archive
						CatalogueApiUtils.registerEntry(entry, outputDir + "/", UserFactory.getByUsername(username));
					}
				}
				else {
					logger.log(Level.WARNING, "Can't upload output file " + localFile.getName() + ", does not exist or has zero size.");
					commander.q_api.putJobLog(queueId, "trace", "Can't upload output file " + localFile.getName() + ", does not exist or has zero size.");
				}

			}
			catch (final IOException e) {
				logger.log(Level.WARNING, "IOException received while attempting to upload files", e);
				uploadedAllOutFiles = false;
			}
		}

		createAndAddResultsJDL(filesTable);

		if (!uploadedAllOutFiles) {
			changeStatus(JobStatus.ERROR_SV);
			return false;
		} // else
			// changeStatus(JobStatus.SAVED); TODO: To be put back later if still needed

		if (!ERROR_E) {
			if (uploadedNotAllCopies)
				changeStatus(JobStatus.DONE_WARN);
			else
				changeStatus(JobStatus.DONE);
		}
		else
			changeStatus(JobStatus.ERROR_E);

		return uploadedAllOutFiles;
	}

	private HashMap<String, String> loadJDLEnvironmentVariables() {
		final HashMap<String, String> hashret = new HashMap<>();

		try {
			final HashMap<String, Object> vars = (HashMap<String, Object>) jdl.getJDLVariables();

			if (vars != null)
				for (final String s : vars.keySet()) {
					String value = "";
					final Object val = jdl.get(s);

					if (val instanceof Collection<?>) {
						@SuppressWarnings("unchecked")
						final Iterator<String> it = ((Collection<String>) val).iterator();
						String sbuff = "";
						boolean isFirst = true;

						while (it.hasNext()) {
							if (!isFirst)
								sbuff += "##";
							final String v = it.next().toString();
							sbuff += v;
							isFirst = false;
						}
						value = sbuff;
					}
					else
						value = val.toString();

					hashret.put("ALIEN_JDL_" + s.toUpperCase(), value);
				}
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "There was a problem getting JDLVariables: " + e);
		}

		return hashret;
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {
		final JobWrapper jw = new JobWrapper();
		jw.run();
	}

	/**
	 * Updates the current state of the job.
	 * 
	 * @param newStatus
	 */
	public void changeStatus(final JobStatus newStatus) {
		jobStatus = newStatus;

		final HashMap<String, Object> extrafields = new HashMap<>();
		extrafields.put("exechost", ceHost);

		// if final status with saved files, we set the path
		if (jobStatus == JobStatus.DONE || jobStatus == JobStatus.DONE_WARN || jobStatus == JobStatus.ERROR_E || jobStatus == JobStatus.ERROR_V)
			extrafields.put("path", getJobOutputDir());
		else if (jobStatus == JobStatus.RUNNING) {
			extrafields.put("spyurl", hostName + ":" + TomcatServer.getPort());
			extrafields.put("node", hostName);
		}
		try {
			// Set the updated status
			TaskQueueApiUtils.setJobStatus(queueId, newStatus, extrafields);

			// Also write status to file for the JobAgent to see
			Files.writeString(Paths.get(currentDir.getAbsolutePath() + "/.jobstatus"), newStatus.name());
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "An error occurred when attempting to change current job status: " + e);
		}
		return;
	}

	/**
	 * @return job output dir (as indicated in the JDL if OK, or the recycle path if not)
	 */
	public String getJobOutputDir() {
		String outputDir = jdl.getOutputDir();

		if (jobStatus == JobStatus.ERROR_V || jobStatus == JobStatus.ERROR_E)
			outputDir = FileSystemUtils.getAbsolutePath(username, null, "~" + "recycle/" + defaultOutputDirPrefix + queueId);
		else if (outputDir == null)
			outputDir = FileSystemUtils.getAbsolutePath(username, null, "~" + defaultOutputDirPrefix + queueId);

		return outputDir;
	}

	private static String getTraceFromFile() {
		final File traceFile = new File(".alienValidation.trace");

		if (traceFile.exists()) {
			try {
				return new String(Files.readAllBytes(traceFile.toPath()));
			}
			catch (final Exception e) {
				logger.log(Level.WARNING, "An error occurred when reading .alienValidation.trace: " + e);
			}
		}
		logger.log(Level.INFO, ".alienValidation.trace does not exist.");
		return null;
	}

	private void createAndAddResultsJDL(ParsedOutput filesTable) {

		final ArrayList<String> jdlOutput = new ArrayList<>();
		for (final OutputEntry entry : filesTable.getEntries()) {

			String entryString = entry.getName();
			File entryFile = new File(currentDir.getAbsolutePath() + "/" + entryString);

			entryString += ";" + String.valueOf(entryFile.length());

			try {
				entryString += ";" + IOUtils.getMD5(entryFile);
			}
			catch (IOException e) {
				logger.log(Level.WARNING, "Could not generate MD5 for a file: " + e);
			}

			jdlOutput.add(entryString);

			// Also add the archive files to outputlist
			if (entry.isArchive()) {

				final ArrayList<String> archiveFiles = entry.getFilesIncluded();
				final HashMap<String, Long> archiveSizes = entry.getSizesIncluded();
				final HashMap<String, String> archiveMd5s = entry.getMD5sIncluded();
				for (final String archiveEntry : archiveFiles) {

					String archiveEntryString = archiveEntry;

					archiveEntryString += ";" + archiveSizes.get(archiveEntry);
					archiveEntryString += ";" + archiveMd5s.get(archiveEntry);
					archiveEntryString += ";" + entry.getName(); // name of its archive

					jdlOutput.add(archiveEntryString);
				}
			}

		}
		jdl.set("OutputFiles", "\n" + String.join("\n", jdlOutput));

		TaskQueueApiUtils.addResultsJdl(jdl, queueId);
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
