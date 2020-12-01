package utils.crawler;

import alien.api.ServerException;
import alien.catalogue.LFN;
import alien.config.ConfigUtils;
import alien.se.SE;
import alien.shell.commands.JAliEnCOMMander;
import alien.taskQueue.JDL;
import alien.taskQueue.Job;
import alien.taskQueue.JobStatus;
import alien.user.JAKeyStore;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import jline.internal.Nullable;
import joptsimple.internal.Strings;
import org.json.simple.JSONObject;

/**
 * Kills jobs launched from the previous iteration
 * Merges the crawling outputs and crawling statistics from the previous iteration into single files
 * Computes the number of jobs and files that must be analyzed for each SE
 * Launches CrawlingPrepare jobs for all SEs to be analyzed
 *
 * @author anegru
 */
class IterationPrepare {

	private static final String FILE_SEPARATOR = " ";
	private static final String FILE_NAME_JOBS_TO_KILL = "jobs_to_kill_iteration_prepare";
	private static final String FILE_NAME_JOBS_OUTPUT_MERGER = "jobs_output_merger";
	private static final int ARGUMENT_COUNT = 8;
	static final Integer TIME_TO_LIVE = Integer.valueOf(21600);
	static final Integer MAX_WAITING_TIME = Integer.valueOf(18000);

	/**
	 * The minimum number of crawling jobs per SE in each iteration, by default 10
	 */
	private static int minCrawlingJobs;

	/**
	 * The maximum number of crawling jobs per SE in each iteration, by default 100
	 */
	private static int maxCrawlingJobs;

	/**
	 * The minimum number of random PFNs to extract per SE in each iteration, by default 1000
	 */
	private static int minRandomPFN;

	/**
	 * The maximum number of random PFNs to extract per SE in each iteration, by default 10000
	 */
	private static int maxRandomPFN;

	/**
	 * The type of output files. Possible values: json, csv
	 */
	private static String outputFileType;

	/**
	 * The unix timestamp of the previous iteration
	 */
	private static String previousIterationUnixTimestamp;

	/**
	 * The unix timestamp  registered at the beginning of each iteration
	 */
	private static String currentIterationUnixTimestamp;

	/**
	 * The JAliEn package to be used in JDL when launching crawling jobs
	 */
	private static String jalienPackage;


	/**
	 * logger
	 */
	private static final Logger logger = ConfigUtils.getLogger(IterationPrepare.class.getCanonicalName());

	/**
	 * JAliEnCOMMander object
	 */
	private static JAliEnCOMMander commander;

	/**
	 * Entry point for every crawling iteration. Submits jobs
	 *
	 * @param args Command line arguments
	 */
	public static void main(String[] args) {
		logger.log(Level.INFO, "Start iteration");

		ConfigUtils.setApplicationName("IterationPrepare");

		if (!JAKeyStore.loadKeyStore()) {
			logger.log(Level.SEVERE, "No identity found, exiting");
			return;
		}

		commander = JAliEnCOMMander.getInstance();

		try {
			parseArguments(args);
			List<SE> ses = getStorageElementsForCrawling();
			String previousIterationPath = getPreviousIterationPath(previousIterationUnixTimestamp);
			killJobsFromPreviousIteration(previousIterationPath, ses);
			mergeFilesFromPreviousIteration(ses, previousIterationPath);

			List<String> jobIds = submitJobs(ses);
			String fileContents = Strings.join(jobIds, FILE_SEPARATOR) + FILE_SEPARATOR;
			String fullPath = commander.getCurrentDirName() + "iteration_" + currentIterationUnixTimestamp + "/" + FILE_NAME_JOBS_TO_KILL;
			CrawlerUtils.writeToDisk(commander, logger, fileContents, FILE_NAME_JOBS_TO_KILL, fullPath);
		}
		catch (Exception exception) {
			exception.printStackTrace();
			logger.log(Level.INFO, exception.getMessage());
		}
	}

	/**
	 * Parse job arguments
	 *
	 * @param args Command line arguments
	 * @throws Exception
	 */
	private static void parseArguments(String[] args) throws Exception {

		if (args.length != ARGUMENT_COUNT)
			throw new Exception("The number of arguments supplied is incorrect: given " + args.length + ", but expected " + ARGUMENT_COUNT);

		minCrawlingJobs = Integer.parseInt(args[0]);
		maxCrawlingJobs = Integer.parseInt(args[1]);
		minRandomPFN = Integer.parseInt(args[2]);
		maxRandomPFN = Integer.parseInt(args[3]);
		outputFileType = args[4];
		previousIterationUnixTimestamp = args[5];
		currentIterationUnixTimestamp = args[6];
		jalienPackage = args[7];
	}

	/**
	 * Get the full list of SEs that have to be crawled. Only SEs with type 'disk' are selected
	 *
	 * @return List<SE>
	 * @throws Exception
	 */
	private static List<SE> getStorageElementsForCrawling() throws Exception {
		Collection<SE> ses = commander.c_api.getSEs(null);

		if (ses == null)
			throw new Exception("Cannot retrieve SEs");

		Predicate<SE> byType = se -> se.isQosType("disk");
		return ses.stream().filter(byType).collect(Collectors.toList());
	}

	/**
	 * Get the full path of the previous iteration.
	 * @param previousIterationTimestamp The timestamp of the previous iteration
	 *
	 * @return String | null
	 */
	private static String getPreviousIterationPath(String previousIterationTimestamp) {
		if (previousIterationTimestamp.equalsIgnoreCase("null"))
			return null;
		return commander.getCurrentDirName() + "iteration_" + previousIterationTimestamp + "/";
	}

	/**
	 * Kill all jobs that were launched in the previous iteration. The job ids to be killed are written to
	 * disk in files called 'jobs_to_kill_*'. These files are located in the home folder of the user and in
	 * every SE folder from the previous iteration. In case of a null parameter, it either means that it is
	 * the first iteration of the crawler and no previous iterations are available or some errors have occurred.
	 * In both cases, the job cleanup step is skipped.
	 *
	 * @param previousIterationPath The path of the previous iteration
	 * @param ses A list of SEs for which jobs must be killed
	 */
	private static void killJobsFromPreviousIteration(@Nullable String previousIterationPath, List<SE> ses) {

		try {

			if (previousIterationPath == null)
				return;

			killJobsFromFile(previousIterationPath, FILE_NAME_JOBS_TO_KILL, false);
			killJobsFromFile(previousIterationPath, IterationEntrypoint.FILE_NAME_JOBS_TO_KILL, false);

			List<LFN> filePaths = commander.c_api.getLFNs(previousIterationPath);

			if (filePaths == null) {
				logger.log(Level.INFO, "No jobs to kill " + previousIterationPath);
				return;
			}

			List<String> seNames = ses.stream().map(CrawlerUtils::getSEName).collect(Collectors.toList());

			for (LFN lfn : filePaths)
				if (lfn.isDirectory() && seNames.contains(lfn.getFileName()))
					killJobsFromFile(lfn.getCanonicalName(), CrawlingPrepare.FILE_NAME_JOBS_TO_KILL, true);
		}
		catch (Exception e) {
			e.printStackTrace();
			logger.log(Level.WARNING, "Cannot kill jobs from previous iteration. " + e.getMessage());
		}
	}

	/**
	 * Kill all jobs whose ids are specified in the file located at the path given as parameter.
	 * The file contains space separated integers that represent job ids launched in the previous
	 * iteration
	 *
	 * @param jobFileDirectory The directory path that contains the file with job ids (must end with '/')
	 * @param jobFileName The file that contains the job ids to kill
	 * @param generateStatistics Flag that tells whether to generate job statistics or not
	 */
	@SuppressWarnings("unchecked")
	private static void killJobsFromFile(String jobFileDirectory, String jobFileName, boolean generateStatistics) {
		try {
			String jobFilePath = jobFileDirectory + jobFileName;
			File downloadedFile = new File(FILE_NAME_JOBS_TO_KILL);
			JSONObject jobStatsJSON = new JSONObject();
			HashMap<JobStatus, Integer> mapJobStatusToCount = new HashMap<>();
			long jobsNotFound = 0;
			int totalJobCount = 0;

			if (downloadedFile.exists() && !downloadedFile.delete())
				logger.log(Level.INFO, "Cannot delete file " + downloadedFile.getName());

			final LFN lfn = commander.c_api.getLFN(jobFilePath, true);

			if (lfn.exists) {
				commander.c_api.downloadFile(jobFilePath, downloadedFile);

				try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(downloadedFile)))) {
					String buffer = bufferedReader.readLine();
					String[] jobIds = buffer.split(FILE_SEPARATOR);

					totalJobCount = jobIds.length;

					for (String jobId : jobIds) {
						long id = Long.parseLong(jobId);
						Job job = commander.q_api.getJob(id);

						if(job != null) {
							if (generateStatistics) {
								mapJobStatusToCount.merge(job.status(), Integer.valueOf(1), Integer::sum);
							}
							if (!job.isFinalState()) {
								logger.log(Level.INFO, "Killing " + jobId);
								if (!commander.q_api.killJob(id)) logger.log(Level.WARNING, "Cannot kill job with id " + jobId);
							}
						}
						else
							jobsNotFound += 1;
					}
				}

				if (generateStatistics) {
					String remoteFilePath = jobFileDirectory + jobFileName + "_stats.json";
					jobStatsJSON.putAll(mapJobStatusToCount);
					jobStatsJSON.put("TOTAL", Integer.valueOf(totalJobCount));

					if(jobsNotFound > 0)
						jobStatsJSON.put("NOT_FOUND", Long.valueOf(jobsNotFound));

					CrawlerUtils.writeToDisk(commander, logger, jobStatsJSON.toJSONString(), jobFileName + "_stats.json", remoteFilePath);
				}
			}
			else {
				logger.log(Level.WARNING, "LFN " + lfn.getCanonicalName() + " does not exist");
			}
		}
		catch (Exception exception) {
			exception.printStackTrace();
			logger.log(Level.WARNING, exception.getMessage());
		}
	}

	/**
	 * Start the merging procedure for the output and statistics from the previous iteration
	 *
	 * @param ses A list of SEs whose outputs from the previous iteration must be merged
	 */
	private static void mergeFilesFromPreviousIteration(List<SE> ses, @Nullable String previousIterationPath) {

		if (previousIterationPath == null) {
			return;
		}

		List<String> jobIds = new ArrayList<>();

		for (SE se : ses) {
			try {
				JDL jdl = getJDLOutputMerger(se);
				logger.log(Level.INFO, "Submitting jobs");
				long jobId = commander.q_api.submitJob(jdl);
				jobIds.add(Long.toString(jobId));
			}
			catch (Exception e) {
				e.printStackTrace();
				logger.log(Level.WARNING, "Cannot submit OutputMerger job to " + se.seName + " " + e.getMessage());
			}
		}

		try {
			String fileContents = Strings.join(jobIds, FILE_SEPARATOR) + FILE_SEPARATOR;
			String fullPath = previousIterationPath + FILE_NAME_JOBS_OUTPUT_MERGER;
			CrawlerUtils.writeToDisk(commander, logger, fileContents, FILE_NAME_JOBS_OUTPUT_MERGER, fullPath);
		}
		catch (Exception e) {
			logger.log(Level.WARNING, "Cannot upload file with OutputMerger job ids " + e.getMessage());
		}
	}

	/**
	 * Submit jobs for the SEs given as parameter. The jobs are of type crawling_prepare.
	 * Returns a list of job ids.
	 *
	 * @param ses A list of SEs for which to launch jobs
	 * @return List<String>
	 */
	private static List<String> submitJobs(List<SE> ses) {

		List<String> jobIds = new ArrayList<>();

		long minFileCount = Long.MAX_VALUE, maxFileCount = Long.MIN_VALUE;

		for (SE se : ses) {
			long numFiles = se.seNumFiles;
			if (numFiles < minFileCount) {
				minFileCount = numFiles;
			}
			if (numFiles > maxFileCount) {
				maxFileCount = numFiles;
			}
		}

		//submitting jobs to all available SEs
		for (SE se : ses) {
			try {
				int sampleSize = linearMathFunction(minFileCount, maxFileCount, minRandomPFN, maxRandomPFN, se.seNumFiles);
				int crawlingJobsCount = linearMathFunction(minFileCount, maxFileCount, minCrawlingJobs, maxCrawlingJobs, se.seNumFiles);
				logger.log(Level.INFO, "Computed values " + sampleSize + " " + crawlingJobsCount);

				JDL jdl = getJDLCrawlingPrepare(se, sampleSize, crawlingJobsCount);
				logger.log(Level.INFO, "Submitting jobs");
				long jobId = commander.q_api.submitJob(jdl);
				if (jobId > 0)
					jobIds.add(Long.toString(jobId));
			}
			catch (ServerException e) {
				e.printStackTrace();
				logger.log(Level.WARNING, "Submitting job to SE " + se.seName + " failed");
			}
		}

		return jobIds;
	}

	/**
	 * f : [a, b] -> [c, d]
	 * 
	 * The function is defined on an interval (for example 0 and the maximum number of files in a SE) with values
	 * on another interval (for example the min and max PFNs to crawl in an iteration). The function arguments a, b
	 * represent the domain, c,d represent the codomain and n is the value for which the function has to be applied.
	 * 
	 * @param a Domain lower bound
	 * @param b Domain upper bound
	 * @param c Codomain lower bound
	 * @param d Codomain upper bound
	 * @param n Value the function is applied to
	 * 
	 * @return the result of the function f applied for the value n
	 */
	private static int linearMathFunction(long a, long b, int c, int d, long n) {
		return (int) Math.floor(((d - c) * (float) (n - a)) / (b - a) + c);
	}

	/**
	 * Get JDL to be used to start the merging  process for the previous iteration, for a specific SE
	 *
	 * @param se The SE for which the job is launched
	 * @return JDL
	 */
	private static JDL getJDLOutputMerger(SE se) {
		JDL jdl = new JDL();
		jdl.append("Package", jalienPackage);
		jdl.append("JobTag", "OutputMerger_" + se.seNumber);
		jdl.set("OutputDir", getSEIterationDirectoryPath(se, previousIterationUnixTimestamp));
		jdl.append("InputFile", "LF:" + commander.getCurrentDirName() + "alien-users.jar");
		jdl.set("Arguments", se.seNumber + " " + previousIterationUnixTimestamp + " " + outputFileType);
		jdl.set("Executable", commander.getCurrentDirName() + "output_merger.sh");
		jdl.append("Output", "output_merger.log");
		jdl.append("Workdirectorysize", "11000MB");
		return jdl;
	}

	/**
	 * Get JDL to be used to start the crawling process for an iteration, for a specific SE
	 *
	 * @param se The SE for which the job is launched
	 * @param sampleSize The sample size computed for the SE
	 * @param crawlingJobsCount The number of crawling jobs to launch
	 * @return JDL
	 */
	private static JDL getJDLCrawlingPrepare(SE se, int sampleSize, int crawlingJobsCount) {
		JDL jdl = new JDL();
		jdl.append("Package", jalienPackage);
		jdl.append("JobTag", "CrawlingPrepare_" + se.seNumber);
		jdl.set("OutputDir", getSEIterationDirectoryPath(se, currentIterationUnixTimestamp));
		jdl.append("InputFile", "LF:" + commander.getCurrentDirName() + "alien-users.jar");
		jdl.set("Arguments", sampleSize + " " + crawlingJobsCount + " " + se.seNumber + " " + currentIterationUnixTimestamp + " " + outputFileType + " " + jalienPackage);
		jdl.set("Executable", commander.getCurrentDirName() + "crawling_prepare.sh");
		jdl.set("TTL", CrawlingPrepare.TIME_TO_LIVE);
		jdl.set("MaxWaitingTime", CrawlingPrepare.MAX_WAITING_TIME);
		jdl.append("Output", "crawling_prepare.log");
		jdl.append("Workdirectorysize", "11000MB");
		jdl.set("Requirements", GetCEs.getSiteJDLRequirement(se.seName));
		return jdl;
	}

	/**
	 * Return the path to the SE given as parameter for the current iteration
	 *
	 * @param se The SE for which to get the directory path
	 * @return String
	 */
	private static String getSEIterationDirectoryPath(SE se, String iterationUnixTimestamp) {
		return getIterationDirectoryPath(iterationUnixTimestamp) + CrawlerUtils.getSEName(se) + "/";
	}

	/**
	 * Return the path of the current iteration
	 *
	 * @return String
	 */
	private static String getIterationDirectoryPath(String iterationUnixTimestamp) {
		return commander.getCurrentDirName() + "iteration_" + iterationUnixTimestamp + "/";
	}
}