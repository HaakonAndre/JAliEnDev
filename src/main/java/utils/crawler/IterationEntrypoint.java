package utils.crawler;

import alien.catalogue.LFN;
import alien.config.ConfigUtils;
import alien.shell.commands.JAliEnCOMMander;
import alien.taskQueue.JDL;
import alien.user.JAKeyStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents the entry point for each iteration. It runs periodically from a cron.
 *
 * @author anegru
 */
class IterationEntrypoint {

	private static final int ARGUMENT_COUNT = 6;

	/**
	 * The minimum number of crawling jobs per SE in each iteration, by default 10
	 */
	private static int minCrawlingJobs = 10;

	/**
	 * The maximum number of crawling jobs per SE in each iteration, by default 100
	 */
	private static int maxCrawlingJobs = 100;

	/**
	 * The minimum number of random PFNs to extract per SE in each iteration, by default 1000
	 */
	private static int minRandomPFN = 1000;

	/**
	 * The maximum number of random PFNs to extract per SE in each iteration, by default 10000
	 */
	private static int maxRandomPFN = 10000;

	/**
	 * The type of output files. Possible values: json, csv
	 */
	private static String outputFileType;

	/**
	 * The unix timestamp  registered at the beginning of each iteration
	 */
	private static long currentIterationUnixTimestamp;

	/**
	 * The JAliEn package to be used in JDL when launching crawling jobs
	 */
	private static String jalienPackage;

	/**
	 * logger
	 */
	private static final Logger logger = ConfigUtils.getLogger(IterationEntrypoint.class.getCanonicalName());

	/**
	 * JAliEnCOMMander object
	 */
	static JAliEnCOMMander commander;

	/**
	 *
	 */
	static final String FILE_NAME_JOBS_TO_KILL = "jobs_to_kill_iteration_entrypoint";

	/**
	 * Entry point for every crawling iteration. Submits jobs
	 *
	 * @param args
	 */
	public static void main(String[] args) {
		logger.log(Level.INFO, "Start iteration");

		ConfigUtils.setApplicationName("IterationEntrypoint");

		if (!JAKeyStore.loadKeyStore()) {
			logger.log(Level.SEVERE, "No identity found, exiting");
			return;
		}

		currentIterationUnixTimestamp = System.currentTimeMillis() / 1000L;
		commander = JAliEnCOMMander.getInstance();

		try {
			parseArguments(args);
			JDL jdl = getJDLIterationStart();
			long jobId = commander.q_api.submitJob(jdl);
			String fullPath = getCurrentIterationDirectoryPath() + FILE_NAME_JOBS_TO_KILL;
			CrawlerUtils.writeToDisk(commander, logger, Long.toString(jobId), FILE_NAME_JOBS_TO_KILL, fullPath);
		}
		catch (Exception e) {
			e.printStackTrace();
			logger.log(Level.SEVERE, "Failed to launch iteration. " + e.getMessage());
		}
	}

	/**
	 * Parse job arguments
	 *
	 * @param args
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
		jalienPackage = args[5];
	}

	/**
	 * Get JDL to be used to start the crawling process for an iteration
	 *
	 * @return JDL
	 */
	private static JDL getJDLIterationStart() {
		String previousIterationUnixTimestamp = getPreviousIterationTimestamp();

		if (previousIterationUnixTimestamp == null)
			previousIterationUnixTimestamp = "null";

		JDL jdl = new JDL();
		jdl.append("Package", jalienPackage);
		jdl.append("JobTag", "IterationPrepare");
		jdl.set("OutputDir", commander.getCurrentDirName() + "iteration_" + currentIterationUnixTimestamp);
		jdl.append("InputFile", "LF:" + commander.getCurrentDirName() + "alien-users.jar");
		jdl.set("Arguments", minCrawlingJobs + " " + maxCrawlingJobs + " " + minRandomPFN + " " + maxRandomPFN + " " + outputFileType + " " + previousIterationUnixTimestamp + " " + currentIterationUnixTimestamp + " " + jalienPackage);
		jdl.set("Executable", commander.getCurrentDirName() + "iteration_prepare.sh");
		jdl.set("TTL", IterationPrepare.TIME_TO_LIVE);
		jdl.set("MaxWaitingTime", IterationPrepare.MAX_WAITING_TIME);
		jdl.append("Output", "iteration_prepare.log");
		jdl.append("Workdirectorysize", "11000MB");
		jdl.set("Requirements", GetCEs.getSiteJDLRequirement("ALICE::CERN::EOS"));
		return jdl;
	}

	/**
	 * Get the full path of the previous iteration.
	 *
	 * @return String
	 */
	private static String getPreviousIterationTimestamp() {
		try {
			List<Long> iterationTimestamps = new ArrayList<>();
			List<LFN> lfns = commander.c_api.getLFNs(commander.getCurrentDirName());

			if (lfns == null) {
				logger.log(Level.INFO, "Cannot list " + commander.getCurrentDirName());
				return null;
			}

			String currentTimestamp = Long.toString(currentIterationUnixTimestamp);

			for (LFN lfn : lfns) {
				try {
					if (lfn.isDirectory() && lfn.getFileName().matches("iteration_[0-9]+")) {
						String timestamp = lfn.getFileName().split("_")[1];
						if (!timestamp.equals(currentTimestamp))
							iterationTimestamps.add(Long.valueOf(Long.parseLong(timestamp)));
					}
				}
				catch (Exception e) {
					e.printStackTrace();
					// ignore folders that are not of the format 'iteration_%d'
					logger.log(Level.INFO, "LFN " + lfn.getCanonicalName() + "is not an iteration directory");
				}
			}

			Collections.sort(iterationTimestamps);

			if (iterationTimestamps.size() > 0)
				return iterationTimestamps.get(iterationTimestamps.size() - 1).toString();
		}
		catch (Exception e) {
			e.printStackTrace();
			logger.log(Level.WARNING, "Cannot list " + commander.getCurrentDirName());
		}

		return null;
	}

	/**
	 * Return the path of the current iteration
	 *
	 * @return String
	 */
	private static String getCurrentIterationDirectoryPath() {
		return commander.getCurrentDirName() + "iteration_" + currentIterationUnixTimestamp + "/";
	}
}