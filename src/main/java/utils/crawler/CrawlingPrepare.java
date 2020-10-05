package utils.crawler;

import alien.api.ServerException;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.se.SE;
import alien.se.SEUtils;
import alien.shell.commands.JAliEnCOMMander;
import alien.taskQueue.JDL;
import alien.user.JAKeyStore;
import joptsimple.internal.Strings;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Extracts random PFNs from the catalogue and serializes them to disk
 * Launches SEFileCrawler jobs using the aforementioned file as InputFile
 *
 * @author anegru
 */
class CrawlingPrepare {

	/**
	 * logger
	 */
	private static final Logger logger = ConfigUtils.getLogger(CrawlingPrepare.class.getCanonicalName());

	/**
	 * JAlienCOMMander object
	 */
	private static JAliEnCOMMander commander;

	/**
	 * The number of random PFNs to extract
	 */
	private static int sampleSize;

	/**
	 * The number of crawling jobs to be launched
	 */
	private static int crawlingJobCount;


	/**
	 * The storage element for which the job runs
	 */
	private static SE se;

	/**
	 * The Unix timestamp associated with the start of the current iteration
	 */
	private static long iterationUnixTimestamp;

	/**
	 * The file type of the output
	 */
	private static String outputFileType;

	private static final int ARGUMENT_COUNT = 5;

	static final String FILE_NAME_JOBS_TO_KILL = "jobs_to_kill_crawling";

	private static final String FILE_NAME_PFN = "pfn";

	private static final String FILE_SEPARATOR = " ";

	static final Integer TIME_TO_LIVE = Integer.valueOf(21600);

	static final Integer MAX_WAITING_TIME = Integer.valueOf(18000);

	/**
	 * Submit jobs that analyze random PFNs for all SEs
	 *
	 * @param args Command line arguments
	 */
	public static void main(String[] args) {
		try {
			ConfigUtils.setApplicationName("CrawlingPrepare");
			ConfigUtils.switchToForkProcessLaunching();

			if (!JAKeyStore.loadKeyStore()) {
				logger.log(Level.SEVERE, "No identity found, exiting");
				return;
			}
			commander = JAliEnCOMMander.getInstance();

			parseArguments(args);
			startCrawlingJobs(se);
		}
		catch (Exception e) {
			logger.log(Level.SEVERE, "Cannot start crawling jobs " + e.getMessage());
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
			throw new Exception("Number of arguments supplied is incorrect. Expected " + ARGUMENT_COUNT + ", but got " + args.length);

		sampleSize = Integer.parseInt(args[0]);
		crawlingJobCount = Integer.parseInt(args[1]);
		se = SEUtils.getSE(Integer.parseInt(args[2]));
		iterationUnixTimestamp = Long.parseLong(args[3]);
		outputFileType = args[4];
	}

	/**
	 * Start the crawling jobs per SE
	 *
	 * @param storageElement The SE for which the crawling starts
	 */
	private static void startCrawlingJobs(SE storageElement) {

		Collection<PFN> pfns = commander.c_api.getRandomPFNsFromSE(storageElement.seNumber, sampleSize);

		if (pfns == null) {
			logger.log(Level.INFO, "Cannot extract random files from the catalogue");
			return;
		}

		ArrayList<PFN> randomPFNs = new ArrayList<>(pfns);
		ArrayList<String> jobIds = new ArrayList<>();

		try {
			writePFNSliceToDisk(randomPFNs, getDirectoryPathSE(), FILE_NAME_PFN);

			for (int i = 0; i < crawlingJobCount; i++) {
				try {
					int startIndex = i * sampleSize / crawlingJobCount;
					int endIndex = (i + 1) * sampleSize / crawlingJobCount;
					JDL jdlCrawling = getJDLCrawlSE(se, i, startIndex, endIndex);
					long jobId = commander.q_api.submitJob(jdlCrawling);
					jobIds.add(Long.toString(jobId));
				}
				catch (ServerException e) {
					logger.log(Level.WARNING, "Cannot submit crawling job " + e.getMessage());
				}
			}

			// these jobs must be killed in the cleanup stage as well
			String fileContents = Strings.join(jobIds, FILE_SEPARATOR) + FILE_SEPARATOR;
			String fullPath = getDirectoryPathSE() + FILE_NAME_JOBS_TO_KILL;
			CrawlerUtils.writeToDisk(commander, logger, fileContents, FILE_NAME_JOBS_TO_KILL, fullPath);
		}
		catch (IOException e) {
			logger.log(Level.WARNING, "Failed to write to disk " + e.getMessage());
		}
	}

	/**
	 * Serialize an array of random PFNs to disk. This array is an InputFile for the crawling jobs
	 *
	 * @param randomPFNs A list of random PFNs
	 * @param outputDirectoryPath The directory path of the output
	 * @param outputFileName The file name of the output
	 * @throws IOException
	 */
	private static void writePFNSliceToDisk(List<PFN> randomPFNs, String outputDirectoryPath, String outputFileName) throws IOException {
		int maxRetires = 3;

		for (int attempts = 0; attempts < maxRetires; attempts++) {
			String fullPath = outputDirectoryPath + outputFileName;
			File f = new File(outputFileName);

			try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(f))) {
				oos.writeObject(randomPFNs);
				oos.flush();
				oos.close();

				LFN lfnUploaded = IOUtils.upload(f, fullPath, commander.getUser(), 3, null, true);

				if(lfnUploaded == null)
					logger.log(Level.WARNING, "Uploading " + fullPath + " failed");
				else
					logger.log(Level.INFO, "Successfully uploaded " + fullPath);

				return;
			}
			catch (IOException e) {
				e.printStackTrace();
				if (attempts == maxRetires - 1)
					throw e;
			}
		}
	}

	/**
	 * Get JDL to be used to crawl files from a specific SE
	 * @param storageElement The SE for which the job is launched
	 * @param jobIndex The index of the job to launch
	 * @param pfnStartIndex The start index from the serialized array
	 * @param pfnEndIndex The end index from the serialized array
	 * @return JDL
	 */
	private static JDL getJDLCrawlSE(SE storageElement, int jobIndex, int pfnStartIndex, int pfnEndIndex) {
		JDL jdl = new JDL();
		jdl.append("JobTag", "Crawling_" + storageElement.seNumber + "_" + jobIndex);
		jdl.set("OutputDir", getDirectoryPathSE() + "logs/" + jobIndex);
		jdl.append("InputFile", "LF:" + commander.getCurrentDirName() + "alien-users.jar");
		jdl.append("InputFile", "LF:" + getDirectoryPathSE() + "pfn");
		jdl.set("Arguments", se.seNumber + " " + jobIndex + " " + outputFileType + " " + iterationUnixTimestamp + " " + pfnStartIndex + " " + pfnEndIndex);
		jdl.set("Executable", commander.getCurrentDirName() + "crawling.sh");
		jdl.set("TTL", SEFileCrawler.TIME_TO_LIVE);
		jdl.set("MaxWaitingTime", SEFileCrawler.MAX_WAITING_TIME);
		jdl.append("Output", "crawling.log");
		jdl.append("Workdirectorysize", "11000MB");
		jdl.set("Requirements", GetCEs.getSiteJDLRequirement(se.seName));
		return jdl;
	}

	private static String getDirectoryPathSE() {
		return commander.getCurrentDirName() + "iteration_" + iterationUnixTimestamp + "/" + CrawlerUtils.getSEName(se) + "/";
	}
}
