package utils.crawler;

import alien.catalogue.LFN;
import alien.config.ConfigUtils;
import alien.se.SE;
import alien.se.SEUtils;
import alien.shell.commands.JAliEnCOMMander;
import alien.user.JAKeyStore;
import jline.internal.Nullable;
import lazyj.Utils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

class OutputMerger {
	/**
	 * logger
	 */
	private static final Logger logger = ConfigUtils.getLogger(OutputMerger.class.getCanonicalName());

	/**
	 * JAlienCOMMander object
	 */
	private static JAliEnCOMMander commander;

	/**
	 * The storage element for which the job runs
	 */
	private static SE se;

	/**
	 * The Unix timestamp of the iteration
	 */
	private static String iterationUnixTimestamp;

	/**
	 * The Unix timestamp associated with the start of the current iteration
	 */
	private static String outputFileType;

	private static final int ARGUMENT_COUNT = 3;

	static final Integer TIME_TO_LIVE = Integer.valueOf(86400);

	static final Integer MAX_WAITING_TIME = Integer.valueOf(1209600);

	/**
	 * Submit jobs that merge the outputs for a specific iteration
	 *
	 * @param args Command line arguments
	 */
	public static void main(String[] args) {
		try {
			ConfigUtils.setApplicationName("OutputMerger");
			ConfigUtils.switchToForkProcessLaunching();

			if (!JAKeyStore.loadKeyStore()) {
				logger.log(Level.SEVERE, "No identity found, exiting");
				return;
			}
			commander = JAliEnCOMMander.getInstance();

			parseArguments(args);
			mergeFilesFromPreviousIteration(se, iterationUnixTimestamp);
		}
		catch (Exception e) {
			logger.log(Level.SEVERE, "Cannot start merging job " + e.getMessage());
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

		se = SEUtils.getSE(Integer.parseInt(args[0]));
		iterationUnixTimestamp = args[1];
		outputFileType = args[2];
	}

	/**
	 * Start the merging procedure for the output and statistics
	 *
	 * @param storageElement The SE whose outputs must be merged
	 * @param previousIterationUnixTimestamp
	 */
	private static void mergeFilesFromPreviousIteration(SE storageElement, @Nullable String previousIterationUnixTimestamp) {

		if (previousIterationUnixTimestamp == null) {
			logger.log(Level.WARNING, "Previous iteration is null");
			return;
		}

		String iterationPath = commander.getCurrentDirName() + "iteration_" + previousIterationUnixTimestamp + "/";

		logger.log(Level.INFO, "Merging files from the previous iteration " + iterationPath);

		File mergedFile = null;
		try {
			logger.log(Level.INFO, "Merging for storageElement " + storageElement.seName);

			String previousIterationSEPath = iterationPath + CrawlerUtils.getSEName(storageElement) + "/";
			String outputDirectoryPath = previousIterationSEPath + "output";
			String statsDirectoryPath = previousIterationSEPath + "stats";
			String mergedOutputPath = previousIterationSEPath + "merged_output." + outputFileType;
			String mergedStatsPath = previousIterationSEPath + "merged_stats.json";

			// merge output files
			try {
				mergedFile = mergeOutputs(storageElement, outputDirectoryPath);
				CrawlerUtils.writeToDisk(commander, logger, mergedFile, mergedOutputPath);
			}
			catch (Exception exception) {
				exception.printStackTrace();
				logger.log(Level.WARNING, exception.getMessage());
			}

			// merge stat files
			try {
				mergedFile = mergeStats(storageElement, statsDirectoryPath);
				CrawlerUtils.writeToDisk(commander, logger, mergedFile, mergedStatsPath);
			}
			catch (Exception exception) {
				exception.printStackTrace();
				logger.log(Level.WARNING, exception.getMessage());
			}

		}
		finally {
			if (mergedFile != null && mergedFile.exists() && !mergedFile.delete())
				logger.log(Level.WARNING, "Cannot delete " + mergedFile.getName());
		}
	}

	/**
	 * Merge job outputs
	 *
	 * @param storageElement The SE for which to merge the output
	 * @param outputDirectoryPath The path of the output directory
	 * @return Merged file
	 * @throws Exception
	 */
	private static File mergeOutputs(SE storageElement, String outputDirectoryPath) throws Exception {

		if (storageElement == null) {
			throw new Exception("SE cannot be null");
		}

		File outputFile = new File("merged_output_" + storageElement.seNumber);

		try (FileWriter fw = new FileWriter(outputFile)) {

			List<LFN> lfns = commander.c_api.getLFNs(outputDirectoryPath);

			if (lfns == null)
				throw new Exception("Cannot get LFNs for path " + outputDirectoryPath);

			if (outputFileType.equals("json"))
				fw.write("{");
			else if (outputFileType.equals("csv"))
				fw.write(PFNData.CSV_HEADER);

			boolean firstSEWithData = true;

			for (LFN lfn : lfns) {
				logger.log(Level.INFO, "Merging " + lfn.getFileName());
				File downloadedFile = new File(lfn.getFileName());

				try {
					commander.c_api.downloadFile(lfn.getCanonicalName(), downloadedFile);

					String fileContents = Utils.readFile(downloadedFile.getCanonicalPath());

					if (fileContents != null) {

						if (outputFileType.equals("json")) {
							fileContents = fileContents.substring(1, fileContents.length() - 1);

							if (firstSEWithData)
								firstSEWithData = false;
							else
								fileContents = "," + fileContents;

						}
						else if (outputFileType.equals("csv")) {
							// remove csv header
							fileContents = fileContents.substring(fileContents.indexOf('\n') + 1);
						}

						fw.write(fileContents);
					}
					else
						logger.log(Level.WARNING, "File contents is null. Something went wrong with file read for SE " + se.seName);
				}
				catch (final IOException ioe) {
					ioe.printStackTrace();
					logger.log(Level.WARNING, ioe.getMessage());
				}
				finally {
					if (downloadedFile.exists() && !downloadedFile.delete()) {
						logger.log(Level.INFO, "Downloaded file cannot be deleted " + downloadedFile.getCanonicalPath());
					}
				}
			}

			if (outputFileType.equals("json"))
				fw.write("}");

			fw.flush();
		}
		catch (Exception exception) {
			exception.printStackTrace();
			logger.log(Level.INFO, exception.getMessage());
		}

		return outputFile;
	}

	/**
	 * Merge job stats
	 *
	 * @param storageElement The SE for which to merge the output
	 * @param outputDirectoryPath The path of the output directory
	 * @return Merged file
	 * @throws Exception
	 */
	private static File mergeStats(SE storageElement, String outputDirectoryPath) throws Exception {

		if (storageElement == null)
			throw new Exception("SE cannot be null");

		File outputFile = new File("merged_output_" + storageElement.seNumber);
		List<LFN> lfns = commander.c_api.getLFNs(outputDirectoryPath);

		if (lfns == null)
			throw new Exception("Cannot get LFNs for path " + outputDirectoryPath);

		JSONParser parser = new JSONParser();
		ArrayList<CrawlingStatistics> jobCrawlingStatistics = new ArrayList<>();

		for (LFN lfn : lfns) {
			File downloadedFile = new File(lfn.getFileName());

			try {
				commander.c_api.downloadFile(lfn.getCanonicalName(), downloadedFile);

				String fileContents = Utils.readFile(downloadedFile.getCanonicalPath());

				if (fileContents != null) {
					try {
						JSONObject statsJSON = (JSONObject) parser.parse(fileContents);
						jobCrawlingStatistics.add(CrawlingStatistics.fromJSON(statsJSON));
					}
					catch (ParseException e) {
						e.printStackTrace();
					}
				}
				else {
					logger.log(Level.WARNING, "File contents is null. Something went wrong with file read for SE " + storageElement.seName);
				}
			}
			catch (final IOException ioe) {
				ioe.printStackTrace();
				logger.log(Level.WARNING, ioe.getMessage());
			}
			finally {
				if (downloadedFile.exists() && !downloadedFile.delete())
					logger.log(Level.INFO, "Downloaded file cannot be deleted " + downloadedFile.getCanonicalPath());
			}
		}

		CrawlingStatistics averagedStats = CrawlingStatistics.getAveragedStats(jobCrawlingStatistics);

		try (FileWriter fw = new FileWriter(outputFile)) {
			fw.write(CrawlingStatistics.toJSON(averagedStats).toJSONString());
			fw.flush();
		}

		return outputFile;
	}
}
