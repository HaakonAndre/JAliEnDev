package utils.crawler;

import static alien.io.protocols.SourceExceptionCode.LOCAL_FILE_SIZE_DIFFERENT;
import static alien.io.protocols.SourceExceptionCode.MD5_CHECKSUMS_DIFFER;
import static alien.io.protocols.SourceExceptionCode.XROOTD_EXITED_WITH_CODE;
import static alien.io.protocols.SourceExceptionCode.XROOTD_TIMED_OUT;
import static utils.crawler.CrawlingStatusCode.E_CATALOGUE_MD5_IS_BLANK;
import static utils.crawler.CrawlingStatusCode.E_FILE_EMPTY;
import static utils.crawler.CrawlingStatusCode.E_FILE_MD5_IS_NULL;
import static utils.crawler.CrawlingStatusCode.E_GUID_NOT_FOUND;
import static utils.crawler.CrawlingStatusCode.E_PFN_DOWNLOAD_FAILED;
import static utils.crawler.CrawlingStatusCode.E_PFN_NOT_ONLINE;
import static utils.crawler.CrawlingStatusCode.E_PFN_NOT_READABLE;
import static utils.crawler.CrawlingStatusCode.E_PFN_XRDSTAT_FAILED;
import static utils.crawler.CrawlingStatusCode.E_UNEXPECTED_ERROR;
import static utils.crawler.CrawlingStatusCode.S_FILE_CHECKSUM_MATCH;
import static utils.crawler.CrawlingStatusCode.S_FILE_CHECKSUM_MISMATCH;
import static utils.crawler.CrawlingStatusType.FILE_CORRUPT;
import static utils.crawler.CrawlingStatusType.FILE_INACCESSIBLE;
import static utils.crawler.CrawlingStatusType.FILE_OK;
import static utils.crawler.CrawlingStatusType.INTERNAL_ERROR;
import static utils.crawler.CrawlingStatusType.UNEXPECTED_ERROR;
import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.io.protocols.Factory;
import alien.io.protocols.SourceException;
import alien.io.protocols.SourceExceptionCode;
import alien.io.protocols.Xrootd;
import alien.se.SE;
import alien.se.SEUtils;
import alien.shell.commands.JAliEnCOMMander;
import alien.user.JAKeyStore;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONObject;

/**
 * Start the crawling process for a chunk of PFNs
 * Write information on disk for all PFNs analyzed
 * Extract statistics for the entire crawling process
 *
 * @author anegru
 */
class SEFileCrawler {

	/**
	 * File output format JSON
	 */
	private static final String OUTPUT_FORMAT_JSON = "json";

	/**
	 * File output format CSV
	 */
	private static final String OUTPUT_FORMAT_CSV = "csv";

	/**
	 * The name of the file that holds crawling data
	 */
	private static final String OUTPUT_FILE_NAME = "output";

	/**
	 * The name of the file that holds statistics about crawled files.
	 */
	private static final String STATS_FILE_NAME = "stats";


	/**
	 * The number of command line arguments required
	 */
	private static final int ARGUMENT_COUNT = 6;

	/**
	 * logger
	 */
	private static final Logger logger = ConfigUtils.getLogger(SEFileCrawler.class.getCanonicalName());

	/**
	 * JAliEnCOMMander object
	 */
	private static JAliEnCOMMander commander;

	/**
	 * Xrootd for download operation
	 */
	private static Xrootd xrootd;

	/**
	 * Map GUID to a PFNCrawled object. Contains data is is written to output.
	 */
	private static Map<String, PFNData> mapGuidToPFN = new HashMap<>();

	/**
	 * Storage element object
	 */
	private static SE se;

	/**
	 * Output file format type. (possible values 'json', 'csv')
	 */
	private static String outputFileType;

	/**
	 * Multiple crawling jobs are launched per SE in an interation. The index of the current job.
	 */
	private static int jobIndex;

	/**
	 * The Unix timestamp of the current running iteration
	 */
	private static String iterationUnixTimestamp;

	/**
	 * The start index in the array of random PFNs that must be crawled in this iteration
	 */
	private static int pfnStartIndex;

	/**
	 * The end index in the array of random PFNs that must be crawled in this iteration
	 */
	private static int pfnEndIndex;

	static final Integer TIME_TO_LIVE = Integer.valueOf(21600);

	static final Integer MAX_WAITING_TIME = Integer.valueOf(18000);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		logger.log(Level.INFO, "Start crawling");

		ConfigUtils.setApplicationName("SEFileCrawler");
		ConfigUtils.switchToForkProcessLaunching();

		if (!JAKeyStore.loadKeyStore()) {
			logger.log(Level.SEVERE, "No identity found, exiting");
			return;
		}

		commander = JAliEnCOMMander.getInstance();
		xrootd = (Xrootd) Factory.xrootd.clone();

		try {
			parseArguments(args);
			CrawlingStatistics stats = startCrawler();

			// writing output to disk
			writeJobOutputToDisk(mapGuidToPFN, outputFileType);

			// writing stats to disk
			if (stats != null) {
				String fileContents = CrawlingStatistics.toJSON(stats).toJSONString();
				CrawlerUtils.writeToDisk(commander, logger, fileContents, OUTPUT_FILE_NAME, getJobStatsPath());
			}
			else
				logger.log(Level.INFO, "No Stats could be generated in this iteration");
		}
		catch (Exception exception) {
			logger.log(Level.INFO, exception.getMessage());
			exception.printStackTrace();
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
			throw new Exception("Number of arguments supplied is incorrect. Expected " + ARGUMENT_COUNT + ", but got " + args.length);

		se = SEUtils.getSE(Integer.parseInt(args[0]));

		if (se == null)
			throw new Exception("Storage element with number " + args[0] + " does not exist");

		jobIndex = Integer.parseInt(args[1]);
		outputFileType = args[2];
		iterationUnixTimestamp = args[3];
		pfnStartIndex = Integer.parseInt(args[4]);
		pfnEndIndex = Integer.parseInt(args[5]);
	}

	/**
	 * Crawl fileCount random files from the SE.
	 *
	 * @return CrawlingStatistics object
	 */
	private static CrawlingStatistics startCrawler() {

		try {
			Collection<PFN> randomPFNs = getPFNsFromDisk(getSEPath(), "pfn", 3);
			ArrayList<PFN> pfns = new ArrayList<>(randomPFNs);

			if (pfnEndIndex > pfns.size())
				throw new Exception("End index given is out of range. Found " + pfns.size() + " random PFNs on disk, but the endIndex is " + pfnEndIndex);

			List<PFN> pfnsToCrawl = pfns.subList(pfnStartIndex, pfnEndIndex);
			logger.info("Job will crawl " + pfnsToCrawl.size() + " pfns");

			long[] timestamps = new long[pfnsToCrawl.size()];
			
			int totalPFNCount = 0, inaccessiblePFNs = 0, corruptPFNs = 0, okPFNs = 0, unknownStatusPFNs = 0;
			long fileSizeBytes = 0, downloadTotalDurationMillis = 0, downloadedPFNsTotalCount = 0;
			long xrdfsTotalDurationMillis = 0, xrdfsPFNsTotalCount = 0;

			if (pfnsToCrawl.size() == 0)
				return null;

			for (int i = 0; i < pfnsToCrawl.size(); i++) {
				try {
					PFN currentPFN = pfnsToCrawl.get(i);
					long startTimestamp = System.currentTimeMillis();
					CrawlingResult crawlingResult = crawlPFN(currentPFN);

					logger.info("PFN = " + currentPFN.pfn + " Result =" + crawlingResult);
					long endTimestamp = System.currentTimeMillis();

					timestamps[i] = endTimestamp - startTimestamp;
					fileSizeBytes += crawlingResult.getFileSizeTotalBytes();
					downloadedPFNsTotalCount += crawlingResult.getDownloadedPFNsTotalCount();
					downloadTotalDurationMillis += crawlingResult.getDownloadTotalDurationMillis();
					xrdfsPFNsTotalCount += crawlingResult.getXrdfsPFNsTotalCount();
					xrdfsTotalDurationMillis += crawlingResult.getXrdfsTotalDurationMillis();

					if (crawlingResult.getStatus().statusHasType(CrawlingStatusType.FILE_OK))
						okPFNs += 1;
					else if (crawlingResult.getStatus().statusHasType(FILE_CORRUPT))
						corruptPFNs += 1;
					else if (crawlingResult.getStatus().statusHasType(CrawlingStatusType.FILE_INACCESSIBLE))
						inaccessiblePFNs += 1;
					else
						unknownStatusPFNs += 1;

					totalPFNCount += 1;
					logger.log(Level.WARNING, "Result has type " + crawlingResult.getStatus());
				}
				catch (Exception e) {
					e.printStackTrace();
					logger.log(Level.SEVERE, "Cannot crawl pfn " + pfns.get(i).pfn + " " + e.getMessage());
				}
			}

			logger.info("Crawling finished for all files");

			CrawlingStatistics stats = new CrawlingStatistics(
					totalPFNCount,
					okPFNs,
					inaccessiblePFNs,
					corruptPFNs,
					unknownStatusPFNs,
					Long.MAX_VALUE,
					Long.MIN_VALUE,
					0L,
					0L,
					0L,
					fileSizeBytes,
					downloadedPFNsTotalCount,
					downloadTotalDurationMillis,
					xrdfsPFNsTotalCount,
					xrdfsTotalDurationMillis,
					System.currentTimeMillis()
			);

			for (int i = 0; i < timestamps.length; i++) {

				if (stats.crawlingMinDurationMillis > timestamps[i])
					stats.crawlingMinDurationMillis = timestamps[i];

				if (stats.crawlingMaxDurationMillis < timestamps[i])
					stats.crawlingMaxDurationMillis = timestamps[i];

				stats.crawlingTotalDurationMillis += timestamps[i];
			}

			stats.crawlingAvgDurationMillis = stats.crawlingTotalDurationMillis / timestamps.length;
			stats.iterationTotalDurationMillis = System.currentTimeMillis() - Long.parseLong(iterationUnixTimestamp) * 1000;
			return stats;
		}
		catch (Exception exception) {
			exception.printStackTrace();
			logger.log(Level.SEVERE, exception.getMessage());
			return null;
		}
	}

	/**
	 * Read the PFNs from the files on disk.
	 *
	 * @param directoryPath
	 * @param fileName
	 * @param retries
	 * @return a list of PFNs to crawl
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private static Collection<PFN> getPFNsFromDisk(String directoryPath, String fileName, int retries) throws IOException, ClassNotFoundException {

		String filePath = directoryPath + fileName;
		Collection<PFN> pfns = new HashSet<>();

		if (retries == 0)
			return pfns;

		try (
				FileInputStream fileInputStream = new FileInputStream(new File(fileName));
				ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
		) {
			Collection<?> pfnsContainer = (Collection<?>) objectInputStream.readObject();

			for (Object pfn : pfnsContainer)
				pfns.add((PFN) pfn);

			return pfns;
		}
		catch (FileNotFoundException exception) {
			try {
				File downloadedFile = new File(fileName);

				if (downloadedFile.exists() && !downloadedFile.delete())
					logger.log(Level.INFO, "Cannot delete downloaded file " + downloadedFile.getCanonicalPath());

				commander.c_api.downloadFile(filePath, downloadedFile);
				return getPFNsFromDisk(directoryPath, fileName, retries - 1);
			}
			catch (Exception e) {
				e.printStackTrace();
				// file cannot be downloaded. throw the original exception
				throw exception;
			}
		}
	}

	/**
	 * Crawl the PFN specified in the argument
	 *
	 * @param currentPFN
	 * @return Status of the crawling
	 */
	private static CrawlingResult crawlPFN(PFN currentPFN) {
		CrawlingStatus status = null;
		PFN pfnToRead = null;
		GUID guid = null;
		Long catalogueFileSize = null, observedFileSize = null;
		String catalogueMD5 = null, observedMD5 = null;
		Long downloadDurationMillis = null, xrdfsDurationMillis = null;

		long fileSize = 0, downloadDuration = 0, downloadedPFN = 0, xrdfsDuration = 0, xrdfsPFN = 0;

		// fill PFN access token
		try {
			pfnToRead = getPFNWithAccessToken(currentPFN);
		}
		catch (Exception exception) {
			logger.log(Level.WARNING, exception.getMessage());
			status = new CrawlingStatus(E_PFN_NOT_READABLE, FILE_INACCESSIBLE, formatError(exception.getMessage()));
		}

		// check if file exists
		if (pfnToRead != null) {
			try {
				final Iterator<LFN> it;
				guid = commander.c_api.getGUID(pfnToRead.getGuid().guid.toString(), false, true);

				if (guid == null)
					status = new CrawlingStatus(E_GUID_NOT_FOUND, FILE_INACCESSIBLE, "Cannot get GUID from PFN");
				else
					catalogueFileSize = guid.size;
			}
			catch (Exception exception) {
				exception.printStackTrace();
				logger.log(Level.WARNING, exception.getMessage());
				status = new CrawlingStatus(E_UNEXPECTED_ERROR, UNEXPECTED_ERROR, formatError(exception.getMessage()));
			}
		}

		// check if file is online
		if (pfnToRead != null && status == null) {
			try {
				long start = System.currentTimeMillis();
				String stat = xrootd.xrdstat(pfnToRead, false, false, false);
				long end = System.currentTimeMillis();
				xrdfsDurationMillis = Long.valueOf(end - start);
				xrdfsDuration = xrdfsDurationMillis.longValue();
				xrdfsPFN = 1;
				if (stat != null) {
					final int idx = stat.indexOf("Flags");
					if (idx >= 0 && stat.indexOf("Offline", idx) > 0)
						status = new CrawlingStatus(E_PFN_NOT_ONLINE, FILE_INACCESSIBLE, "PFN is not online");
				}
			}
			catch (IOException exception) {
				exception.printStackTrace();
				logger.log(Level.WARNING, exception.getMessage());

				if (exception instanceof SourceException) {
					SourceExceptionCode code = ((SourceException) exception).getCode();
					CrawlingStatusType type;

					if (code == XROOTD_TIMED_OUT || code == XROOTD_EXITED_WITH_CODE)
						type = FILE_INACCESSIBLE;
					else
						type = INTERNAL_ERROR;

					status = new CrawlingStatus(code, type, formatError(exception.getMessage()));
				}
				else
					status = new CrawlingStatus(E_PFN_XRDSTAT_FAILED, INTERNAL_ERROR,  formatError(exception.getMessage()));
			}
		}

		// check size and checksum
		if (pfnToRead != null && status == null) {
			File downloadedFile = null;

			try {
				long start = System.currentTimeMillis();
				downloadedFile = xrootd.get(pfnToRead, null);
				long end = System.currentTimeMillis();
				downloadDurationMillis = Long.valueOf(end - start);
				downloadDuration = downloadDurationMillis.longValue();
				downloadedPFN = 1;
			}
			catch (IOException exception) {
				exception.printStackTrace();
				logger.log(Level.WARNING, exception.getMessage());

				if (exception instanceof SourceException) {
					SourceExceptionCode code = ((SourceException) exception).getCode();
					CrawlingStatusType type;

					if (code == MD5_CHECKSUMS_DIFFER || code == LOCAL_FILE_SIZE_DIFFERENT)
						type = FILE_CORRUPT;
					else if (code == XROOTD_TIMED_OUT || code == XROOTD_EXITED_WITH_CODE)
						type = FILE_INACCESSIBLE;
					else
						type = INTERNAL_ERROR;

					status = new CrawlingStatus(code, type, formatError(exception.getMessage()));
				}
				else
					status = new CrawlingStatus(E_PFN_DOWNLOAD_FAILED, INTERNAL_ERROR,  formatError(exception.getMessage()));
			}

			if (status == null && downloadedFile != null) {
				try {
					observedFileSize = Long.valueOf(downloadedFile.length());
					observedMD5 = IOUtils.getMD5(downloadedFile);
					catalogueMD5 = guid.md5;
					fileSize = observedFileSize.longValue();

					if (downloadedFile.exists() && !downloadedFile.delete())
						logger.log(Level.INFO, "Cannot delete " + downloadedFile.getName());

					if (catalogueFileSize == 0)
						status = new CrawlingStatus(E_FILE_EMPTY, FILE_CORRUPT, "Catalogue file size is 0");
					else if (catalogueMD5 == null)
						status = new CrawlingStatus(E_FILE_MD5_IS_NULL, FILE_CORRUPT, "Catalogue MD5 is null");
					else if (catalogueMD5.isBlank())
						status = new CrawlingStatus(E_CATALOGUE_MD5_IS_BLANK, FILE_CORRUPT, "Catalogue MD5 is blank");
					else if (!catalogueMD5.equalsIgnoreCase(observedMD5))
						status = new CrawlingStatus(S_FILE_CHECKSUM_MISMATCH, FILE_CORRUPT, "Recomputed checksum does not match catalogue checksum");
					else
						status = new CrawlingStatus(S_FILE_CHECKSUM_MATCH, FILE_OK, "Recomputed checksum matches catalogue checksum");
				}
				catch (IOException exception) {
					exception.printStackTrace();
					logger.log(Level.WARNING, exception.getMessage());
					status = new CrawlingStatus(E_PFN_DOWNLOAD_FAILED, INTERNAL_ERROR,  formatError(exception.getMessage()));
				}
			}
		}

		if (status == null)
			status = new CrawlingStatus(E_UNEXPECTED_ERROR, UNEXPECTED_ERROR, "Unexpected error while processing PFN");


		if (guid != null) {
			PFNData pfnData = new PFNData(
					se.seName,
					currentPFN.pfn,
					status,
					observedFileSize,
					catalogueFileSize,
					observedMD5,
					catalogueMD5,
					downloadDurationMillis,
					xrdfsDurationMillis
			);

			mapGuidToPFN.put(guid.guid.toString(), pfnData);
		}

		return new CrawlingResult(status, fileSize, downloadDuration, downloadedPFN, xrdfsDuration, xrdfsPFN);
	}

	/**
	 * Removes newlines and commas from a string so that it can be used in a CSV file
	 * @param error
	 * @return String
	 */
	private static String formatError(String error) {
		if (outputFileType.equals(OUTPUT_FORMAT_CSV)) {
			error = error.replaceAll("\\R", " ");
			return error.replaceAll(",", " ");
		}
		else
			return error;
	}

	/**
	 * Fill access token of PFN so that it can be read
	 *
	 * @param pfn
	 * @return PFN to read
	 * @throws Exception
	 */
	private static PFN getPFNWithAccessToken(PFN pfn) throws Exception {
		GUID guid = pfn.getGuid();

		if (guid == null)
			throw new Exception("PFN " + pfn.pfn + " has a null GUID");

		List<String> ses = new ArrayList<>();
		List<String> exses = new ArrayList<>();
		ses.add(se.seName);

		Collection<PFN> pfnsToRead = commander.c_api.getPFNsToRead(guid, ses, exses);

		if (pfnsToRead == null)
			throw new Exception("Cannot get PFNs to read for " + pfn.pfn);

		Iterator<PFN> pfnIterator = pfnsToRead.iterator();

		if (!pfnIterator.hasNext())
			throw new Exception("Cannot get PFNs to read for " + pfn.pfn);

		return pfnIterator.next();
	}

	/**
	 * Write the crawling output to disk
	 *
	 * @param fileType
	 */
	private static void writeJobOutputToDisk(Map<String, PFNData> mapGuidToPFN, String fileType) {
		try {
			String jobOutput;

			if (fileType.toLowerCase().equals(OUTPUT_FORMAT_JSON))
				jobOutput = getOutputAsJSON(mapGuidToPFN);
			else
				jobOutput = getOutputAsCSV(mapGuidToPFN);

			CrawlerUtils.writeToDisk(commander, logger, jobOutput, OUTPUT_FILE_NAME, getJobOutputPath());
		}
		catch (Exception e) {
			logger.log(Level.WARNING, "Cannot write output to disk " + e.getMessage());
		}
	}

	/**
	 * @param guidToPFN
	 * @return The job output as JSON
	 */
	@SuppressWarnings("unchecked")
	private static String getOutputAsJSON(Map<String, PFNData> guidToPFN) {
		JSONObject result = new JSONObject();
		result.put(se.seName, new JSONObject());
		JSONObject files = (JSONObject) result.get(se.seName);
		files.putAll(guidToPFN);
		return result.toJSONString();
	}

	/**
	 * @param guidToPFN
	 * @return The crawling output as CSV
	 */
	private static String getOutputAsCSV(Map<String, PFNData> guidToPFN) {
		StringBuilder builder = new StringBuilder();
		builder.append(PFNData.CSV_HEADER);

		for (Map.Entry<String, PFNData> entry : guidToPFN.entrySet()) {
			String guid = entry.getKey();
			PFNData pfnCrawled = entry.getValue();
			String resultCSV = guid + "," + pfnCrawled.toCSV() + "\n";
			builder.append(resultCSV);
		}

		return builder.toString();
	}

	/**
	 * @return The path of the SE in the current iteration
	 */
	private static String getSEPath() {
		return commander.getCurrentDirName() + "iteration_" + iterationUnixTimestamp + "/" + CrawlerUtils.getSEName(se) + "/";
	}

	/**
	 * @return The path of the current crawling job output
	 */
	private static String getJobOutputPath() {
		String fileType = outputFileType.equals(OUTPUT_FORMAT_JSON) ? OUTPUT_FORMAT_JSON : OUTPUT_FORMAT_CSV;
		return getSEPath() + "output/" + OUTPUT_FILE_NAME + "_" + jobIndex + "." + fileType;
	}

	/**
	 * @return The path of the current crawling job statistics
	 */
	public static String getJobStatsPath() {
		return getSEPath() + "stats/" + STATS_FILE_NAME + "_" + jobIndex + ".json";
	}
}