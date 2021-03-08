package utils.crawler;

import alien.catalogue.LFN;
import alien.config.ConfigUtils;
import alien.io.protocols.SourceExceptionCode;
import alien.se.SE;
import alien.shell.commands.JAliEnCOMMander;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import utils.StatusCode;
import utils.StatusType;

/**
 * @author anegru
 */
public class CrawlerUtils {
	/**
	 * Logger
	 */
	private static final Logger logger = ConfigUtils.getLogger(CrawlerUtils.class.getCanonicalName());

	public CrawlerUtils() {

	}

	public static final List<StatusCode> getStatuses() {
		List<StatusCode> crawlingStatuses = new ArrayList<>();
		crawlingStatuses.addAll(Arrays.asList(SourceExceptionCode.values()));
		crawlingStatuses.addAll(Arrays.asList(CrawlingStatusCode.values()));
		return Collections.unmodifiableList(crawlingStatuses);
	}

	public static final List<String> getStatusTypes() {
		return Arrays.stream(StatusType.values()).map(Enum::toString).collect(Collectors.toUnmodifiableList());
	}

	public static String getSEName(SE se) {
		return se.seName.replace("::", "_");
	}

	public static void uploadToGrid(JAliEnCOMMander commander, File f, String remoteFullPath) throws IOException {

		logger.log(Level.INFO, "Uploading " + remoteFullPath);

		LFN lfnUploaded = commander.c_api.uploadFile(f, remoteFullPath);

		if(lfnUploaded == null)
			logger.log(Level.WARNING, "Uploading " + remoteFullPath + " failed");
		else
			logger.log(Level.INFO, "Successfully uploaded " + remoteFullPath);
	}

	public static void uploadToGrid(JAliEnCOMMander commander, String contents, String localFileName, String remoteFullPath) {
		final File f = new File(localFileName);
		try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(f))) {
			bufferedWriter.write(contents);
			bufferedWriter.flush();
			bufferedWriter.close();
			uploadToGrid(commander, f, remoteFullPath);
		}
		catch (IOException e) {
			e.printStackTrace();
			logger.log(Level.SEVERE, "Cannot write to disk " + e.getMessage());
		}
		finally {
			try {
				if (f.exists() && !f.delete())
					logger.log(Level.INFO, "Cannot delete already existing local file " + f.getCanonicalPath());
			}
			catch (Exception e) {
				e.printStackTrace();
				logger.log(Level.WARNING, "Cannot delete already existing local file " + e.getMessage());
			}
		}
	}

	/**
	 * Get the full list of SEs that have to be crawled. Only SEs with type 'disk' are selected
	 *
	 * @return List<SE>
	 * @throws Exception
	 */
	public static List<SE> getStorageElementsForCrawling(JAliEnCOMMander commander) throws Exception {
		Collection<SE> ses = commander.c_api.getSEs(new ArrayList<>());

		if (ses == null)
			throw new Exception("Cannot retrieve SEs");

		Predicate<SE> byType = se -> se.isQosType("disk");
		return ses.stream().filter(byType).collect(Collectors.toList());
	}

	/**
	 * Return the path to the SE given as parameter for the current iteration
	 *
	 * @param se The SE for which to get the directory path
	 * @return String
	 */
	public static String getSEIterationDirectoryPath(JAliEnCOMMander commander, SE se, String iterationUnixTimestamp) {
		return getIterationDirectoryPath(commander, iterationUnixTimestamp) + CrawlerUtils.getSEName(se) + "/";
	}

	/**
	 * Return the path of the current iteration
	 *
	 * @return String
	 */
	private static String getIterationDirectoryPath(JAliEnCOMMander commander, String iterationUnixTimestamp) {
		return commander.getCurrentDirName() + "iteration_" + iterationUnixTimestamp + "/";
	}
}

