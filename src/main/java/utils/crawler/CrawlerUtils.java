package utils.crawler;

import alien.catalogue.LFN;
import alien.io.IOUtils;
import alien.se.SE;
import alien.shell.commands.JAliEnCOMMander;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author anegru
 */
class CrawlerUtils {

	public static String getSEName(SE se) {
		return se.seName.replace("::", "_");
	}

	public static void writeToDisk(JAliEnCOMMander commander, Logger logger, File f, String remoteFullPath) throws IOException {

		LFN lfn = commander.c_api.getLFN(remoteFullPath, true);

		if (lfn != null && lfn.exists)
			throw new IOException("LFN " + lfn.getCanonicalName() + " already exists");

		logger.log(Level.INFO, "Uploading " + remoteFullPath);
		IOUtils.upload(f, remoteFullPath, commander.getUser(), 3, null, true);
	}

	public static void writeToDisk(JAliEnCOMMander commander, Logger logger, String contents, String localFileName, String remoteFullPath) throws IOException {
		final File f = new File(localFileName);

		try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(f))) {
			bufferedWriter.write(contents);
			bufferedWriter.flush();
			bufferedWriter.close();
			writeToDisk(commander, logger, f, remoteFullPath);
		}
		finally {
			if (f.exists() && !f.delete())
				logger.log(Level.INFO, "Cannot delete already existing local file " + f.getCanonicalPath());
		}
	}
}

