package utils.crawler;

import alien.catalogue.LFN;
import alien.config.ConfigUtils;
import alien.shell.commands.JAliEnCOMMander;
import alien.user.JAKeyStore;
import java.util.List;
import java.util.logging.Logger;

/**
 * Clean old iteration data from the grid
 */
public class GridCleanup {

	private static final Integer TWO_WEEKS = 2 * 7 * 24 * 60 * 60 * 1000;
	private static final Logger logger = ConfigUtils.getLogger(GridCleanup.class.getCanonicalName());
	private static JAliEnCOMMander commander = JAliEnCOMMander.getInstance();

	public static void main(String args[]) {
		try {
			ConfigUtils.setApplicationName("GridCleanup");
			ConfigUtils.switchToForkProcessLaunching();

			if (!JAKeyStore.loadKeyStore()) {
				return;
			}

			performGridCleanup();
		}
		catch (Exception e) {
			logger.severe(e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * Removes all files that are older than two weeks from the grid.
	 */
	private static void performGridCleanup() throws Exception {

		List<LFN> filePaths = commander.c_api.getLFNs(commander.getCurrentDirName());
		long currentTimestamp = System.currentTimeMillis();

		for (LFN lfn : filePaths) {
			try {
				if (lfn.isDirectory() && lfn.getFileName().matches("iteration_[0-9]+")) {
					long iterationTimestamp = Long.parseLong(lfn.getFileName().split("_")[1]) * 1000;
					if (currentTimestamp - iterationTimestamp > TWO_WEEKS) {
						String remotePath = lfn.getCanonicalName();
						System.out.println("Removing " + remotePath);
						commander.c_api.removeLFN(remotePath, true, true);
					}
				}
			}
			catch (Exception e) {
				logger.warning(e.getMessage());
				e.printStackTrace();
			}
		}
	}
}
