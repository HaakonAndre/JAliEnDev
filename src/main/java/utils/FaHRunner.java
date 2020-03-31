/**
 *
 */
package utils;

import java.io.File;
import java.io.IOException;

import alien.api.Dispatcher;
import alien.api.taskQueue.FaHTask;
import alien.catalogue.LFN;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.shell.commands.JAliEnCOMMander;
import alien.user.AliEnPrincipal;
import alien.user.JAKeyStore;
import alien.user.UsersHelper;

/**
 * @author costing
 * @since Mar 31, 2020
 */
public class FaHRunner {

	/**
	 * Entry point where the Folding@Home job starts
	 *
	 * @param args
	 * @throws Exception
	 */
	public static void main(final String[] args) throws Exception {
		ConfigUtils.setApplicationName("FaH");

		if (!JAKeyStore.loadKeyStore()) {
			System.err.println("No identity found, exiting");
			return;
		}

		final AliEnPrincipal account = AuthorizationFactory.getDefaultUser();

		final long currentJobID = ConfigUtils.getConfig().getl("ALIEN_PROC_ID", -1);

		if (currentJobID < 0) {
			System.err.println("Cannot get the job ID, exiting");
			return;
		}

		final FaHTask task = Dispatcher.execute(new FaHTask(currentJobID));

		if (task.getSequenceId() < 0) {
			System.err.println("No task for me, exiting");
			return;
		}

		final String baseFolder = UsersHelper.getDefaultUserDir(account.getDefaultUser() + "/fah/" + task.getSequenceId());

		final String snapshotArchive = baseFolder + "/snapshot.tar.gz";

		final JAliEnCOMMander commander = JAliEnCOMMander.getInstance();

		final LFN lSnapshot = commander.c_api.getLFN(snapshotArchive);

		if (lSnapshot != null) {
			try {
				commander.c_api.downloadFile(snapshotArchive, new File("snapshot.tar.gz"));
			}
			catch (final IOException ioe) {
				System.err.println("Snapshot cannot be retrieved due to " + ioe.getMessage());
				System.err.println("Continuing with an empty sandbox");
			}
		}

		final ProcessBuilder pBuilder = new ProcessBuilder("./fah.sh");
		final Process p = pBuilder.start();
		p.waitFor();

		// ok, now we have to upload the results, if any
		final File outputSnapshot = new File("snapshot.tar.gz");

		if (outputSnapshot.exists() && outputSnapshot.length() > 0) {
			if (lSnapshot != null)
				commander.c_api.removeLFN(snapshotArchive);

			commander.c_api.uploadFile(outputSnapshot, snapshotArchive);
		}
	}
}
