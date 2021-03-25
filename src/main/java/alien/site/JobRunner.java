package alien.site;

import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;

/**
 * @author sweisz
 * @since Mar 25, 2021
 */
public class JobRunner extends JobAgent {

	/**
	 * logger object
	 */
	final Logger logger = ConfigUtils.getLogger(JobRunner.class.getCanonicalName());

	@Override
	public void run() {
		long timestamp = System.currentTimeMillis() / 1000;
		final long ttlEnd = timestamp + JobAgent.origTtl;
		Thread jaThread;
		int i = 0;

		final int maxRetries = Integer.parseInt(System.getenv().getOrDefault("MAX_RETRIES", "5"));

		while (timestamp < ttlEnd) {
			synchronized (JobAgent.requestSync) {
				try {
					if (checkParameters() == true) {
						logger.log(Level.INFO, "Spawned thread nr " + i);
						jaThread = new Thread(new JobAgent());
						jaThread.start();
						i++;
					}
					else {
						logger.log(Level.INFO, "No new thread");
					}

					JobAgent.requestSync.wait(5 * 60 * 1000);

				}
				catch (final InterruptedException e) {
					logger.log(Level.WARNING, "JobRunner interrupted", e);
				}

				timestamp = System.currentTimeMillis() / 1000;

				if (JobAgent.retries.get() == maxRetries) {
					logger.log(Level.INFO, "JobRunner going to exit from lack of jobs");
					break;
				}
			}
		}
		System.out.println("JobRunner Exiting");
	}

	public static void main(final String[] args) {
		ConfigUtils.setApplicationName("JobRunner");
		ConfigUtils.switchToForkProcessLaunching();
		final JobRunner jr = new JobRunner();
		jr.run();
	}
}
