package alien.site;

import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;

public class JobRunner extends JobAgent {

	/**
	 * logger object
	 */
	final Logger logger = ConfigUtils.getLogger(JobRunner.class.getCanonicalName());

	public void run() {
		long timestamp = System.currentTimeMillis()/1000;
		long ttlEnd = timestamp + JobAgent.origTtl;
		Thread jaThread;
		int i = 0;
		Integer maxRetries = 5;

		maxRetries = Integer.valueOf(System.getenv().getOrDefault("MAX_RETRIES", maxRetries.toString()));

		while (timestamp < ttlEnd) {
			synchronized (JobAgent.requestSync) {
				try {
					if (checkParameters() == true) {
						logger.log(Level.INFO, "Spawned thread nr " + i);
						jaThread = new Thread(new JobAgent());
						jaThread.start();
						i++;
					} else {
						logger.log(Level.INFO, "No new thread");
					}

					JobAgent.requestSync.wait(5 * 60 * 1000);

				} catch (InterruptedException e) {
					logger.log(Level.WARNING, "JobRunner interrupted");
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
		JobRunner jr = new JobRunner();
		jr.run();
	}
}
