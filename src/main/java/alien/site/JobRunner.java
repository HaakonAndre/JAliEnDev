package alien.site;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import utils.ProcessWithTimeout;

/**
 * @author sweisz
 * @since Mar 25, 2021
 */
public class JobRunner extends JobAgent {

	/**
	 * logger object
	 */
	private static final Logger logger = ConfigUtils.getLogger(JobRunner.class.getCanonicalName());

	/**
	 * ML monitor object
	 */
	static final Monitor monitor = MonitorFactory.getMonitor(JobRunner.class.getCanonicalName());

	static {
		monitor.addMonitoring("resource_status", (names, values) -> {
			names.add("totalcpu");
			values.add(JobAgent.MAX_CPU);

			names.add("availablecpu");
			values.add(JobAgent.RUNNING_CPU);

			names.add("allocatedcpu");
			values.add(Long.valueOf(JobAgent.MAX_CPU.longValue() - JobAgent.RUNNING_CPU.longValue()));

			names.add("runningja");
			values.add(Long.valueOf(JobAgent.RUNNING_JOBAGENTS));

			names.add("slotlength");
			values.add(Integer.valueOf(JobAgent.origTtl));

		});
	}

	@Override
	public void run() {
		long timestamp = System.currentTimeMillis() / 1000;
		final long ttlEnd = timestamp + JobAgent.origTtl;
		Thread jaThread;
		int i = 0;

		final int maxRetries = Integer.parseInt(System.getenv().getOrDefault("MAX_RETRIES", "5"));

		collectSystemInformation();

		while (timestamp < ttlEnd) {
			synchronized (JobAgent.requestSync) {
				try {
					if (checkParameters() == true) {
						logger.log(Level.INFO, "Spawned thread nr " + i);
						jaThread = new Thread(new JobAgent());
						jaThread.start();
						monitor.sendParameter("state", "Waiting for JA to get a job");
						monitor.sendParameter("statenumeric", Long.valueOf(1));
						i++;
					}
					else {
						monitor.sendParameter("state", "All slots busy");
						monitor.sendParameter("statenumeric", Long.valueOf(3));
						logger.log(Level.INFO, "No new thread");
					}

					JobAgent.requestSync.wait(5 * 60 * 1000);

				}
				catch (final InterruptedException e) {
					logger.log(Level.WARNING, "JobRunner interrupted", e);
				}

				timestamp = System.currentTimeMillis() / 1000;

				monitor.incrementCounter("startedja");

				monitor.sendParameter("retries", Long.valueOf(JobAgent.retries.get()));

				monitor.sendParameter("remainingttl", Long.valueOf(ttlEnd - timestamp));

				if (JobAgent.retries.get() == maxRetries) {
					monitor.sendParameter("state", "Last JA cannot get job");
					monitor.sendParameter("statenumeric", Long.valueOf(2));
					logger.log(Level.INFO, "JobRunner going to exit from lack of jobs");
					break;
				}
			}
		}
		System.out.println("JobRunner Exiting");
	}

	private static final String SITESONAR = "/cvmfs/alice.cern.ch/sitesonar/sitesonar.sh";

	private static void collectSystemInformation() {
		final File f = new File(SITESONAR);

		if (f.exists() && f.canExecute()) {
			final ProcessBuilder pBuilder = new ProcessBuilder(SITESONAR);

			try {
				final Process p = pBuilder.start();

				if (p != null) {
					final ProcessWithTimeout ptimeout = new ProcessWithTimeout(p, pBuilder);
					ptimeout.waitFor(5, TimeUnit.MINUTES);

					if (!ptimeout.exitedOk())
						logger.log(Level.WARNING, "Sitesonar didn't finish in due time");
				}
			}
			catch (@SuppressWarnings("unused") final IOException | InterruptedException e) {
				// ignore
			}
		}
	}

	public static void main(final String[] args) {
		ConfigUtils.setApplicationName("JobRunner");
		ConfigUtils.switchToForkProcessLaunching();
		final JobRunner jr = new JobRunner();
		jr.run();
	}
}
