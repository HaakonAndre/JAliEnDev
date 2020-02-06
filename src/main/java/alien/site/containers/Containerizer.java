package alien.site.containers;

import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.site.JobAgent;
import alien.site.packman.CVMFS;

/**
 * @author mstoretv
 */
public abstract class Containerizer {

	final String DEFAULT_JOB_CONTAINER_PATH = "centos-7";
	final String CONTAINER_JOBDIR = "/workdir";
	final String envSetup = "source <( " + CVMFS.getAlienvForSource() + " ); ";

	static final Logger logger = ConfigUtils.getLogger(JobAgent.class.getCanonicalName());
	final String containerImgPath;

	String workdir = null;

	/**
	 * Simple constructor, initializing the container path from default location or from the environment (DEFAULT_JOB_CONTAINER_PATH key)
	 */
	public Containerizer() {
		containerImgPath = System.getenv().getOrDefault("JOB_CONTAINER_PATH", DEFAULT_JOB_CONTAINER_PATH);
		if (containerImgPath.equals(DEFAULT_JOB_CONTAINER_PATH)) {
			logger.log(Level.INFO, "Environment variable JOB_CONTAINER_PATH not set. Using default path instead: " + DEFAULT_JOB_CONTAINER_PATH);
		}
	}

	/**
	 * @return <code>true</code> if running a simple command (java -version) is possible under the given container implementation
	 */
	public boolean isSupported() {
		final String javaTest = "java -version";

		boolean supported = false;
		try {
			final ProcessBuilder pb = new ProcessBuilder(containerize(javaTest));
			final Process probe = pb.start();
			probe.waitFor();

			try (Scanner cmdScanner = new Scanner(probe.getErrorStream())) {
				while (cmdScanner.hasNext()) {
					if (cmdScanner.next().contains("Runtime")) {
						supported = true;
					}
				}
			}
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Failed to start container: " + e.toString());
		}
		return supported;
	}

	/**
	 * Decorating arguments to run the given command under a container
	 *
	 * @param cmd
	 * @return parameter
	 */
	public abstract List<String> containerize(String cmd);

	/**
	 * @param newWorkdir
	 */
	public void setWorkdir(final String newWorkdir) {
		workdir = newWorkdir;
	}

	/**
	 * @return working directory
	 */
	public String getWorkdir() {
		return workdir;
	}

	/**
	 * @return Class name of the container wrapping code
	 */
	public String getContainerizerName() {
		return this.getClass().getName();
	}
}
