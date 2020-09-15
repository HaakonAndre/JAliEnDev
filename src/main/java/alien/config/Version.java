package alien.config;

import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import lazyj.ExtProperties;

/**
 * @author costing
 * @since 2020-09-15
 */
public class Version {

	private static ExtProperties versionConfig = ConfigUtils.getConfiguration("version");

	private final static String UNKNOWN = "unknown";

	private static Monitor monitor = MonitorFactory.getMonitor(Version.class.getCanonicalName());

	static {
		if (monitor != null)
			monitor.addMonitoring("version_reporter", (keys, values) -> {
				keys.add("tag");
				values.add(getTag());

				keys.add("git_hash");
				values.add(getGitHash());

				keys.add("compilation_timestamp");
				values.add(Double.valueOf(getCompilationTimestamp()));

				keys.add("compiling_hostname");
				values.add(getCompilingHostname());
			});
	}

	/**
	 * @return the latest defined tag at the time of the compilation
	 */
	public static String getTag() {
		if (versionConfig == null)
			return UNKNOWN;

		String tag = versionConfig.gets("git_last_tag", UNKNOWN);

		if (tag.indexOf('-') > 0)
			tag = tag.substring(0, tag.indexOf('-'));

		return tag;
	}

	/**
	 * @return the latest git hash at the time of the compilation
	 */
	public static String getGitHash() {
		if (versionConfig != null)
			return versionConfig.gets("git_hash", UNKNOWN);

		return UNKNOWN;
	}

	/**
	 * @return the epoch time (in millis) when the jar file was generated
	 */
	public static long getCompilationTimestamp() {
		if (versionConfig != null)
			return versionConfig.getl("build_timestamp", 0) * 1000;

		return 0;
	}

	/**
	 * @return the hostname that compiled tha package
	 */
	public static String getCompilingHostname() {
		if (versionConfig != null)
			return versionConfig.gets("build_hostname", UNKNOWN);

		return UNKNOWN;
	}
}
