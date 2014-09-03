package alien.catalogue;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcessBuilder;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.taskQueue.JDL;

/**
 * @author ron
 * @since Nov 23, 2011
 */
public class PackageUtils {
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(IndexTableEntry.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(IndexTableEntry.class.getCanonicalName());

	private static long lastCacheCheck = 0;

	private static Map<String, Package> packages = null;

	private static Set<String> cvmfsPackages = null;

	private static synchronized void cacheCheck() {
		if ((System.currentTimeMillis() - lastCacheCheck) > 1000 * 60) {
			final Map<String, Package> newPackages = new LinkedHashMap<>();

			final DBFunctions db = ConfigUtils.getDB("alice_users");

			if (monitor != null)
				monitor.incrementCounter("Package_db_lookup");

			final String q = "SELECT DISTINCT packageVersion, packageName, username, platform, lfn FROM PACKAGES ORDER BY 3,2,1,4,5;";

			try {
				if (!db.query(q))
					return;

				Package prev = null;

				while (db.moveNext()) {
					final Package next = new Package(db);

					if (prev != null && next.equals(prev))
						prev.setLFN(db.gets("platform"), db.gets("lfn"));
					else {
						next.setLFN(db.gets("platform"), db.gets("lfn"));
						prev = next;

						newPackages.put(next.getFullName(), next);
					}
				}
			} finally {
				db.close();
			}

			final Set<String> newCvmfsPackages = new HashSet<>();

			try {
				final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder("/cvmfs/alice.cern.ch/bin/alienv", "q");

				pBuilder.returnOutputOnExit(true);
				pBuilder.timeout(60, TimeUnit.SECONDS);

				pBuilder.redirectErrorStream(false);

				pBuilder.redirectErrorStream(false);

				final ExitStatus exitStatus = pBuilder.start().waitFor();

				if (exitStatus.getExtProcExitStatus() == 0) {
					final BufferedReader br = new BufferedReader(new StringReader(exitStatus.getStdOut()));

					String line;

					while ((line = br.readLine()) != null)
						newCvmfsPackages.add(line.trim());
				}
			} catch (final Throwable t) {
				logger.log(Level.WARNING, "Exception getting the CVMFS package list", t);
			}

			lastCacheCheck = System.currentTimeMillis();
			packages = newPackages;
			cvmfsPackages = newCvmfsPackages;
		}
	}

	/**
	 * @return list of defined packages
	 */
	public static List<Package> getPackages() {
		cacheCheck();

		if (packages != null)
			return new ArrayList<>(packages.values());

		return null;
	}

	/**
	 * @return the set of known package names
	 */
	public static Set<String> getPackageNames() {
		cacheCheck();

		if (packages != null)
			return packages.keySet();

		return null;
	}

	public static Set<String> getCvmfsPackages() {
		cacheCheck();

		return cvmfsPackages;
	}

	/**
	 * @param j
	 *            JDL to check
	 * @return <code>null</code> if the requirements are met and the JDL can be submitted, or a String object with the message detailing what condition was not met.
	 */
	public static String checkPackageRequirements(final JDL j) {
		if (j == null)
			return "JDL is null";

		cacheCheck();

		if (packages == null)
			return "Package list could not be fetched from the database";

		final List<String> packageVersions = j.getList("Packages");

		if (packageVersions == null || packageVersions.size() == 0)
			return null;

		for (final String requiredPackage : packageVersions) {
			if (!packages.containsKey(requiredPackage))
				return "Package not defined: " + requiredPackage;

			if (cvmfsPackages != null && cvmfsPackages.size() > 0 && !cvmfsPackages.contains(requiredPackage))
				return "Package not seen yet in CVMFS: " + requiredPackage;
		}

		return null;
	}
}
