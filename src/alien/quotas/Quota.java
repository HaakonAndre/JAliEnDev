/**
 * 
 */
package alien.quotas;

import java.io.Serializable;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import lia.util.StringFactory;
import alien.config.ConfigUtils;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author costing
 * @since Nov 4, 2010
 */
public class Quota implements Serializable, Comparable<Quota> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6590424126764110021L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(Quota.class.getCanonicalName());

	/*
	 * user | varchar(64) priority | float maxparallelJobs | int(11) userload | float nominalparallelJobs | int(11) computedpriority | float waiting | int(11) running | int(11) maxUnfinishedJobs |
	 * int(11) maxTotalCpuCost | float totalRunningTimeLast24h | bigint(20) unfinishedJobsLast24h | int(11) totalSize | bigint(20) maxNbFiles | int(11) nbFiles | int(11) tmpIncreasedTotalSize |
	 * bigint(20) totalCpuCostLast24h | float maxTotalSize | bigint(20) maxTotalRunningTime | bigint(20) tmpIncreasedNbFiles | int(11)
	 */

	/**
	 * Account name
	 */
	public final String user;

	/**
	 * Priority
	 */
	public final float priority;

	/**
	 * Max parallel jobs
	 */
	public final int maxparallelJobs;

	/**
	 * User load
	 */
	public final float userload;

	/**
	 * Current number of parallel jobs
	 */
	public final int nominalparallelJobs;

	/**
	 * Computed priority
	 */
	public final float computedpriority;

	/**
	 * Waiting jobs
	 */
	public final int waiting;

	/**
	 * Running jobs
	 */
	public final int running;

	/**
	 * Max unfinished jobs
	 */
	public final int maxUnfinishedJobs;

	/**
	 * Max total CPU cost
	 */
	public final float maxTotalCpuCost;

	/**
	 * Total running time in the last 24 h
	 */
	public final long totalRunningTimeLast24h;

	/**
	 * Unfinished jobs in the last 24 h
	 */
	public final int unfinishedJobsLast24h;

	/**
	 * Total size of the files
	 */
	public final long totalSize;

	/**
	 * Maximum number of files
	 */
	public final int maxNbFiles;

	/**
	 * Current number of files
	 */
	public final int nbFiles;

	/**
	 * Temp increased total size
	 */
	public final long tmpIncreasedTotalSize;

	/**
	 * Total CPU cost in the last 24 h
	 */
	public final float totalCpuCostLast24h;

	/**
	 * Maximum total size
	 */
	public final long maxTotalSize;

	/**
	 * Maximum total running time
	 */
	public final long maxTotalRunningTime;

	/**
	 * Temp increased number of files
	 */
	public final int tmpIncreasedNbFiles;

	/**
	 * Fields allowed to modify via jquota set command
	 */
	private final static ArrayList<String> allowed_to_update = new ArrayList<>(Arrays.asList("maxUnfinishedJobs", "maxTotalCpuCost", "maxTotalRunningTime"));

	/**
	 * @param db
	 */
	public Quota(final DBFunctions db) {
		user = StringFactory.get(db.gets("user").toLowerCase());

		priority = db.getf("priority");

		maxparallelJobs = db.geti("maxparallelJobs");

		userload = db.getf("userload");

		nominalparallelJobs = db.geti("nominalparallelJobs");

		computedpriority = db.getf("computedpriority");

		waiting = db.geti("waiting");

		running = db.geti("running");

		maxUnfinishedJobs = db.geti("maxUnfinishedJobs");

		maxTotalCpuCost = db.getf("maxTotalCpuCost");

		totalRunningTimeLast24h = db.getl("totalRunningTimeLast24h");

		unfinishedJobsLast24h = db.geti("unfinishedJobsLast24h");

		totalSize = db.getl("totalSize");

		maxNbFiles = db.geti("maxNbFiles");

		nbFiles = db.geti("nbFiles");

		tmpIncreasedTotalSize = db.getl("tmpIncreasedTotalSize");

		totalCpuCostLast24h = db.getf("totalCpuCostLast24h");

		maxTotalSize = db.getl("maxTotalSize");

		maxTotalRunningTime = db.getl("maxTotalRunningTime");

		tmpIncreasedNbFiles = db.geti("tmpIncreasedNbFiles");
	}

	@Override
	public String toString() {
		return "Quota: user: " + user + "\n" + "priority\t: " + priority + "\n" + "maxparallelJobs\t: " + maxparallelJobs + "\n" + "userload\t: " + userload + "\n" + "nominalparallelJobs\t: "
				+ nominalparallelJobs + "\n" + "computedpriority\t: " + computedpriority + "\n" + "waiting\t: " + waiting + "\n" + "running\t: " + running + "\n" + "maxUnfinishedJobs\t: "
				+ maxUnfinishedJobs + "\n" + "maxTotalCpuCost\t: " + maxTotalCpuCost + "\n" + "totalRunningTimeLast24h\t: " + totalRunningTimeLast24h + "\n" + "unfinishedJobsLast24h\t: "
				+ unfinishedJobsLast24h + "\n" + "totalCpuCostLast24h\t: " + totalCpuCostLast24h + "\n" + "maxTotalRunningTime\t: " + maxTotalRunningTime;
	}

	@Override
	public int compareTo(final Quota o) {
		return user.compareTo(o.user);
	}

	@Override
	public boolean equals(final Object obj) {
		if (!(obj instanceof Quota))
			return false;

		return compareTo((Quota) obj) == 0;
	}

	@Override
	public int hashCode() {
		return user.hashCode();
	}

	/**
	 * @return <code>true</code> if the user is below the quota and is allowed to submit more jobs
	 */
	public boolean canSubmit() {
		if (totalCpuCostLast24h < maxTotalCpuCost && totalRunningTimeLast24h < maxTotalRunningTime && (running + waiting) < maxUnfinishedJobs)
			return true;

		return false;
	}

	public static boolean canUpdateField(String fieldname) {
		return allowed_to_update.contains(fieldname);
	}
}
