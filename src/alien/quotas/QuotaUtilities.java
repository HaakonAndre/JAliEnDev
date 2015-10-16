/**
 * 
 */
package alien.quotas;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import lazyj.Format;
import alien.catalogue.CatalogueUtils;
import alien.config.ConfigUtils;
import alien.taskQueue.TaskQueueUtils;

/**
 * @author costing
 * @since Nov 4, 2010
 */
public final class QuotaUtilities {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(QuotaUtilities.class.getCanonicalName());

	private static Map<String, Quota> jobQuotas = null;
	private static long jobQuotasLastUpdated = 0;

	private static final ReentrantReadWriteLock jobQuotasRWLock = new ReentrantReadWriteLock();
	private static final ReadLock jobQuotaReadLock = jobQuotasRWLock.readLock();
	private static final WriteLock jobQuotaWriteLock = jobQuotasRWLock.writeLock();

	private static void updateJobQuotasCache() {
		jobQuotaReadLock.lock();

		try {
			if (System.currentTimeMillis() - jobQuotasLastUpdated > CatalogueUtils.CACHE_TIMEOUT || jobQuotas == null) {
				jobQuotaReadLock.unlock();

				jobQuotaWriteLock.lock();

				try {
					if (System.currentTimeMillis() - jobQuotasLastUpdated > CatalogueUtils.CACHE_TIMEOUT || jobQuotas == null) {
						if (logger.isLoggable(Level.FINER))
							logger.log(Level.FINER, "Updating Quotas cache");

						try (DBFunctions db = ConfigUtils.getDB("processes")) {
							db.setReadOnly(true);

							String q = "SELECT * FROM PRIORITY";

							if (TaskQueueUtils.dbStructure2_20)
								q += " inner join QUEUE_USER using(userId)";

							if (db.query(q)) {
								final Map<String, Quota> newQuotas = new HashMap<>();

								while (db.moveNext()) {
									final Quota quota = new Quota(db);

									if (quota.user != null)
										newQuotas.put(quota.user, quota);
								}

								jobQuotas = Collections.unmodifiableMap(newQuotas);
								jobQuotasLastUpdated = System.currentTimeMillis();
							} else
								jobQuotasLastUpdated = System.currentTimeMillis() - CatalogueUtils.CACHE_TIMEOUT + 1000 * 10;
						}
					}
				} finally {
					jobQuotaWriteLock.unlock();
				}

				jobQuotaReadLock.lock();
			}
		} finally {
			jobQuotaReadLock.unlock();
		}
	}

	private static Map<String, FileQuota> fileQuotas = null;
	private static long fileQuotasLastUpdated = 0;

	private static final ReentrantReadWriteLock fileQuotasRWLock = new ReentrantReadWriteLock();
	private static final ReadLock fileQuotaReadLock = fileQuotasRWLock.readLock();
	private static final WriteLock fileQuotaWriteLock = fileQuotasRWLock.writeLock();

	private static void updateFileQuotasCache() {
		fileQuotaReadLock.lock();

		try {
			if (System.currentTimeMillis() - fileQuotasLastUpdated > CatalogueUtils.CACHE_TIMEOUT || fileQuotas == null) {
				fileQuotaReadLock.unlock();

				fileQuotaWriteLock.lock();

				try {
					if (System.currentTimeMillis() - fileQuotasLastUpdated > CatalogueUtils.CACHE_TIMEOUT || fileQuotas == null) {
						if (logger.isLoggable(Level.FINER))
							logger.log(Level.FINER, "Updating File Quota cache");

						try (DBFunctions db = ConfigUtils.getDB("alice_users")) {
							db.setReadOnly(true);

							if (db.query("SELECT * FROM FQUOTAS;")) {
								final Map<String, FileQuota> newQuotas = new HashMap<>();

								while (db.moveNext()) {
									final FileQuota fq = new FileQuota(db);

									if (fq.user != null)
										newQuotas.put(fq.user, fq);
								}

								fileQuotas = Collections.unmodifiableMap(newQuotas);
								fileQuotasLastUpdated = System.currentTimeMillis();
							} else
								fileQuotasLastUpdated = System.currentTimeMillis() - CatalogueUtils.CACHE_TIMEOUT + 1000 * 10;
						}
					}
				} finally {
					fileQuotaWriteLock.unlock();
				}

				fileQuotaReadLock.lock();
			}
		} finally {
			fileQuotaReadLock.unlock();
		}
	}

	/**
	 * Get the job quota for a particular account
	 * 
	 * @param account
	 * @return job quota
	 */
	public static Quota getJobQuota(final String account) {
		if (account == null || account.length() == 0)
			return null;

		updateJobQuotasCache();

		if (jobQuotas == null)
			return null;

		return jobQuotas.get(account.toLowerCase());
	}

	/**
	 * Sets job quota field value for a username
	 * 
	 * @param username
	 * @param fld
	 * @param val
	 * @return <code>true</code> if the field was updated
	 */
	public static boolean saveJobQuota(final String username, final String fld, final String val) {
		if (!Quota.canUpdateField(fld))
			return false;
		try (DBFunctions db = ConfigUtils.getDB("processes")) {
			final String query = "UPDATE PRIORITY p LEFT JOIN QUEUE_USER qu " + "ON qu.user='" + Format.escSQL(username) + "' SET p." + Format.escSQL(fld) + "=" + Format.escSQL(val)
					+ " WHERE qu.userid=p.userid";
			db.query(query);
			jobQuotasLastUpdated = 0;
			updateJobQuotasCache();
		}

		return true;
	}

	/**
	 * Get the file quota for a particular account
	 * 
	 * @param account
	 * @return file quota
	 */
	public static FileQuota getFileQuota(final String account) {
		if (account == null || account.length() == 0)
			return null;

		updateFileQuotasCache();

		if (fileQuotas == null)
			return null;

		return fileQuotas.get(account.toLowerCase());
	}

	/**
	 * Sets file quota field value for a username
	 * 
	 * @param username
	 * @param fld
	 * @param val
	 * @return <code>true</code> if the field was updated
	 */
	public static boolean saveFileQuota(final String username, final String fld, final String val) {
		if (!FileQuota.canUpdateField(fld))
			return false;

		try (DBFunctions db = ConfigUtils.getDB("alice_users")) {
			final String query = "UPDATE FQUOTAS SET " + Format.escSQL(fld) + "='" + Format.escSQL(val) + "'" + " WHERE user='" + Format.escSQL(username) + "'";
			db.query(query);
			fileQuotasLastUpdated = 0;
			updateFileQuotasCache();
		}

		return true;
	}

	/**
	 * Get the list of quotas for all accounts
	 * 
	 * @return file quota for all accounts, sorted by username
	 */
	public static final List<Quota> getJobQuotas() {
		updateJobQuotasCache();

		if (jobQuotas == null)
			return null;

		final ArrayList<Quota> ret = new ArrayList<>(jobQuotas.values());

		Collections.sort(ret);

		return ret;
	}

	/**
	 * Get the list of quotas for all accounts
	 * 
	 * @return file quota for all accounts, sorted by username
	 */
	public static final List<FileQuota> getFileQuotas() {
		updateFileQuotasCache();

		if (fileQuotas == null)
			return null;

		final ArrayList<FileQuota> ret = new ArrayList<>(fileQuotas.values());

		Collections.sort(ret);

		return ret;
	}
}
