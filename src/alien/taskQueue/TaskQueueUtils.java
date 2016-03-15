package alien.taskQueue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.LFNfromString;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PackageUtils;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;
import alien.user.LDAPHelper;
import alien.user.UsersHelper;
import lazyj.DBFunctions;
import lazyj.Format;
import lazyj.StringFactory;
import lazyj.Utils;
import lazyj.cache.GenericLastValuesCache;

/**
 * @author ron
 * @since Mar 1, 2011
 */
public class TaskQueueUtils {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(TaskQueueUtils.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(TaskQueueUtils.class.getCanonicalName());

	/**
	 * Flag that tells if the QUEUE table is v2.20+ (JDL text in QUEUEJDL, using status, host, user, notification ids and so on instead of the string versions)
	 */
	public static final boolean dbStructure2_20;

	private static final Map<String, String> fieldMap;

	static {
		fieldMap = new HashMap<String, String>();
		fieldMap.put("path_table", "QUEUEJDL");
		fieldMap.put("path_field", "path");
	}

	static {
		try (DBFunctions db = getQueueDB()) {
			if (db != null) {
				db.setReadOnly(true);

				db.query("select count(1) from information_schema.tables where table_schema='processes' and table_name='QUEUEJDL';");

				dbStructure2_20 = db.geti(1) == 1;
			} else {
				logger.log(Level.WARNING, "There is no direct database connection to the task queue.");

				dbStructure2_20 = false;
			}
		}
	}

	// private static final DateFormat formatter = new
	// SimpleDateFormat("MMM dd HH:mm");

	/**
	 * @return the database connection to 'processes'
	 */
	public static DBFunctions getQueueDB() {
		final DBFunctions db = ConfigUtils.getDB("processes");
		return db;
	}

	/**
	 * @return the database connection to 'ADMIN'
	 */
	public static DBFunctions getAdminDB() {
		final DBFunctions db = ConfigUtils.getDB("admin");
		return db;
	}

	/**
	 * Get the Job from the QUEUE
	 *
	 * @param queueId
	 * @return the job, or <code>null</code> if it cannot be located
	 */
	public static Job getJob(final int queueId) {
		return getJob(queueId, false);
	}

	private static final String ALL_BUT_JDL = "queueId, priority, execHost, sent, split, name, spyurl, commandArg, finished, masterjob, status, splitting, node, error, current, received, validate, command, merging, submitHost, path, site, started, expires, finalPrice, effectivePriority, price, si2k, jobagentId, agentid, notify, chargeStatus, optimized, mtime";

	/**
	 * Get the Job from the QUEUE
	 *
	 * @param queueId
	 * @param loadJDL
	 * @return the job, or <code>null</code> if it cannot be located
	 */
	public static Job getJob(final int queueId, final boolean loadJDL) {
		return getJob(queueId, loadJDL, 0);
	}

	/**
	 * Get the Job from the QUEUE
	 *
	 * @param queueId
	 * @param loadJDL
	 * @param archiveYear
	 *            queue archive year to query instead of the main queue
	 * @return the job, or <code>null</code> if it cannot be located
	 */
	public static Job getJob(final int queueId, final boolean loadJDL, final int archiveYear) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return null;

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_lookup");
				monitor.incrementCounter("TQ_jobdetails");
			}

			final long lQueryStart = System.currentTimeMillis();

			final String q;

			if (dbStructure2_20) {
				if (archiveYear < 2000) {
					if (loadJDL)
						q = "SELECT QUEUE.*,origJdl as JDL FROM QUEUE INNER JOIN QUEUEJDL using(queueId) WHERE queueId=?";
					else
						q = "SELECT * FROM QUEUE WHERE queueId=?";
				} else if (loadJDL)
					q = "SELECT *,origJdl as JDL FROM QUEUEARCHIVE" + archiveYear + " WHERE queueId=?";
				else
					q = "SELECT * FROM QUEUEARCHIVE" + archiveYear + " WHERE queueId=?";
			} else if (archiveYear < 2000)
				q = "SELECT " + (loadJDL ? "*" : ALL_BUT_JDL) + " FROM QUEUE WHERE queueId=?";
			else
				q = "SELECT " + (loadJDL ? "*" : ALL_BUT_JDL) + " FROM QUEUEARCHIVE" + archiveYear + " WHERE queueId=?";

			db.setReadOnly(true);

			if (!db.query(q, false, Integer.valueOf(queueId)))
				return null;

			monitor.addMeasurement("TQ_jobdetails_time", (System.currentTimeMillis() - lQueryStart) / 1000d);

			if (!db.moveNext())
				return null;

			return new Job(db, loadJDL);
		}
	}

	/**
	 * Get the list of active masterjobs
	 *
	 * @param account
	 *            the account for which the masterjobs are needed, or <code>null</code> for all active masterjobs, of everybody
	 * @return the list of active masterjobs for this account
	 */
	public static List<Job> getMasterjobs(final String account) {
		return getMasterjobs(account, false);
	}

	/**
	 * Get the list of active masterjobs
	 *
	 * @param account
	 *            the account for which the masterjobs are needed, or <code>null</code> for all active masterjobs, of everybody
	 * @param loadJDL
	 * @return the list of active masterjobs for this account
	 */
	public static List<Job> getMasterjobs(final String account, final boolean loadJDL) {
		return getMasterjobs(account, loadJDL, 0);
	}

	/**
	 * Get the list of active masterjobs
	 *
	 * @param account
	 *            the account for which the masterjobs are needed, or <code>null</code> for all active masterjobs, of everybody
	 * @param loadJDL
	 * @param archiveYear
	 *            queue archive year to query instead of the main queue
	 * @return the list of active masterjobs for this account
	 */
	public static List<Job> getMasterjobs(final String account, final boolean loadJDL, final int archiveYear) {
		final List<Job> ret = new ArrayList<>();

		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return null;

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_lookup");
				monitor.incrementCounter("TQ_getmasterjobs");
			}

			String q;

			if (dbStructure2_20) {
				if (archiveYear < 2000) {
					if (loadJDL)
						q = "SELECT QUEUE.*,origJdl as JDL FROM QUEUE INNER JOIN QUEUEJDL using(queueId) ";
					else
						q = "SELECT * FROM QUEUE ";
				} else if (loadJDL)
					q = "SELECT *,origJdl as JDL FROM QUEUEARCHIVE" + archiveYear + " ";
				else
					q = "SELECT * FROM QUEUEARCHIVE" + archiveYear + " ";

				q += "WHERE split=0 AND statusId!=" + JobStatus.KILLED.getAliEnLevel() + " ";
			} else if (archiveYear < 2000)
				q = "SELECT " + (loadJDL ? "*" : ALL_BUT_JDL) + " FROM QUEUE WHERE split=0 AND status!='KILLED' ";
			else
				q = "SELECT " + (loadJDL ? "*" : ALL_BUT_JDL) + " FROM QUEUEARCHIVE" + archiveYear + " WHERE split=0 AND status!='KILLED' ";

			if (account != null && account.length() > 0)
				if (dbStructure2_20)
					q += "AND userId=" + getUserId(account);
				else
					q += "AND submitHost LIKE '" + Format.escSQL(account) + "@%'";

			q += " AND received>UNIX_TIMESTAMP(now())-60*60*24*14 ORDER BY queueId ASC;";

			final long lQueryStart = System.currentTimeMillis();

			db.setReadOnly(true);

			db.query(q);

			if (monitor != null)
				monitor.addMeasurement("TQ_getmasterjobs_time", (System.currentTimeMillis() - lQueryStart) / 1000d);

			while (db.moveNext())
				ret.add(new Job(db, loadJDL));
		}

		return ret;
	}

	/**
	 * @param initial
	 * @param maxAge
	 *            the age in milliseconds
	 * @return the jobs that are active or have finished since at most maxAge
	 */
	public static List<Job> filterMasterjobs(final List<Job> initial, final long maxAge) {
		if (initial == null)
			return null;

		final List<Job> ret = new ArrayList<>(initial.size());

		final long now = System.currentTimeMillis();

		for (final Job j : initial)
			if (j.isActive() || j.mtime == null || (now - j.mtime.getTime() < maxAge))
				ret.add(j);

		return ret;
	}

	/**
	 * @param account
	 * @return the masterjobs for this account and the subjob statistics for them
	 */
	public static Map<Job, Map<JobStatus, Integer>> getMasterjobStats(final String account) {
		return getMasterjobStats(getMasterjobs(account));
	}

	/**
	 * @param jobs
	 * @return the same masterjobs and the respective subjob statistics
	 */
	public static Map<Job, Map<JobStatus, Integer>> getMasterjobStats(final List<Job> jobs) {
		final Map<Job, Map<JobStatus, Integer>> ret = new TreeMap<>();

		if (jobs.size() == 0)
			return ret;

		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return ret;

			final StringBuilder sb = new StringBuilder(jobs.size() * 10);

			final Map<Integer, Job> reverse = new HashMap<>();

			for (final Job j : jobs) {
				if (sb.length() > 0)
					sb.append(',');

				sb.append(j.queueId);

				reverse.put(Integer.valueOf(j.queueId), j);
			}

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_lookup");
				monitor.incrementCounter("TQ_getmasterjob_stats");
			}

			final long lQueryStart = System.currentTimeMillis();

			final String q;

			if (dbStructure2_20)
				q = "select split,statusId,count(1) from QUEUE where split in (" + sb.toString() + ") AND statusId!=" + JobStatus.KILLED.getAliEnLevel() + " group by split,statusId order by 1,2;";
			else
				q = "select split,status,count(1) from QUEUE where split in (" + sb.toString() + ") AND status!='KILLED' group by split,status order by 1,2;";

			db.setReadOnly(true);

			db.query(q);

			if (monitor != null)
				monitor.addMeasurement("TQ_getmasterjob_stats_time", (System.currentTimeMillis() - lQueryStart) / 1000d);

			Map<JobStatus, Integer> m = null;
			int oldJobID = -1;

			while (db.moveNext()) {
				final int j = db.geti(1);

				if (j != oldJobID || m == null) {
					m = new HashMap<>();

					final Integer jobId = Integer.valueOf(j);
					ret.put(reverse.get(jobId), m);
					reverse.remove(jobId);

					oldJobID = j;
				}

				JobStatus status;

				if (dbStructure2_20)
					status = JobStatus.getStatusByAlien(Integer.valueOf(db.geti(2)));
				else
					status = JobStatus.getStatus(db.gets(2));

				m.put(status, Integer.valueOf(db.geti(3)));
			}

			// now, what is left, something that doesn't have subjobs ?
			for (final Job j : reverse.values()) {
				m = new HashMap<>(1);
				m.put(j.status(), Integer.valueOf(1));
				ret.put(j, m);
			}

			return ret;
		}
	}

	/**
	 * Get the subjobs of this masterjob
	 *
	 * @param queueId
	 * @return the subjobs, if any
	 */
	public static List<Job> getSubjobs(final int queueId) {
		return getSubjobs(queueId, false);
	}

	/**
	 * Get the subjobs of this masterjob
	 *
	 * @param queueId
	 * @param loadJDL
	 * @return the subjobs, if any
	 */
	public static List<Job> getSubjobs(final int queueId, final boolean loadJDL) {
		return getSubjobs(queueId, loadJDL, 0);
	}

	/**
	 * Get the subjobs of this masterjob
	 *
	 * @param queueId
	 * @param loadJDL
	 * @param archiveYear
	 *            archive year to query instead of the main queue table
	 * @return the subjobs, if any
	 */
	public static List<Job> getSubjobs(final int queueId, final boolean loadJDL, final int archiveYear) {
		final List<Job> ret = new ArrayList<>();

		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return null;

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_lookup");
				monitor.incrementCounter("TQ_getsubjobs");
			}

			String q;

			if (dbStructure2_20) {
				if (archiveYear < 2000) {
					if (loadJDL)
						q = "SELECT QUEUE.*,origJdl AS jdl FROM QUEUE INNER JOIN QUEUEJDL using(queueId)";
					else
						q = "SELECT * FROM QUEUE";
				} else if (loadJDL)
					q = "SELECT *,origJdl AS jdl FROM QUEUEARCHIVE" + archiveYear;
				else
					q = "SELECT * FROM QUEUEARCHIVE" + archiveYear;

				q += " WHERE split=? AND statusId!=" + JobStatus.KILLED.getAliEnLevel() + " ORDER BY queueId ASC";
			} else if (archiveYear < 2000)
				q = "SELECT " + (loadJDL ? "*" : ALL_BUT_JDL) + " FROM QUEUE WHERE split=? AND status!='KILLED' ORDER BY queueId ASC;";
			else
				q = "SELECT " + (loadJDL ? "*" : ALL_BUT_JDL) + " FROM QUEUEARCHIVE" + archiveYear + " WHERE split=? AND status!='KILLED' ORDER BY queueId ASC;";

			final long lQueryStart = System.currentTimeMillis();

			db.setReadOnly(true);

			db.query(q, false, Integer.valueOf(queueId));

			if (monitor != null)
				monitor.addMeasurement("TQ_getsubjobs_time", (System.currentTimeMillis() - lQueryStart) / 1000d);

			while (db.moveNext())
				ret.add(new Job(db, loadJDL));
		}

		return ret;
	}

	/**
	 * Get the subjob status of this masterjob
	 *
	 * @param queueId
	 * @param status
	 * @param id
	 * @param site
	 * @param bPrintId
	 * @param bPrintSite
	 * @param bMerge
	 * @param bKill
	 * @param bResubmit
	 * @param bExpunge
	 * @param limit
	 * @return the subjobs, if any
	 */
	public static List<Job> getMasterJobStat(final int queueId, final Set<JobStatus> status, final List<Integer> id, final List<String> site, final boolean bPrintId, final boolean bPrintSite,
			final boolean bMerge, final boolean bKill, final boolean bResubmit, final boolean bExpunge, final int limit) {

		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return null;

			if (monitor != null)
				monitor.incrementCounter("TQ_db_lookup");

			String where = "";

			if (queueId > 0)
				where = " split=" + queueId + " and ";
			else
				return null;

			if (status != null && status.size() > 0 && !status.contains(JobStatus.ANY)) {
				final StringBuilder whe = new StringBuilder(" ( statusId in (");

				boolean first = true;

				for (final JobStatus s : status) {
					if (!first)
						whe.append(',');
					else
						first = false;

					if (dbStructure2_20)
						whe.append(s.getAliEnLevel());
					else
						whe.append('\'').append(s.toSQL()).append('\'');
				}

				where += whe + ") ) and ";
			}

			if (id != null && id.size() > 0) {
				final StringBuilder whe = new StringBuilder(" ( queueId in (");

				boolean first = true;

				for (final int i : id) {
					if (!first)
						whe.append(',');
					else
						first = false;

					whe.append(i);
				}

				where += whe + ") ) and ";
			}

			if (site != null && site.size() > 0) {
				final StringBuilder whe = new StringBuilder(" ( ");

				boolean first = true;

				for (final String s : site) {
					if (!first)
						whe.append(" or ");
					else
						first = false;

					if (dbStructure2_20)
						whe.append("ifnull(substring(exechost,POSITION('\\@' in exechost)+1),'')='").append(Format.escSQL(s)).append('\'');
					else
						whe.append("execHostId=").append(getHostId(s));
				}

				where += whe.substring(0, whe.length() - 3) + " ) and ";
			}

			if (dbStructure2_20)
				where += " statusId!=" + JobStatus.KILLED.getAliEnLevel();
			else
				where += " status!='KILLED' ";

			int lim = 20000;
			if (limit > 0 && limit < 20000)
				lim = limit;

			final String q;

			if (dbStructure2_20)
				q = "SELECT queueId,statusId,split,execHostId FROM QUEUE WHERE " + where + " ORDER BY queueId ASC limit " + lim + ";";
			else
				q = "SELECT queueId,status,split,execHost FROM QUEUE WHERE " + where + " ORDER BY queueId ASC limit " + lim + ";";

			db.setReadOnly(true);

			if (!db.query(q))
				return null;

			final List<Job> ret = new ArrayList<>();

			while (db.moveNext())
				ret.add(new Job(db, false));

			return ret;
		}
	}

	/**
	 * @param jobs
	 * @return the jobs grouped by their state
	 */
	public static Map<JobStatus, List<Job>> groupByStates(final List<Job> jobs) {
		if (jobs == null)
			return null;

		final Map<JobStatus, List<Job>> ret = new TreeMap<>();

		if (jobs.size() == 0)
			return ret;

		for (final Job j : jobs) {
			List<Job> l = ret.get(j.status());

			if (l == null) {
				l = new ArrayList<>();
				ret.put(j.status(), l);
			}

			l.add(j);
		}

		return ret;
	}

	/**
	 * @param queueId
	 * @return trace log
	 */
	public static String getJobTraceLog(final int queueId) {
		final JobTraceLog trace = new JobTraceLog(queueId);
		return trace.getTraceLog();
	}

	/**
	 * @param job
	 * @param newStatus
	 * @return <code>true</code> if the job status was changed
	 */
	public static boolean setJobStatus(final int job, final JobStatus newStatus) {
		return setJobStatus(job, newStatus, null, null);
	}

	/**
	 * @param job
	 * @param newStatus
	 * @param oldStatusConstraint
	 *            change the status only if the job is still in this state. Can be <code>null</code> to disable checking the current status.
	 * @return <code>true</code> if the job status was changed
	 */
	public static boolean setJobStatus(final int job, final JobStatus newStatus, final JobStatus oldStatusConstraint) {
		return setJobStatus(job, newStatus, oldStatusConstraint, null);
	}

	/**
	 * @param job
	 * @param newStatus
	 * @param oldStatusConstraint
	 *            change the status only if the job is still in this state. Can be <code>null</code> to disable checking the current status.
	 * @param extrafields
	 *            other fields to set at the same time
	 * @return <code>true</code> if the job status was changed
	 */
	public static boolean setJobStatus(final int job, final JobStatus newStatus, final JobStatus oldStatusConstraint, final HashMap<String, Object> extrafields) {
		if (job <= 0)
			throw new IllegalArgumentException("Job ID " + job + " is illegal");

		if (newStatus == null)
			throw new IllegalArgumentException("The new status code cannot be null");

		try (DBFunctions db = getQueueDB()) {
			if (db == null) {
				logger.log(Level.SEVERE, "Cannot get the queue database entry");

				return false;
			}

			String q;

			if (dbStructure2_20)
				q = "SELECT statusId FROM QUEUE where queueId=?;";
			else
				q = "SELECT status FROM QUEUE where queueId=?;";

			db.setReadOnly(true);

			if (!db.query(q, false, Integer.valueOf(job))) {
				logger.log(Level.SEVERE, "Error executing the select query from QUEUE");

				return false;
			}

			if (!db.moveNext()) {
				logger.log(Level.FINE, "Could not find queueId " + job + " in the queue");

				return false;
			}

			db.setReadOnly(false);

			JobStatus oldStatus;

			if (dbStructure2_20)
				oldStatus = JobStatus.getStatusByAlien(Integer.valueOf(db.geti(1)));
			else
				oldStatus = JobStatus.getStatus(db.gets(1));

			if (oldStatus == null) {
				logger.log(Level.WARNING, "Cannot get the status string from " + db.gets(1));
				return false;
			}

			if (oldStatusConstraint != null && oldStatus != oldStatusConstraint) {
				if (logger.isLoggable(Level.FINE))
					logger.log(Level.FINE,
							"Refusing to do the update of " + job + " to state " + newStatus.name() + " because old status is not " + oldStatusConstraint.name() + " but " + oldStatus.name());

				return false;
			}

			Object newstatus;

			if (dbStructure2_20) {
				newstatus = Integer.valueOf(newStatus.getAliEnLevel());
				q = "UPDATE QUEUE SET statusId=? WHERE queueId=?;";
			} else {
				newstatus = newStatus.toSQL();
				q = "UPDATE QUEUE SET status=? WHERE queueId=?;";
			}

			if (!db.query(q, false, newstatus, Integer.valueOf(job)))
				return false;

			final boolean updated = db.getUpdateCount() != 0;

			putJobLog(job, "state", "Job state transition from " + oldStatus.name() + " to " + newStatus.name(), null);

			if (JobStatus.finalStates().contains(newStatus) || newStatus == JobStatus.SAVED_WARN || newStatus == JobStatus.SAVED)
				deleteJobToken(job);

			if (extrafields != null) {
				logger.log(Level.INFO, "extrafields: " + extrafields.toString());
				for (String key : extrafields.keySet()) {
					if (fieldMap.containsKey(key + "_table")) {
						HashMap<String, Object> map = new HashMap<>();
						map.put(fieldMap.get(key + "_field"), extrafields.get(key));
						String query = DBFunctions.composeUpdate(fieldMap.get(key + "_table"), map, null);
						query += " where queueId = ?";
						db.query(query, false, Integer.valueOf(job));
					}
				}
			}

			return updated;
		}
	}

	/**
	 * @param queueId
	 * @return the JDL of this subjobID, if known, <code>null</code> otherwise
	 */
	public static String getJDL(final int queueId) {
		return getJDL(queueId, true);
	}

	/**
	 * @param queueId
	 * @param originalJDL
	 *            if <code>true</code> then the original JDL will be returned, otherwise the processed JDL. Only possible for AliEn v2.20+, for older versions the only known JDL is returned.
	 * @return the JDL of this subjobID, if known, <code>null</code> otherwise
	 */
	@SuppressWarnings("deprecation")
	public static String getJDL(final int queueId, final boolean originalJDL) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return null;

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_lookup");
				monitor.incrementCounter("TQ_get_jdl");
			}

			String q;

			if (dbStructure2_20)
				q = "SELECT origJdl" + (originalJDL ? "" : ",resultsJdl") + " FROM QUEUEJDL WHERE queueId=?;";
			else
				q = "SELECT jdl FROM QUEUE WHERE queueId=?;";

			db.setReadOnly(true);

			if (!db.query(q, false, Integer.valueOf(queueId)) || !db.moveNext()) {
				final Date d = new Date();
				q = "SELECT origJdl" + (originalJDL ? "" : ",resultsJdl") + " as JDL FROM QUEUEARCHIVE" + (1900 + d.getYear()) + " WHERE queueId=?";

				if (!db.query(q, false, Integer.valueOf(queueId)) || !db.moveNext()) {
					final String jdlArchiveDir = ConfigUtils.getConfig().gets("alien.taskQueue.TaskQueueUtils.jdlArchiveDir");

					if (jdlArchiveDir.length() > 0) {
						File f = new File(jdlArchiveDir, queueId + ".txt");

						if (f.exists() && f.canRead()) {
							String content = Utils.readFile(f.getAbsolutePath());

							final int idx = content.indexOf("// --------");

							if (idx >= 0)
								content = content.substring(0, idx);

							return content;
						}

						f = new File(jdlArchiveDir, queueId + ".html");

						String content = null;

						if (f.exists() && f.canRead())
							content = Utils.readFile(f.getAbsolutePath());
						else {
							f = new File(jdlArchiveDir, (queueId / 10000000) + ".zip");

							if (f.exists() && f.canRead()) {
								final Path zipFile = Paths.get(f.getAbsolutePath());

								try (FileSystem fileSystem = FileSystems.newFileSystem(zipFile, null)) {
									final Path source = fileSystem.getPath(queueId + ".html");

									if (source != null) {
										final ByteArrayOutputStream baos = new ByteArrayOutputStream();

										Files.copy(source, baos);

										content = baos.toString();
									}
								} catch (final IOException e) {
									// ignore
								}
							}
						}

						if (content != null) {
							content = Utils.htmlToText(content);

							final int idx = content.indexOf("// --------");

							if (idx >= 0)
								content = content.substring(0, idx);

							return content;
						}
					}

					logger.log(Level.WARNING, "Could not locate the archived jdl of " + queueId);

					return null;
				}
			}

			if (dbStructure2_20 && !originalJDL) {
				final String jdl = db.gets(2);

				if (jdl.length() > 0)
					return jdl;
			}

			return db.gets(1);
		}
	}

	/**
	 * @param states
	 * @param users
	 * @param sites
	 * @param nodes
	 * @param mjobs
	 * @param jobids
	 * @param orderByKey
	 * @param limit
	 * @return the ps listing
	 */
	public static List<Job> getPS(final Collection<JobStatus> states, final Collection<String> users, final Collection<String> sites, final Collection<String> nodes, final Collection<Integer> mjobs,
			final Collection<Integer> jobids, final String orderByKey, final int limit) {

		final List<Job> ret = new ArrayList<>();

		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return null;

			if (monitor != null)
				monitor.incrementCounter("TQ_db_lookup");

			int lim = 20000;

			if (limit > 0 && limit < 20000)
				lim = limit;

			String where = "";

			if (states != null && states.size() > 0 && !states.contains(JobStatus.ANY)) {
				final StringBuilder whe = new StringBuilder();

				if (dbStructure2_20)
					whe.append(" (statusId in (");
				else
					whe.append(" (status in (");

				boolean first = true;

				for (final JobStatus s : states) {
					if (!first)
						whe.append(",");
					else
						first = false;

					if (dbStructure2_20)
						whe.append(s.getAliEnLevel());
					else
						whe.append('\'').append(s.toSQL()).append('\'');
				}

				where += whe + ") ) and ";
			}

			if (users != null && users.size() > 0 && !users.contains("%")) {
				final StringBuilder whe = new StringBuilder(" ( ");

				boolean first = true;

				for (final String u : users) {
					if (!first)
						whe.append(" or ");
					else
						first = false;

					if (dbStructure2_20)
						whe.append("userId=").append(getUserId(u));
					else
						whe.append("submitHost like '").append(Format.escSQL(u)).append("@%'");
				}

				where += whe + " ) and ";
			}

			if (sites != null && sites.size() > 0 && !sites.contains("%")) {
				final StringBuilder whe = new StringBuilder(" ( site in (");

				boolean first = true;

				for (final String s : sites) {
					if (!first)
						whe.append(',');
					else
						first = false;

					whe.append('\'').append(Format.escSQL(s)).append('\'');
				}

				where += whe + ") ) and ";
			}

			if (nodes != null && nodes.size() > 0 && !nodes.contains("%")) {
				final StringBuilder whe = new StringBuilder(" ( node in (");

				boolean first = true;

				for (final String n : nodes) {
					if (!first)
						whe.append(',');
					else
						first = false;

					whe.append('\'').append(Format.escSQL(n)).append('\'');
				}

				where += whe + ") ) and ";
			}

			if (mjobs != null && mjobs.size() > 0 && !mjobs.contains(Integer.valueOf(0))) {
				final StringBuilder whe = new StringBuilder(" ( split in (");

				boolean first = true;

				for (final Integer m : mjobs) {
					if (!first)
						whe.append(',');
					else
						first = false;

					whe.append(m);
				}

				where += whe + ") ) and ";
			}

			if (jobids != null && jobids.size() > 0 && !jobids.contains(Integer.valueOf(0))) {
				final StringBuilder whe = new StringBuilder(" ( queueId in (");

				boolean first = true;

				for (final Integer i : jobids) {
					if (!first)
						whe.append(',');
					else
						first = false;

					whe.append(i);
				}

				where += whe + ") ) and ";
			}

			if (where.endsWith(" and "))
				where = where.substring(0, where.length() - 5);

			String orderBy = " order by ";

			if (orderByKey == null || orderByKey.length() == 0)
				orderBy += " queueId asc ";
			else
				orderBy += orderByKey + " asc ";

			if (where.length() > 0)
				where = " WHERE " + where;

			final String q;

			if (dbStructure2_20)
				q = "SELECT * FROM QUEUE " + where + orderBy + " limit " + lim + ";";
			else
				q = "SELECT " + ALL_BUT_JDL + " FROM QUEUE " + where + orderBy + " limit " + lim + ";";

			// System.out.println("SQL: " + q);

			db.setReadOnly(true);

			if (!db.query(q))
				return null;

			while (db.moveNext()) {
				final Job j = new Job(db, false);
				ret.add(j);
			}
		}

		return ret;
	}

	/**
	 * @return matching jobs histograms
	 */
	public static Map<Integer, Integer> getMatchingHistogram() {
		final Map<Integer, Integer> ret = new TreeMap<>();

		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return ret;

			final Map<Integer, AtomicInteger> work = new HashMap<>();

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_lookup");
				monitor.incrementCounter("TQ_matching_histogram");
			}

			db.setReadOnly(true);

			db.query("select site,sum(counter) from JOBAGENT where counter>0 group by site");

			while (db.moveNext()) {
				final String site = db.gets(1);

				int key = 0;

				final StringTokenizer st = new StringTokenizer(site, ",; ");

				while (st.hasMoreTokens())
					if (st.nextToken().length() > 0)
						key++;

				final int value = db.geti(2);

				final Integer iKey = Integer.valueOf(key);

				final AtomicInteger ai = work.get(iKey);

				if (ai == null)
					work.put(iKey, new AtomicInteger(value));
				else
					ai.addAndGet(value);
			}

			for (final Map.Entry<Integer, AtomicInteger> entry : work.entrySet())
				ret.put(entry.getKey(), Integer.valueOf(entry.getValue().intValue()));

			return ret;
		}
	}

	/**
	 * @return the number of matching waiting jobs for each site
	 */
	public static Map<String, Integer> getMatchingJobsSummary() {
		final Map<String, Integer> ret = new TreeMap<>();

		int addToAll = 0;

		final Map<String, AtomicInteger> work = new HashMap<>();

		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return ret;

			db.setReadOnly(true);

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_lookup");
				monitor.incrementCounter("TQ_matching_jobs_summary");
			}

			db.query("select site,sum(counter) from JOBAGENT where counter>0 group by site order by site;");

			while (db.moveNext()) {
				final String sites = db.gets(1);
				final int count = db.geti(2);

				if (sites.length() == 0)
					addToAll = count;
				else {
					final StringTokenizer st = new StringTokenizer(sites, ",; ");

					while (st.hasMoreTokens()) {
						final String site = st.nextToken().trim();

						final AtomicInteger ai = work.get(site);

						if (ai == null)
							work.put(site, new AtomicInteger(count));
						else
							ai.addAndGet(count);
					}
				}
			}
		}

		for (final Map.Entry<String, AtomicInteger> entry : work.entrySet())
			ret.put(entry.getKey(), Integer.valueOf(entry.getValue().intValue() + addToAll));

		if (addToAll > 0) {
			final Set<String> sites = LDAPHelper.checkLdapInformation("(objectClass=organizationalUnit)", "ou=Sites,", "ou", false);

			if (sites != null)
				for (final String site : sites)
					if (!ret.containsKey(site))
						ret.put(site, Integer.valueOf(addToAll));
		}

		return ret;
	}

	/**
	 * Kill a job in the queue
	 *
	 * @param user
	 * @param queueId
	 * @return status of the kill
	 */
	public static boolean kill(final AliEnPrincipal user, final int queueId) {
		// TODO check if the user is allowed to kill and do it
		return false;
	}

	private static final Pattern p = Pattern.compile("\\$(\\d+)");

	/**
	 * @param jdlArguments
	 * @return array of arguments to apply to the JDL
	 * @see #applyJDLArguments(String, AliEnPrincipal, String, String...)
	 * @see #submit(LFN, AliEnPrincipal, String, String...)
	 */
	public static String[] splitArguments(final String jdlArguments) {
		final StringTokenizer st = new StringTokenizer(jdlArguments);

		final List<String> split = new LinkedList<>();

		while (st.hasMoreTokens()) {
			final String tok = st.nextToken();

			if (tok.length() > 0)
				split.add(tok);
		}

		return split.toArray(new String[0]);
	}

	/**
	 * @param jdlContents
	 *            JDL specification
	 * @param account
	 * @param role
	 * @param arguments
	 *            arguments to the JDL, should be at least as many as the largest $N that shows up in the JDL
	 * @return the parsed JDL, with all $N parameters replaced with the respective argument
	 * @throws IOException
	 *             if there is any problem parsing the JDL content
	 */
	public static JDL applyJDLArguments(final String jdlContents, final AliEnPrincipal account, final String role, final String... arguments) throws IOException {
		if (jdlContents == null)
			return null;

		String jdlToSubmit = JDL.removeComments(jdlContents);

		Matcher m = p.matcher(jdlToSubmit);

		while (m.find()) {
			final String s = m.group(1);

			final int i = Integer.parseInt(s);

			if (arguments == null || arguments.length < i)
				throw new IOException("The JDL indicates argument $" + i + " but you haven't provided it");

			jdlToSubmit = jdlToSubmit.replaceAll("\\$" + i + "(?!\\d)", arguments[i - 1]);

			m = p.matcher(jdlToSubmit);
		}

		final JDL jdl = new JDL(jdlToSubmit);

		if (arguments != null && arguments.length > 0) {
			final StringBuilder sb = new StringBuilder();

			for (final String s : arguments) {
				if (sb.length() > 0)
					sb.append(' ');

				sb.append(s);
			}

			jdl.set("JDLArguments", sb.toString());
		}

		final String executable = jdl.getExecutable();

		if (executable == null)
			throw new IOException("The JDL has to indicate an Executable");

		boolean found = false;

		try {
			final List<String> options = new LinkedList<>();
			options.add(executable);

			if (!executable.startsWith("/")) {
				options.add("/bin/" + executable);
				options.add("/alice/bin/" + executable);
				options.add("/panda/bin/" + executable);

				if (role != null && !account.getName().equals(role))
					options.add(UsersHelper.getHomeDir(role) + "bin/" + executable);

				options.add(UsersHelper.getHomeDir(account.getName()) + "bin/" + executable);
			}

			final LFNfromString answer = Dispatcher.execute(new LFNfromString(account, role, true, false, options));

			final List<LFN> lfns = answer.getLFNs();

			if (lfns != null)
				for (final LFN l : lfns)
					if (l.isFile()) {
						found = true;
						jdl.set("Executable", l.getCanonicalName());
					}
		} catch (final ServerException se) {
			throw new IOException(se.getMessage(), se);
		}

		if (!found)
			throw new IOException("The Executable name you indicated (" + executable + ") cannot be located in any standard PATH");

		return jdl;
	}

	/**
	 * Submit the JDL indicated by this file
	 *
	 * @param file
	 *            the catalogue name of the JDL to be submitted
	 * @param account
	 *            account from where the submit command was received
	 * @param role
	 * @param arguments
	 *            arguments to the JDL, should be at least as many as the largest $N that shows up in the JDL
	 * @return the job ID
	 * @throws IOException
	 *             in case of problems like downloading the respective JDL or not enough arguments provided to it
	 */
	public static int submit(final LFN file, final AliEnPrincipal account, final String role, final String... arguments) throws IOException {
		if (file == null || !file.exists || !file.isFile())
			throw new IllegalArgumentException("The LFN is not a valid file");

		final String jdlContents = IOUtils.getContents(file);

		if (jdlContents == null || jdlContents.length() == 0)
			throw new IOException("Could not download " + file.getCanonicalName());

		final JDL jdl = applyJDLArguments(jdlContents, account, role, arguments);

		jdl.set("JDLPath", file.getCanonicalName());

		return submit(jdl, account, role);
	}

	private static void prepareJDLForSubmission(final JDL jdl, final String owner) throws IOException {
		Float price = jdl.getFloat("Price");

		if (price == null)
			price = Float.valueOf(1);

		jdl.set("Price", price);

		Integer ttl = jdl.getInteger("TTL");

		if (ttl == null || ttl.intValue() <= 0)
			ttl = Integer.valueOf(21600);

		jdl.set("TTL", ttl);

		jdl.set("Type", "Job");

		if (jdl.get("OrigRequirements") == null)
			jdl.set("OrigRequirements", jdl.get("Requirements"));

		if (jdl.get("MemorySize") == null)
			jdl.set("MemorySize", "8GB");

		jdl.set("User", owner);

		// set the requirements anew
		jdl.delete("Requirements");

		jdl.addRequirement("other.Type == \"machine\"");

		final Collection<String> packages = jdl.getList("Packages");

		if (packages != null)
			for (final String pack : packages)
				jdl.addRequirement("member(other.Packages,\"" + pack + "\")");

		jdl.addRequirement(jdl.gets("OrigRequirements"));

		jdl.addRequirement("other.TTL > " + ttl);
		jdl.addRequirement("other.Price <= " + price.intValue());

		final Collection<String> inputFiles = jdl.getList("InputFile");

		if (inputFiles != null)
			for (final String file : inputFiles) {
				if (file.indexOf('/') < 0)
					throw new IOException("InputFile contains an illegal entry: " + file);

				String lfn = file;

				if (lfn.startsWith("LF:"))
					lfn = lfn.substring(3);
				else
					throw new IOException("InputFile doesn't start with 'LF:' : " + lfn);

				final LFN l = LFNUtils.getLFN(lfn);

				if (l == null || !l.isFile())
					throw new IOException("InputFile " + lfn + " doesn't exist in the catalogue");
			}

		final Collection<String> inputData = jdl.getList("InputData");

		if (inputData != null)
			for (final String file : inputData) {
				if (file.indexOf('/') < 0)
					throw new IOException("InputData contains an illegal entry: " + file);

				String lfn = file;

				if (lfn.startsWith("LF:"))
					lfn = lfn.substring(3);
				else
					throw new IOException("InputData doesn't start with 'LF:' : " + lfn);

				if (lfn.indexOf(',') >= 0)
					lfn = lfn.substring(0, lfn.indexOf(',')); // "...,nodownload"
																// for example

				final LFN l = LFNUtils.getLFN(lfn);

				if (l == null || !l.isFile())
					throw new IOException("InputData " + lfn + " doesn't exist in the catalogue");
			}

		// sanity check of other tags

		for (final String tag : Arrays.asList("Executable", "ValidationCommand", "InputDataCollection")) {
			final Collection<String> files = jdl.getList(tag);

			if (files == null) {
				if (tag.equals("Executable"))
					throw new IOException("Your JDL lacks an Executable tag");

				continue;
			}

			for (final String file : files) {
				String fileName = file;

				if (fileName.startsWith("LF:"))
					fileName = fileName.substring(3);

				if (fileName.indexOf(',') >= 0)
					fileName = fileName.substring(0, fileName.indexOf(','));

				final LFN l = LFNUtils.getLFN(fileName);

				if (l == null || (!l.isFile() && !l.isCollection()))
					throw new IOException(tag + " tag required " + fileName + " which is not valid: " + (l == null ? "not in the catalogue" : "not a file or collection"));
			}
		}
	}

	/**
	 * Submit this JDL body
	 *
	 * @param j
	 *            job description, in plain text
	 * @param account
	 *            account from where the submit command was received
	 * @param role
	 * @return the job ID
	 * @throws IOException
	 *             in case of problems such as the number of provided arguments is not enough
	 * @see #applyJDLArguments(String, AliEnPrincipal, String, String...)
	 */
	public static int submit(final JDL j, final AliEnPrincipal account, final String role) throws IOException {
		final String owner = prepareSubmission(j, account, role);

		return insertJob(j, account, owner, null);
	}

	/**
	 * Check the validity of the JDL (package versions, existing critical input files etc) and if the indicated account has access to the indicated role. Will also decorate the JDL with various helper
	 * tags.
	 * 
	 * @param j
	 *            JDL to submit
	 * @param account
	 *            AliEn account that requests the submission
	 * @param role
	 *            one of the role names to which the account has access to
	 * @return the AliEn account name that will own the job
	 * @throws IOException
	 */
	public static String prepareSubmission(final JDL j, final AliEnPrincipal account, final String role) throws IOException {
		// TODO : check this account's quota before submitting

		final String packageMessage = PackageUtils.checkPackageRequirements(j);

		if (packageMessage != null)
			throw new IOException(packageMessage);

		final String owner = role != null && (account.hasRole(role) || account.canBecome(role)) ? role : account.getName();

		prepareJDLForSubmission(j, owner);

		return owner;
	}

	/**
	 * Insert a job in the given status. N
	 *
	 * @param j
	 *            full JDL
	 * @param account
	 *            AliEn account
	 * @param owner
	 *            AliEn account name that the indicated account has access to
	 * @param targetStatus
	 *            job status. Can be <code>null</code> to have the default behavior of putting it to <code>INSERTING</code> and letting AliEn process it.
	 * @return the just inserted job ID
	 * @throws IOException
	 */
	public static int insertJob(final JDL j, final AliEnPrincipal account, final String owner, final JobStatus targetStatus) throws IOException {
		final String clientAddress;

		final InetAddress addr = account.getRemoteEndpoint();

		if (addr != null)
			clientAddress = addr.getCanonicalHostName();
		else
			clientAddress = MonitorFactory.getSelfHostname();

		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				throw new IOException("This service has no direct database connection");

			final Map<String, Object> values = new HashMap<>();

			final String executable = j.getExecutable();

			final Float price = j.getFloat("Price");

			values.put("priority", Integer.valueOf(0));

			final String notify = j.gets("email");

			final JobStatus jobStatus = targetStatus != null ? targetStatus : JobStatus.INSERTING;

			if (dbStructure2_20) {
				values.put("statusId", Integer.valueOf(jobStatus.getAliEnLevel()));
				values.put("userId", getUserId(owner));
				values.put("submitHostId", getHostId(clientAddress));
				values.put("commandId", getCommandId(executable));

				if (notify != null && notify.length() > 0)
					values.put("notifyId", getNotifyId(notify));
			} else {
				values.put("status", jobStatus.toSQL());
				values.put("jdl", "\n    [\n" + j.toString() + "\n    ]");
				values.put("submitHost", owner + "@" + clientAddress);
				values.put("notify", notify);
				values.put("name", executable);
			}

			values.put("chargeStatus", Integer.valueOf(0));
			values.put("price", price);
			values.put("received", Long.valueOf(System.currentTimeMillis() / 1000));

			Integer masterjobID = j.getInteger("MasterJobID");

			if (jobStatus.equals(JobStatus.SPLIT) || j.get("Split") != null) {
				values.put("masterjob", Integer.valueOf(1));
				masterjobID = null;
			} else
				values.put("masterjob", Integer.valueOf(0));

			if (masterjobID != null)
				values.put("split", masterjobID);
			else
				values.put("split", Integer.valueOf(0));

			final String insert = DBFunctions.composeInsert("QUEUE", values);

			db.setLastGeneratedKey(true);

			if (!db.query(insert))
				throw new IOException("Could not insert the job in the queue");

			final Integer pid = db.getLastGeneratedKey();

			if (pid == null)
				throw new IOException("Last generated key is unknown");

			db.query("INSERT INTO QUEUEPROC (queueId) VALUES (?);", false, pid);

			if (dbStructure2_20) {
				final Map<String, Object> valuesJDL = new HashMap<>();

				valuesJDL.put("queueId", pid);
				valuesJDL.put("origJdl", "\n    [\n" + j.toString() + "\n    ]");

				final String insertJDL = DBFunctions.composeInsert("QUEUEJDL", valuesJDL);

				db.query(insertJDL);
			}

			// the JobBroker will insert a token when it dispatches the job to
			// somebody, don't insert anything at this stage
			// insertJobToken(pid.intValue(), owner, true);

			setAction(jobStatus);

			putJobLog(pid.intValue(), "state", "Job state transition to " + jobStatus.toString(), null);

			return pid.intValue();
		}
	}

	private static final GenericLastValuesCache<String, Integer> userIdCache = new GenericLastValuesCache<String, Integer>() {
		private static final long serialVersionUID = 1L;

		@Override
		protected Integer resolve(final String key) {
			try (DBFunctions db = getQueueDB()) {
				if (db == null)
					return null;

				db.setReadOnly(true);

				db.query("SELECT userId FROM QUEUE_USER where user=?;", false, key);

				db.setReadOnly(false);

				if (!db.moveNext()) {
					db.setLastGeneratedKey(true);

					final Set<String> ids = LDAPHelper.checkLdapInformation("uid=" + key, "ou=People,", "CCID");

					int id = 0;

					if (ids != null && ids.size() > 0)
						for (final String s : ids)
							try {
								id = Integer.parseInt(s);
								break;
							} catch (final Throwable t) {
								// ignore
							}

					if (id > 0) {
						if (db.query("INSERT INTO QUEUE_USER (userId, user) VALUES (" + id + ", '" + Format.escSQL(key) + "');", true))
							return Integer.valueOf(id);

						// did it fail because the user was inserted by somebody
						// else?
						db.query("SELECT userId FROM QUEUE_USER where user=?;", false, key);

						if (db.moveNext())
							return Integer.valueOf(db.geti(1));

						// if it gets here it means there is a duplicate CCID in
						// LDAP

						logger.log(Level.WARNING, "Duplicate CCID " + id + " in LDAP, failed to correctly insert user " + key
								+ " because of it. Will generate a new userid for this guy, but the consistency with LDAP is lost now!");
					}

					if (db.query("INSERT INTO QUEUE_USER (user) VALUES (?);", true, key))
						return db.getLastGeneratedKey();

					// somebody probably has inserted the same entry concurrently
					db.query("SELECT userId FROM QUEUE_USER where user=?", false, key);

					if (db.moveNext())
						return Integer.valueOf(db.geti(1));
				} else
					return Integer.valueOf(db.geti(1));

				return null;
			}
		}
	};

	private static synchronized Integer getUserId(final String owner) {
		if (owner == null || owner.length() == 0)
			return null;

		return userIdCache.get(owner);
	}

	private static final GenericLastValuesCache<String, Integer> commandIdCache = new GenericLastValuesCache<String, Integer>() {
		private static final long serialVersionUID = 1L;

		@Override
		protected Integer resolve(final String key) {
			try (DBFunctions db = getQueueDB()) {
				if (db == null)
					return null;

				db.setReadOnly(true);

				db.query("SELECT commandId FROM QUEUE_COMMAND where command=?;", false, key);

				db.setReadOnly(false);

				if (!db.moveNext()) {
					db.setLastGeneratedKey(true);

					if (db.query("INSERT INTO QUEUE_COMMAND (command) VALUES (?);", true, key))
						return db.getLastGeneratedKey();

					// somebody probably has inserted the same entry
					// concurrently
					db.query("SELECT commandId FROM QUEUE_COMMAND where command=?;", false, key);

					if (db.moveNext())
						return Integer.valueOf(db.geti(1));
				} else
					return Integer.valueOf(db.geti(1));
			}

			return null;
		}
	};

	private static synchronized Integer getCommandId(final String command) {
		if (command == null || command.length() == 0)
			return null;

		return commandIdCache.get(command);
	}

	private static final GenericLastValuesCache<String, Integer> hostIdCache = new GenericLastValuesCache<String, Integer>() {
		private static final long serialVersionUID = 1L;

		@Override
		protected Integer resolve(final String key) {
			try (DBFunctions db = getQueueDB()) {
				if (db == null)
					return null;

				db.setReadOnly(true);

				db.query("SELECT hostId FROM QUEUE_HOST where host=?;", false, key);

				db.setReadOnly(false);

				if (!db.moveNext()) {
					db.setLastGeneratedKey(true);

					if (db.query("INSERT INTO QUEUE_HOST (host) VALUES (?);", true, key))
						return db.getLastGeneratedKey();

					// somebody probably has inserted the same entry
					// concurrently
					db.query("SELECT hostId FROM QUEUE_HOST where host=?", false, key);

					if (db.moveNext())
						return Integer.valueOf(db.geti(1));
				} else
					return Integer.valueOf(db.geti(1));
			}

			return null;
		}
	};

	private static synchronized Integer getHostId(final String host) {
		if (host == null || host.length() == 0)
			return null;

		return hostIdCache.get(host);
	}

	private static final GenericLastValuesCache<String, Integer> notifyIdCache = new GenericLastValuesCache<String, Integer>() {
		private static final long serialVersionUID = 1L;

		@Override
		protected Integer resolve(final String key) {
			try (DBFunctions db = getQueueDB()) {
				if (db == null)
					return null;

				db.setReadOnly(true);

				db.query("SELECT notifyId FROM QUEUE_NOTIFY where notify=?;", false, key);

				db.setReadOnly(false);

				if (!db.moveNext()) {
					db.setLastGeneratedKey(true);

					if (db.query("INSERT INTO QUEUE_NOTIFY (notify) VALUES (?);", true, key))
						return db.getLastGeneratedKey();

					// somebody probably has inserted the same entry
					// concurrently
					db.query("SELECT notifyId FROM QUEUE_NOTIFY where notify=?;", false, key);

					if (db.moveNext())
						return Integer.valueOf(db.geti(1));
				} else
					return Integer.valueOf(db.geti(1));
			}

			return null;
		}
	};

	private static synchronized Integer getNotifyId(final String notify) {
		if (notify == null || notify.length() == 0)
			return null;

		return notifyIdCache.get(notify);
	}

	private static final GenericLastValuesCache<Integer, String> userCache = new GenericLastValuesCache<Integer, String>() {
		private static final long serialVersionUID = 1L;

		@Override
		protected String resolve(final Integer key) {
			try (DBFunctions db = getQueueDB()) {
				if (db == null)
					return null;

				db.setReadOnly(true);

				db.query("SELECT user FROM QUEUE_USER where userId=?;", false, key);

				if (db.moveNext())
					return StringFactory.get(db.gets(1));

				return null;
			}
		}
	};

	/**
	 * @param userId
	 * @return user name for the respective userId
	 */
	public static String getUser(final int userId) {
		if (userId <= 0)
			return null;

		return userCache.get(Integer.valueOf(userId));
	}

	private static final GenericLastValuesCache<Integer, String> hostCache = new GenericLastValuesCache<Integer, String>() {
		private static final long serialVersionUID = 1L;

		@Override
		protected int getMaximumSize() {
			return 20000;
		}

		@Override
		protected String resolve(final Integer key) {
			try (DBFunctions db = getQueueDB()) {
				if (db == null)
					return null;

				db.setReadOnly(true);

				db.query("SELECT host FROM QUEUE_HOST where hostId=?;", false, key);

				if (db.moveNext())
					return StringFactory.get(db.gets(1));
			}

			return null;
		}
	};

	/**
	 * @param hostId
	 * @return host name for the respective hostId
	 */
	public static String getHost(final int hostId) {
		if (hostId <= 0)
			return null;

		return hostCache.get(Integer.valueOf(hostId));
	}

	private static final GenericLastValuesCache<Integer, String> notifyCache = new GenericLastValuesCache<Integer, String>() {
		private static final long serialVersionUID = 1L;

		@Override
		protected String resolve(final Integer key) {
			try (DBFunctions db = getQueueDB()) {
				if (db == null)
					return null;

				db.setReadOnly(true);

				db.query("SELECT notify FROM QUEUE_NOTIFY where notifyId=?;", false, key);

				if (db.moveNext())
					return StringFactory.get(db.gets(1));
			}

			return null;
		}
	};

	/**
	 * @param notifyId
	 * @return notification string (email address) for the respective notifyId
	 */
	public static String getNotify(final int notifyId) {
		if (notifyId <= 0)
			return null;

		return notifyCache.get(Integer.valueOf(notifyId));
	}

	private static final GenericLastValuesCache<Integer, String> commandCache = new GenericLastValuesCache<Integer, String>() {
		private static final long serialVersionUID = 1L;

		@Override
		protected String resolve(final Integer key) {
			try (DBFunctions db = getQueueDB()) {
				if (db == null)
					return null;

				db.setReadOnly(true);

				db.query("SELECT command FROM QUEUE_COMMAND where commandId=?", false, key);

				if (db.moveNext())
					return StringFactory.get(db.gets(1));
			}

			return null;
		}
	};

	/**
	 * @param commandId
	 * @return the command corresponding to the id
	 */
	public static String getCommand(final int commandId) {
		if (commandId <= 0)
			return null;

		return commandCache.get(Integer.valueOf(commandId));
	}

	/**
	 * @param user
	 * @param role
	 * @param queueId
	 * @return state of the kill operation
	 */
	public static boolean killJob(final AliEnPrincipal user, final String role, final int queueId) {
		if (AuthorizationChecker.canModifyJob(TaskQueueUtils.getJob(queueId), user, role)) {
			System.out.println("Authorized job kill for [" + queueId + "] by user/role [" + user.getName() + "/" + role + "].");

			final Job j = getJob(queueId, true);

			setJobStatus(j, JobStatus.KILLED, "", null, null, null);

			System.out.println("Exec host: " + j.execHost);

			if (j.execHost != null) {

				// my ($port) =
				// $self->{DB}->getFieldFromHosts($data->{exechost}, "hostport")
				// or
				// $self->info("Unable to fetch hostport for host $data->{exechost}")
				// and return (-1,
				// "unable to fetch hostport for host $data->{exechost}");
				//
				// $DEBUG and $self->debug(1,
				// "Sending a signal to $data->{exechost} $port to kill the process... ");
				final String target = j.execHost.substring(j.execHost.indexOf('@' + 1));

				final int expires = (int) (System.currentTimeMillis() / 1000) + 300;

				insertMessage(target, "ClusterMonitor", "killProcess", j.queueId + "", expires);

			}

			// The removal has to be done properly, in Perl it was just the
			// default !/alien-job directory
			// $self->{CATALOGUE}->execute("rmdir", $procDir, "-r")

			return false;

		}

		System.out.println("Job kill authorization failed for [" + queueId + "] by user/role [" + user.getName() + "/" + role + "].");
		return false;
	}

	// status and jdl
	private static boolean updateJob(final Job j, final JobStatus newStatus, final Map<String, String> jdltags) {

		if (newStatus.smallerThanEquals(j.status()) && (j.status() == JobStatus.ZOMBIE || j.status() == JobStatus.IDLE || j.status() == JobStatus.INTERACTIV) && j.isMaster())
			return false;

		if (j.status() == JobStatus.WAITING && j.jobagentId > 0)
			if (!deleteJobAgent(j.jobagentId))
				logger.log(Level.WARNING, "Error killing jobAgent: [" + j.jobagentId + "].");

		// $self->info("THE UPDATE WORKED!! Let's see if we have to delete an agent $status");
		// if ($dboldstatus =~ /WAITING/ and $oldjobinfo->{agentid}) {
		// $self->deleteJobAgent($oldjobinfo->{agentid});
		// }

		// TODO:
		// # send a job's status to MonaLisa
		// sub sendJobStatus {
		// my $self = shift;
		// my ($jobID, $newStatus, $execHost, $submitHost) = @_;
		//
		// if ($self->{MONITOR}) {
		// my $statusID = AliEn::Util::statusForML($newStatus);
		// $execHost = $execHost || "NO_SITE";
		// my @params = ("jobID", $jobID, "statusID", $statusID);
		// push(@params, "submitHost", "$jobID/$submitHost") if $submitHost;
		// $self->{MONITOR}->sendParameters("TaskQueue_Jobs_" .
		// $self->{CONFIG}->{ORG_NAME}, $execHost, @params);
		// }
		// }

		if (j.notify != null && !j.notify.equals(""))
			sendNotificationMail(j);

		if (j.split != 0)
			setSubJobMerges(j);

		if (j.status() != newStatus)
			if (newStatus == JobStatus.ASSIGNED) {
				// $self->_do("UPDATE $self->{SITEQUEUETABLE} SET $status=$status+1 where site=?",
				// {bind_values =>
				// [$dbsite]})
				// TODO:
			} else {
				// do(
				// "UPDATE $self->{SITEQUEUETABLE} SET $dboldstatus = $dboldstatus-1, $status=$status+1 where site=?",
				// {bind_values => [$dbsite]}
			}

		if (newStatus == JobStatus.KILLED || newStatus == JobStatus.SAVED || newStatus == JobStatus.SAVED_WARN || newStatus == JobStatus.STAGING)
			setAction(newStatus);

		// if ($status =~ /^DONE_WARN$/) {
		// $self->sendJobStatus($id, "DONE", $execHost, "");
		// }

		return true;
	}

	/**
	 * @param j
	 * @param newStatus
	 * @param arg
	 * @param site
	 * @param spyurl
	 * @param node
	 * @return <code>true</code> if the status was successfully changed
	 */
	public static boolean setJobStatus(final Job j, final JobStatus newStatus, final String arg, final String site, final String spyurl, final String node) {

		final String time = String.valueOf(System.currentTimeMillis() / 1000);

		final HashMap<String, String> jdltags = new HashMap<>();
		jdltags.put("procinfotime", time);
		if (spyurl != null)
			jdltags.put("spyurl", spyurl);
		if (site != null)
			jdltags.put("site", site);
		if (node != null)
			jdltags.put("node", node);

		if (newStatus == JobStatus.WAITING)
			jdltags.put("exechost", arg);
		else if (newStatus == JobStatus.RUNNING)
			jdltags.put("started", time);
		else if (newStatus == JobStatus.STARTED) {
			jdltags.put("started", time);
			jdltags.put("batchid", arg);
		} else if (newStatus == JobStatus.SAVING)
			jdltags.put("error", arg);
		else if ((newStatus == JobStatus.SAVED && arg != null && !"".equals(arg)) || newStatus == JobStatus.ERROR_V || newStatus == JobStatus.STAGING)
			jdltags.put("jdl", arg);
		else if (newStatus == JobStatus.DONE || newStatus == JobStatus.DONE_WARN) {
			jdltags.put("finished", time);
			if (j.usesValidation()) {
				final String host = j.execHost.substring(j.execHost.indexOf('@') + 1);
				final int port = 0; // $self->{CONFIG}->{CLUSTERMONITOR_PORT};

				// my $executable = "";
				// $data->{jdl} =~ /executable\s*=\s*"?(\S+)"?\s*;/i and
				// $executable = $1;
				// $executable =~ s/\"//g;
				// my $validatejdl = "[
				// Executable=\"$executable.validate\";
				// Arguments=\"$queueId $data->{host} $port\";
				// Requirements= member(other.GridPartition,\"Validation\");
				// Type=\"Job\";
				// ]";
				// $DEBUG and $self->debug(1,
				// "In changeStatusCommand sending the command to validate the result of $queueId...");
				// $self->enterCommand("$data->{submithost}", "$validatejdl");
				// }

			}

		} else if (JobStatus.finalStates().contains(newStatus) || newStatus == JobStatus.SAVED_WARN || newStatus == JobStatus.SAVED) {

			jdltags.put("spyurl", "");
			jdltags.put("finished", time);
			deleteJobToken(j.queueId);

		}
		// put the JobLog message

		final HashMap<String, String> joblogtags = new HashMap<>(jdltags);

		String message = "Job state transition from " + j.getStatusName() + " to " + newStatus;

		final boolean success = updateJob(j, newStatus, jdltags);

		if (!success)
			message = "FAILED: " + message;

		putJobLog(j.queueId, "state", message, joblogtags);

		if (site != null) {
			// # lock queues with submission errors ....
			// if ($status eq "ERROR_S") {
			// $self->_setSiteQueueBlocked($site)
			// or $self->{LOGGER}->error("JobManager",
			// "In changeStatusCommand cannot block site $site for ERROR_S");
			// } elsif ($status eq "ASSIGNED") {
			// my $sitestat = $self->getSiteQueueStatistics($site);
			// if (@$sitestat) {
			// if (@$sitestat[0]->{'ASSIGNED'} > 5) {
			// $self->_setSiteQueueBlocked($site)
			// or $self->{LOGGER}->error("JobManager",
			// "In changeStatusCommand cannot block site $site for ERROR_S");
			// }
			// }
			// }
		}
		return success;
	}

	private static boolean updateJDLAndProcInfo(final Job j, final Map<String, String> jdltags, final Map<String, String> procInfo) {

		// my $procSet = {};
		// foreach my $key (keys %$set) {
		// if ($key =~
		// /(si2k)|(cpuspeed)|(maxrsize)|(cputime)|(ncpu)|(cost)|(cpufamily)|(cpu)|(vsize)|(rsize)|(runtimes)|(procinfotime)|(maxvsize)|(runtime)|(mem)|(batchid)/
		// ) {
		// $procSet->{$key} = $set->{$key};
		// delete $set->{$key};
		// }
		// }

		// TODO: set the procinfo, is necessary

		// TODO: update the jdltags
		return true;

	}

	private static boolean insertMessage(final String target, final String service, final String message, final String messageArgs, final int expires) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return false;

			final String q = "INSERT INTO MESSAGES ( TargetService, Message, MessageArgs, Expires)  VALUES ('" + Format.escSQL(target) + "','" + Format.escSQL(service) + "','" + Format.escSQL(message)
					+ "','" + Format.escSQL(messageArgs) + "'," + Format.escSQL(expires + "") + ");";

			if (db.query(q)) {
				if (monitor != null)
					monitor.incrementCounter("Message_db_insert");

				return true;
			}

			return false;
		}
	}

	/**
	 * @param jobId
	 * @param username
	 * @param forceUpdate
	 * @return the new token
	 */
	public static JobToken insertJobToken(final int jobId, final String username, final boolean forceUpdate) {
		try (DBFunctions db = getQueueDB()) {
			JobToken jb = getJobToken(jobId);

			if (jb != null)
				System.out.println("TOKEN EXISTED");

			if (jb != null && !forceUpdate)
				return null;

			if (jb == null)
				jb = new JobToken(jobId, username);

			jb.emptyToken(db);

			// System.out.println("forceUpdate token: " + jb.toString());

			if (jb.exists())
				return jb;

			// System.out.println("jb does not exist");

			return null;
		}
	}

	private static JobToken getJobToken(final int jobId) {
		try (DBFunctions db = getQueueDB()) {
			if (monitor != null) {
				monitor.incrementCounter("TQ_db_lookup");
				monitor.incrementCounter("TQ_jobtokendetails");
			}

			final long lQueryStart = System.currentTimeMillis();

			final String q = "SELECT * FROM JOBTOKEN WHERE jobId=?;";

			db.setReadOnly(true);

			if (!db.query(q, false, Integer.valueOf(jobId)))
				return null;

			monitor.addMeasurement("TQ_jobtokendetails_time", (System.currentTimeMillis() - lQueryStart) / 1000d);

			if (!db.moveNext())
				return null;

			return new JobToken(db);
		}
	}

	private static boolean deleteJobToken(final int queueId) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return false;

			if (monitor != null)
				monitor.incrementCounter("QUEUE_db_lookup");

			if (!db.query("DELETE FROM JOBTOKEN WHERE jobId=?;", false, Integer.valueOf(queueId))) {
				putJobLog(queueId, "state", "Failed to execute job token deletion query", null);

				return false;
			}

			final int cnt = db.getUpdateCount();

			putJobLog(queueId, "state", "Job token deletion query affected " + cnt + " rows", null);

			return cnt > 0;
		}
	}

	/**
	 * @param queueId
	 * @param action
	 * @param message
	 * @param joblogtags
	 * @return <code>true</code> if the log was successfully added
	 */
	public static boolean putJobLog(final int queueId, final String action, final String message, final HashMap<String, String> joblogtags) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return false;

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_lookup");
				monitor.incrementCounter("TQ_JOBMESSAGES_insert");
			}

			final Map<String, Object> insertValues = new HashMap<>(4);

			insertValues.put("timestamp", Long.valueOf(System.currentTimeMillis() / 1000));
			insertValues.put("jobId", Integer.valueOf(queueId));
			insertValues.put("procinfo", message);
			insertValues.put("tag", action);

			return db.query(DBFunctions.composeInsert("JOBMESSAGES", insertValues));
		}
	}

	private static boolean deleteJobAgent(final int jobagentId) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return false;

			logger.log(Level.INFO, "We would be asked to kill jobAgent: [" + jobagentId + "].");

			db.query("update JOBAGENT set counter=counter-1 where entryId=?", false, Integer.valueOf(jobagentId));

			final int updated = db.getUpdateCount();

			db.query("delete from JOBAGENT where counter<1");

			return updated > 0;
		}
	}

	private static void checkFinalAction(final Job j) {
		if (j.notify != null && !j.notify.equals(""))
			sendNotificationMail(j);

	}

	private static void sendNotificationMail(final Job j) {
		// send j.notify an info
		// TODO:
	}

	private static boolean setSubJobMerges(final Job j) {

		// if ($info->{split}) {
		// $self->info("We have to check if all the subjobs of $info->{split} have finished");
		// $self->do(
		// "insert into JOBSTOMERGE (masterId) select ? from DUAL where not exists (select masterid from JOBSTOMERGE where masterid = ?)",
		// {bind_values => [ $info->{split}, $info->{split} ]}
		// );
		// $self->do("update ACTIONS set todo=1 where action='MERGING'");
		// }
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return false;

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_lookup");
				monitor.incrementCounter("TQ_JOBSTOMERGE_lookup");
			}

			final String q = "INSERT INTO JOBSTOMERGE (masterId) SELECT " + j.split + " FROM DUAL WHERE NOT EXISTS (SELECT masterid FROM JOBSTOMERGE WHERE masterid = " + j.split + ");";

			if (!db.query(q))
				return false;

			return setAction(JobStatus.MERGING);
		}
	}

	private static boolean setAction(final JobStatus status) {
		// $self->update("ACTIONS", {todo => 1}, "action='$status'");
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return false;

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_update");
				monitor.incrementCounter("TQ_ACTIONS_update");
			}

			final String q = "UPDATE ACTIONS SET todo=1 WHERE action=? AND todo=0;";

			if (!db.query(q, false, status.toSQL()))
				return false;
		}

		return true;
	}

	/**
	 * @param args
	 */
	public static void main(final String[] args) {
		System.out.println("QUEUE TESTING...");

		if (getAdminDB() == null)
			System.out.println("ADMIN DB NULL.");

		System.out.println("---------------------------------------------------------------------");

		if (insertJobToken(12341234, "me", true) == null)
			System.out.println("exists, update refused.");

		System.out.println("---------------------------------------------------------------------");

		if (insertJobToken(12341234, "me", true) == null)
			System.out.println("exists, update refused.");

		System.out.println("---------------------------------------------------------------------");

		deleteJobToken(12341234);

	}

	/**
	 * Get the number of jobs in the respective state
	 *
	 * @param states
	 * @return the aggregated number of jobs per user
	 */
	public static Map<String, Integer> getJobCounters(final Set<JobStatus> states) {
		final Map<String, Integer> ret = new TreeMap<>();

		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return null;

			final StringBuilder sb = new StringBuilder();

			if (monitor != null)
				monitor.incrementCounter("TQ_db_lookup");

			if (states != null && !states.contains(JobStatus.ANY))
				for (final JobStatus s : states) {
					if (sb.length() > 0)
						sb.append(',');

					if (dbStructure2_20)
						sb.append(s.getAliEnLevel());
					else
						sb.append('\'').append(s.toSQL()).append('\'');
				}

			String q;

			if (dbStructure2_20)
				q = "SELECT user,count(1) FROM QUEUE INNER JOIN QUEUE_USER using(userId) WHERE statusId IN (" + sb + ")";
			else
				q = "SELECT substring_index(submithost,'@',1),count(1) FROM QUEUE WHERE status IN (" + sb + ")";

			q += "GROUP BY 1 ORDER BY 1;";

			db.setReadOnly(true);

			db.query(q);

			while (db.moveNext())
				ret.put(StringFactory.get(db.gets(1)), Integer.valueOf(db.geti(2)));
		}

		return ret;
	}

	/**
	 * @param ce
	 * @param status
	 * @param extraparams
	 */
	public static void setSiteQueueStatus(final String ce, final String status, final Object... extraparams) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return;

			logger.log(Level.INFO, "Setting site with ce " + ce + " to " + status);

			db.query("update SITEQUEUES set statustime=UNIX_TIMESTAMP(NOW()), status=? where site=?", false, status, ce);

			if (db.getUpdateCount() == 0) {
				logger.log(Level.INFO, "Inserting the site " + ce);
				insertSiteQueue(ce);
			}
		}
	}

	/**
	 * @param ce
	 */
	public static void insertSiteQueue(final String ce) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return;

			if (!db.query("insert into SITEQUEUES (siteid, site) select ifnull(max(siteid)+1,1), ? from SITEQUEUES", false, ce)) {
				logger.log(Level.INFO, "Couldn't insert queue " + ce);
				return;
			}

			resyncSiteQueueTable();
		}
	}

	/**
	 *
	 */
	public static void resyncSiteQueueTable() {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return;

			final HashMap<String, Integer> status = getJobStatusFromDB();

			String sql = " update SITEQUEUES left join (select siteid, sum(cost) REALCOST, ";
			String set = " Group by statusId, siteid) dd group by siteid) bb using (siteid) set cost=REALCOST, ";

			for (final String st : status.keySet()) {
				sql += " max(if(statusId=" + status.get(st) + ", count, 0)) REAL" + st + ",";
				set += " " + st + "=REAL" + st + ",";
			}
			sql = sql.substring(0, sql.length() - 1);
			sql = set.substring(0, set.length() - 1);

			sql += " from (select siteid, statusId, sum(cost) as cost, count(*) as count from QUEUE join QUEUEPROC using(queueid) ";
			sql += set;

			logger.log(Level.INFO, "resyncSiteQueueTable with " + sql);

			db.query(sql, false);
		}
	}

	/**
	 * @return dictionary of job statuses
	 */
	public static HashMap<String, Integer> getJobStatusFromDB() {
		// FIXME cache the result
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return null;

			db.query("select status, statusId from QUEUE_STATUS", false);
			final HashMap<String, Integer> status = new HashMap<>();

			while (db.moveNext())
				status.put(db.gets(1), Integer.valueOf(db.geti(2)));

			return status;
		}
	}

	/**
	 * @param host
	 * @param status
	 * @return <code>true</code> if the status was updated
	 */
	public static boolean updateHostStatus(final String host, final String status) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return false;

			if (host == null || host.equals("") || status == null || status.equals("")) {
				logger.log(Level.INFO, "Host or status parameters are empty");
				return false;
			}

			logger.log(Level.INFO, "Updating host " + host + " to status " + status);

			if (!db.query("update HOSTS set status=?,date=UNIX_TIMESTAMP(NOW()) where hostName=?", false, status, host)) {
				logger.log(Level.INFO, "Update HOSTS failed: " + host + " and " + status);
				return false;
			}

			return db.getUpdateCount() != 0;
		}
	}

	/**
	 * @param key
	 * @param value
	 * @return value for this key
	 */
	public static int getOrInsertFromLookupTable(final String key, final String value) {
		// FIXME: these values can also be cached
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return 0;

			final String table = "QUEUE_" + key.toUpperCase();
			final String id = key + "id";
			final String q = "select " + id + " from " + table + " where " + key + "=?";

			logger.log(Level.INFO, "Going to get hostId, query: " + q);

			db.setReadOnly(true);
			db.query(q, false, value);

			// the host exists
			if (db.moveNext()) {
				logger.log(Level.INFO, "The host exists: " + db.geti(1));
				return db.geti(1);
			}
			// host doesn't exist, we insert it
			logger.log(Level.INFO, "The host doesn't exist. Inserting...");

			db.setLastGeneratedKey(true);

			boolean ret = db.query("insert into " + table + " (" + key + ") values (?)", false, value);
			
			logger.log(Level.INFO, "insert into " + table + " (" + key + ") values (?) with ?="+value+": "+ret);
			
			if (ret)
				return db.getLastGeneratedKey().intValue();

			// something went wrong ? :-(
			return 0;
		}
	}

	/**
	 * @param ceName
	 * @return site ID
	 */
	public static int getSiteId(final String ceName) {
		// FIXME cache the values
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return 0;

			logger.log(Level.INFO, "Going to select siteId: select siteid from SITEQUEUES where site=? " + ceName);

			db.setReadOnly(true);
			db.query("select siteid from SITEQUEUES where site=?", false, ceName);

			if (db.moveNext())
				return db.geti(1);
		}
		return 0;
	}

	public static int getUserIdFromName(final String user) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return 0;

			db.setReadOnly(true);
			db.query("select userId from QUEUE_USER where user=?", false, user);

			if (db.moveNext())
				return db.geti(1);
		}
		return 0;
	}

	public static int deleteJobAgent(final int agentId, final int queueId) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return 0;

			final ArrayList<Object> bindValues = new ArrayList<>();
			String oldestQueueIdQ = "";

			if (queueId > 0) {
				bindValues.add(queueId);
				oldestQueueIdQ = ",oldestQueueId=?";
			}

			bindValues.add(agentId);

			db.query("update JOBAGENT set counter=counter-1 " + oldestQueueIdQ + " where entryId=?", false, bindValues.toArray(new Object[0]));

			db.query("delete from JOBAGENT where counter<1", false);
		}
		return 1;
	}

}
