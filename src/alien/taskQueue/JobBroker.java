package alien.taskQueue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;

/**
 * 
 */
public class JobBroker {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(JobBroker.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(JobBroker.class.getCanonicalName());

	/**
	 * @param matchRequest
	 * @return the information of a matching job for the jobAgent (queueId, JDL...)
	 */
	public static HashMap<String, Object> getMatchJob(final HashMap<String, Object> matchRequest) {
		try (DBFunctions db = TaskQueueUtils.getQueueDB()) {
			if (db == null)
				return null;

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_lookup");
				monitor.incrementCounter("TQ_get_match_job");
			}

			HashMap<String, Object> matchAnswer = new HashMap<>();
			HashMap<String, Object> waiting = new HashMap<>();

			// Checking if the CE is open
			final int openQueue = checkQueueOpen((String) matchRequest.get("CE"));
			if (openQueue != 1) {
				logger.log(Level.INFO, "Queue is not open! Check queueinfo");
				matchAnswer.put("Error", "Queue is not open! Check queueinfo");
				return matchAnswer;
			}

			matchRequest.put("Remote", Integer.valueOf(0));
			matchRequest.put("Return", "entryId"); // skipping ,filebroker

			// print output to JobBroker/host file
			if (!TaskQueueUtils.updateHostStatus((String) matchRequest.get("CE"), "ACTIVE")) {
				logger.log(Level.INFO, "Updating queue failed!");
				matchAnswer.put("Error", "Updating host failed");
				return matchAnswer;
			}

			waiting = getNumberWaitingForSite(matchRequest);

			// we got something back fitting all requirements :-)
			if (waiting.containsKey("entryId")) {
				logger.log(Level.INFO, "We have a job back");
				matchAnswer = getWaitingJobForAgentId(waiting);
			} else {
				// try without InstalledPackages
				final String installedPackages = (String) matchRequest.get("InstalledPackages");
				matchRequest.remove("InstalledPackages");
				matchRequest.put("Return", "packages");

				waiting = getNumberWaitingForSite(matchRequest);

				if (waiting.containsKey("packages")) {
					final String packages = (String) waiting.get("packages");
					logger.log(Level.INFO, "Telling the site to install packages '" + packages + "'");

					final ArrayList<String> list = new ArrayList<>(Arrays.asList(packages.split(",")));
					list.removeAll(Collections.singleton("%"));

					logger.log(Level.INFO, "After removing, we have to install @packs ");

					TaskQueueUtils.setSiteQueueStatus((String) matchRequest.get("CE"), "jobagent-install-pack");

					matchAnswer.put("Error", "Packages needed to install: " + list.toString());
					matchAnswer.put("Code", Integer.valueOf(-3));
				} else {
					// try remote access (no site)
					logger.log(Level.INFO, "Going to try with remote execution agents");
					matchRequest.put("InstalledPackages", installedPackages);
					matchRequest.put("Return", "entryId");
					matchRequest.put("Remote", Integer.valueOf(1));
					matchRequest.remove("Site");

					waiting = getNumberWaitingForSite(matchRequest);

					if (waiting.containsKey("entryId")) {
						logger.log(Level.INFO, "We have a job back for remote");
						matchAnswer = getWaitingJobForAgentId(waiting);
					} else {
						// last case, no site && no packages...
						matchRequest.put("Return", "packages");
						matchRequest.remove("InstalledPackages");

						waiting = getNumberWaitingForSite(matchRequest);

						if (waiting.containsKey("packages")) {
							final String packages = (String) waiting.get("packages");
							logger.log(Level.INFO, "Telling the site to install packages '" + packages + "'");

							final ArrayList<String> list = new ArrayList<>(Arrays.asList(packages.split(",")));
							list.removeAll(Collections.singleton("%"));

							logger.log(Level.INFO, "After removing, we have to install @packs ");

							TaskQueueUtils.setSiteQueueStatus((String) matchRequest.get("CE"), "jobagent-install-pack");

							matchAnswer.put("Error", "Packages needed to install (remote): " + list.toString());
							matchAnswer.put("Code", Integer.valueOf(-3));
						} else {
							logger.log(Level.INFO, "Removing site and packages requirements hasn't been enough. Nothing to run!");
							matchAnswer.put("Error", "Nothing to run :-(");
							matchAnswer.put("Code", Integer.valueOf(-2));
							TaskQueueUtils.setSiteQueueStatus((String) matchRequest.get("CE"), "jobagent-no-match");
						}
					}
				}
			}

			// we get back the needed information
			if (matchAnswer.containsKey("queueId")) {
				// success!!
				matchAnswer.put("Code", Integer.valueOf(1));

				// TODO:joblog
				// putlog($queueid, "state", "Job state transition from WAITING to ASSIGNED (to $queueName)");

				final JobToken jobToken = TaskQueueUtils.insertJobToken(((Integer) matchAnswer.get("queueId")).intValue(), (String) matchAnswer.get("User"), false);

				if (jobToken == null || !jobToken.spawnToken(db)) {
					logger.log(Level.INFO, "The job already had a jobToken (or failed creating)!");
					db.setReadOnly(true);
					if (db.query("select * from QUEUE where queueId=?", false, (Integer) matchAnswer.get("queueId"))) {
						final Job j = new Job(db, false);
						TaskQueueUtils.setJobStatus(j, JobStatus.ERROR_A, "", null, null, null);
					}
					// TODO: putlog($queueid, "state", "Job state transition from ASSIGNED to ERRROR_A");
					matchAnswer.put("Code", Integer.valueOf(-1));
					matchAnswer.put("Error", "Error getting the token of the job " + matchAnswer.get("queueId"));
				} else {
					logger.log(Level.INFO, "Creating a jobToken for the job...");
					matchAnswer.put("jobToken", jobToken.token);
					TaskQueueUtils.setSiteQueueStatus((String) matchRequest.get("CE"), "jobagent-match");
				}
			} // nothing back, something went wrong while obtaining queueId from the positive cases
			else if (!matchAnswer.containsKey("Code")) {
				matchAnswer.put("Error", "Nothing to run :-( (no waiting jobs?) ");
				matchAnswer.put("Code", Integer.valueOf(-2));
				TaskQueueUtils.setSiteQueueStatus((String) matchRequest.get("CE"), "jobagent-no-match");
			}

			return matchAnswer;
		}
	}

	private static HashMap<String, Object> getWaitingJobForAgentId(final HashMap<String, Object> waiting) {
		try (DBFunctions db = TaskQueueUtils.getQueueDB()) {
			if (db == null)
				return null;

			final String agentId = (String) waiting.get("entryId");
			final String host = (String) waiting.get("Host");
			final String ceName = (String) waiting.get("CE");

			int hostId, siteId;

			hostId = TaskQueueUtils.getOrInsertFromLookupTable("host", host);
			siteId = TaskQueueUtils.getSiteId(ceName);

			if (hostId == 0 || siteId == 0)
				logger.log(Level.INFO, "The value for " + (hostId > 0 ? "site" : "host") + " is missing");

			logger.log(Level.INFO, "Getting a waiting job for " + agentId + " and " + host + " and " + hostId);

			String extra = "";
			if (((Integer) waiting.get("Remote")).intValue() == 1)
				extra = "and timestampdiff(SECOND,mtime,now())>=ifnull(remoteTimeout,$self->{DEFAULTREMOTETIMEOUT})";

			db.query("UPDATE QUEUE set statusId=" + JobStatus.ASSIGNED.getAliEnLevel() + " ,siteid=?, exechostid=?, sent=now() " + "where statusId=" + JobStatus.WAITING.getAliEnLevel()
					+ " and agentid=? " + extra + " and @assigned_job:=queueid limit 1", false, Integer.valueOf(siteId), Integer.valueOf(hostId), agentId);

			if (db.getUpdateCount() > 0) {
				// we got something to run

				String jdl, user;
				int queueId;

				db.query("select queueId, origjdl jdl, user from QUEUEJDL join QUEUE using (queueid) " + "join QUEUE_USER using (userId) where queueId=@assigned_job", false);

				if (db.moveNext()) {
					queueId = db.geti(1);
					jdl = db.gets(2);
					user = db.gets(3);
				} else {
					logger.log(Level.INFO, "Couldn't get the queueId, jdl and user for the agentId: " + agentId);
					return null;
				}

				db.query("update QUEUEPROC set lastupdate=CURRENT_TIMESTAMP where queueId=@assigned_job", false);

				db.query("update SITEQUEUES set ASSIGNED=ASSIGNED+1 where siteid=?", false, Integer.valueOf(siteId));
				db.query("update SITEQUEUES set WAITING=WAITING-1 where siteid=?", false, Integer.valueOf(siteId));

				final HashMap<String, Object> job = new HashMap<>();
				job.put("queueId", Integer.valueOf(queueId));
				job.put("JDL", jdl);
				job.put("User", user);

				logger.log(Level.INFO, "Going to return " + queueId + " and " + user + " and " + jdl);
				return job;
			}
			logger.log(Level.INFO, "No jobs to give back");
			return null;
		}
	}

	private static int checkQueueOpen(final String ce) {
		try (DBFunctions db = TaskQueueUtils.getQueueDB()) {
			if (db == null)
				return -2;

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_lookup");
				monitor.incrementCounter("TQ_queue_open");
			}

			db.setReadOnly(true);

			db.query("select count(*) from SITEQUEUES where blocked='open' and site='" + ce + "'");

			if (db.moveNext())
				if (db.geti(1) > 0)
					return 1;

			return -1;
		}
	}

	/**
	 * @param matchRequest
	 * @return number of jobs waiting for a site given its parameters, or an entry to JOBAGENT if asked for
	 */
	public static HashMap<String, Object> getNumberWaitingForSite(final HashMap<String, Object> matchRequest) {
		try (DBFunctions db = TaskQueueUtils.getQueueDB()) {
			if (db == null)
				return null;

			final HashMap<String, Object> matchAnswer = new HashMap<>();
			matchAnswer.put("Code", Integer.valueOf(0));

			String where = "";
			String ret = "sum(counter) as counter";
			if (matchRequest.containsKey("Return"))
				ret = (String) matchRequest.get("Return");

			final ArrayList<Object> bindValues = new ArrayList<>();

			if (matchRequest.containsKey("TTL")) {
				where += "and ttl < ? ";
				bindValues.add(matchRequest.get("TTL"));
			}

			if (matchRequest.containsKey("Disk")) {
				where += "and disk < ? ";
				bindValues.add(matchRequest.get("Disk"));
			}

			if (matchRequest.containsKey("Site")) {
				where += "and (site='' or site like concat('%,',?,',%')) ";
				bindValues.add(matchRequest.get("Site"));
			}
			// skipping extrasites: used ?

			if (matchRequest.containsKey("InstalledPackages")) {
				where += "and ? like packages ";
				bindValues.add(matchRequest.get("InstalledPackages"));
			} else {
				where += "and ? like packages ";
				bindValues.add(matchRequest.get("Packages"));
			}

			if (matchRequest.containsKey("Partition")) {
				where += "and ? like concat('%,',`partition`, '%,') ";
				bindValues.add(matchRequest.get("Partition"));
			}

			if (matchRequest.containsKey("CE")) {
				where += " and (ce like '' or ce like concat('%,',?,',%')) and noce not like concat('%,',?,',%')";
				bindValues.add(matchRequest.get("CE"));
				bindValues.add(matchRequest.get("CE"));
			}

			if (matchRequest.containsKey("Users")) {
				final ArrayList<String> users = (ArrayList<String>) matchRequest.get("Users");
				String orconcat = " and (";
				for (final String user : users) {
					where += orconcat + "userId like ?";
					orconcat = " or ";
					bindValues.add(user);
				}
				where += ")";
			}

			db.setReadOnly(true);

			if (((Integer) matchRequest.get("Remote")).intValue() == 1) {
				logger.log(Level.INFO, "Checking for remote agents");

				// TODO: ask cache for ns:jobbroker key:remoteagents
				// $self->{CONFIG}->{CACHE_SERVICE_ADDRESS}?ns=jobbroker&key=remoteagents

				db.query("select distinct agentId from QUEUE where agentId is not null and statusId=5 " + "and timestampdiff(SECOND,mtime,now())>=ifnull(remoteTimeout,43200)");

				if (db.moveNext()) {
					where += "and entryId in (";

					do {
						final int agentid = db.geti("agentId");
						where += agentid + ",";
					} while (db.moveNext());

					where = where.substring(0, where.length() - 1);
					where += ")";

					// TODO: store in cache
					// $self->{CONFIG}->{CACHE_SERVICE_ADDRESS}?ns=jobbroker&key=remoteagents&timeout=300&value=".Dumper([@$agents])
				} else
					return matchAnswer;
			}

			db.setReadOnly(true);

			final String q = "select " + ret + " from JOBAGENT where 1=1 " + where + " order by priority desc limit 1";
			logger.log(Level.INFO, "Going to select agents (" + q + ")");

			db.query(q, false, bindValues.toArray(new Object[0]));

			if (db.moveNext()) {
				final String[] columns = db.getColumnNames();
				matchAnswer.put("Code", Integer.valueOf(1));

				for (final String col : columns)
					matchAnswer.put(col, db.gets(col));
			}

			return matchAnswer;
		}
	}

}
