package alien.shell.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import alien.taskQueue.Job;
import alien.taskQueue.JobStatus;

/**
 * @author ron
 * @since June 9, 2011
 */
public class JAliEnCommandmasterjob extends JAliEnBaseCommand {

	/**
	 * marker for -M argument
	 */
	private boolean bMerge = false;

	/**
	 * marker for -a argument
	 */
	private boolean bKill = false;

	/**
	 * marker for -l argument
	 */
	private boolean bResubmit = false;

	/**
	 * marker for -M argument
	 */
	private boolean bExpunge = false;

	/**
	 * marker for -M argument
	 */
	private boolean bPrintId = false;

	/**
	 * marker for -M argument
	 */
	private boolean bPrintSite = false;

	private final int jobId;

	private final List<Integer> id = new ArrayList<>();

	private Set<JobStatus> status = new HashSet<>();

	private final List<String> sites = new ArrayList<>();

	@Override
	public void run() {

		final Job j = commander.q_api.getJob(jobId);

		List<Job> subjobstates = null;

		// for (Job j : masterjobstatus.keySet()) {
		if (j != null)
			subjobstates = commander.q_api.getMasterJobStatus(j.queueId, status, id, sites, bPrintId, bPrintSite, bMerge, bKill, bResubmit, bExpunge);
		else
			return;

		if (out.isRootPrinter()) {
			out.nextResult();
			out.setField("Checking the masterjob ", " " + j.queueId);
			out.setField("The job ", j.queueId + " is in status: " + j.status());
			final HashMap<String, List<Job>> stateCount = new HashMap<>();

			ArrayList<String> sitesIn = new ArrayList<>();
			ArrayList<JobStatus> statesIn = new ArrayList<>();

			final ArrayList<JobStatus> allStates = new ArrayList<>();
			final ArrayList<String> allSites = new ArrayList<>();

			if (subjobstates != null) {
				out.setField("message ", "It has the following subjobs:");

				for (final Job sj : subjobstates) {
					// count the states the subjobs have

					String key = sj.status().toString();

					if (bPrintSite) {
						String site = "";
						if (sj.execHost != null && sj.execHost.contains("@"))
							site = sj.execHost.substring(sj.execHost.indexOf('@') + 1);

						if (site.length() > 0)
							key += "/" + site;

						if (sitesIn.size() <= 0)
							if (!allSites.contains(site))
								allSites.add(site);

					}

					List<Job> jobs = stateCount.get(key);

					if (jobs == null) {
						jobs = new ArrayList<>();
						stateCount.put(key, jobs);
					}

					jobs.add(sj);

					if (statesIn.size() <= 0 && !allStates.contains(sj.status()))
						allStates.add(sj.status());
				}
				if (statesIn.size() <= 0)
					statesIn = allStates;

				if (sitesIn.size() <= 0)
					sitesIn = allSites;

				//
				if (sitesIn.size() > 0)
					for (final String site : sitesIn)
						printSubJobs(stateCount, statesIn, site);
				else
					printSubJobs(stateCount, statesIn, null);

				out.setField("In total, there are ", subjobstates.size() + " subjobs");

			}
		} else {

			out.printOutln("Checking the masterjob " + j.queueId);
			out.printOutln("The job " + j.queueId + " is in status: " + j.status());

			final HashMap<String, List<Job>> stateCount = new HashMap<>();

			ArrayList<String> sitesIn = new ArrayList<>();
			ArrayList<JobStatus> statesIn = new ArrayList<>();

			final ArrayList<JobStatus> allStates = new ArrayList<>();
			final ArrayList<String> allSites = new ArrayList<>();

			if (subjobstates != null) {

				out.printOutln("It has the following subjobs:");

				for (final Job sj : subjobstates) {
					// count the states the subjobs have

					String key = sj.status().toString();

					if (bPrintSite) {
						String site = "";
						if (sj.execHost != null && sj.execHost.contains("@"))
							site = sj.execHost.substring(sj.execHost.indexOf('@') + 1);

						if (site.length() > 0)
							key += "/" + site;

						if (sitesIn.size() <= 0)
							if (!allSites.contains(site))
								allSites.add(site);

					}

					List<Job> jobs = stateCount.get(key);

					if (jobs == null) {
						jobs = new ArrayList<>();
						stateCount.put(key, jobs);
					}

					jobs.add(sj);

					if (statesIn.size() <= 0 && !allStates.contains(sj.status()))
						allStates.add(sj.status());
				}
				if (statesIn.size() <= 0)
					statesIn = allStates;

				if (sitesIn.size() <= 0)
					sitesIn = allSites;

				//
				if (sitesIn.size() > 0)
					for (final String site : sitesIn)
						printSubJobs(stateCount, statesIn, site);
				else
					printSubJobs(stateCount, statesIn, null);

				out.printOutln();
				out.printOutln("In total, there are " + subjobstates.size() + " subjobs");

			}
		}
	}

	private void printSubJobs(final HashMap<String, List<Job>> stateCount, final List<JobStatus> showStatus, final String site) {
		String key = "";

		if (site != null && site.length() > 0)
			key = "/" + site;

		for (final JobStatus state : showStatus) {
			final List<Job> subjobs = stateCount.get(state.toString() + key);

			if (subjobs != null && subjobs.size() > 0) {
				final StringBuilder ret = new StringBuilder();

				ret.append(padSpace(16)).append("Subjobs in ").append(state);
				if (bPrintSite)
					ret.append(" (").append(site).append(")");

				ret.append(": ").append(subjobs.size());

				if (bPrintId) {
					ret.append(" (ids: ");

					boolean first = true;

					for (final Job sj : subjobs) {
						if (!first)
							ret.append(',');
						else
							first = false;

						ret.append(sj.queueId);
					}

					ret.append(')');
				}

				out.printOutln(ret.toString());
			}
		}
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {

		out.printOutln();
		out.printOutln(helpUsage("masterjob", "<jobId> [-options] [merge|kill|resubmit|expunge]"));
		out.printOutln(helpStartOptions());
		out.printOutln(helpOption("-status <status>", "display only the subjobs with that status"));
		out.printOutln(helpOption("-id <id>", "display only the subjobs with that id"));
		out.printOutln(helpOption("-site <id>", "display only the subjobs on that site"));
		out.printOutln(helpOption("-printid", "print also the id of all the subjobs"));
		out.printOutln(helpOption("-printsite", "split the number of jobs according to the execution site"));
		out.printOutln();
		out.printOutln(helpOption("merge", "collect the output of all the subjobs that have already finished"));
		out.printOutln(helpOption("kill", "kill all the subjobs"));
		out.printOutln(helpOption("resubmit", "resubmit all the subjobs selected"));
		out.printOutln(helpOption("expunge", "delete completely the subjobs"));
		out.printOutln();
		out.printOutln(helpParameter("You can combine kill and resubmit with '-status <status>' and '-id <id>'."));
		out.printOutln(helpParameter("For instance, if you do something like 'masterjob <jobId> -status ERROR_IB resubmit',"));
		out.printOutln(helpParameter(" all the subjobs with status ERROR_IB will be resubmitted"));
		out.printOutln();
	}

	/**
	 * cat cannot run without arguments
	 * 
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * Constructor needed for the command factory in commander
	 * 
	 * @param commander
	 * @param out
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandmasterjob(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);

		try {
			if (alArguments.size() > 0) {

				{
					try {
						jobId = Integer.parseInt(alArguments.get(0));
					} catch (final NumberFormatException e) {
						throw new JAliEnCommandException();
					}
				}

				final OptionParser parser = new OptionParser();

				parser.accepts("status").withRequiredArg();
				parser.accepts("id").withRequiredArg();
				parser.accepts("site").withRequiredArg();

				parser.accepts("printid");
				parser.accepts("printsite");

				final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

				if (options.has("status") && options.hasArgument("status")) {
					final StringTokenizer st = new StringTokenizer((String) options.valueOf("status"), ",");
					while (st.hasMoreTokens()) {
						final String state = st.nextToken();

						if ("ERROR_ALL".equals(state)) {
							status = JobStatus.errorneousStates();
							break;
						}

						status.add(JobStatus.getStatus(state));
					}
				}

				if (options.has("id") && options.hasArgument("id")) {
					final StringTokenizer st = new StringTokenizer((String) options.valueOf("id"), ",");
					while (st.hasMoreTokens())
						try {
							id.add(Integer.valueOf(st.nextToken()));
						} catch (final NumberFormatException e) {
							// ignore
						}
				}

				if (options.has("site") && options.hasArgument("site")) {
					final StringTokenizer st = new StringTokenizer((String) options.valueOf("site"), ",");
					while (st.hasMoreTokens())
						sites.add(st.nextToken());
				}

				bPrintId = options.has("printid");
				bPrintSite = options.has("printsite");

				final String flag = alArguments.get(alArguments.size() - 1);

				if (flag != null)
					if (flag.equals("merge"))
						bMerge = true;
					else if (flag.equals("kill"))
						bKill = true;
					else if (flag.equals("resubmit"))
						bResubmit = true;
					else if (flag.equals("expunge"))
						bExpunge = true;

			} else
				throw new JAliEnCommandException();
		} catch (final OptionException e) {
			printHelp();
			throw e;
		}

	}

}
