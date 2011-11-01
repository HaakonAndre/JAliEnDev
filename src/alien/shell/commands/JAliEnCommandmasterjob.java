package alien.shell.commands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import alien.taskQueue.Job;

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

	private List<Integer> id = new ArrayList<Integer>();

	private List<String> status = new ArrayList<String>();

	private List<String> sites = new ArrayList<String>();

	public void run() {

		Job j = commander.q_api.getJob(jobId);

		List<Job> subjobstates = null;

		// for (Job j : masterjobstatus.keySet()) {
		if (j != null)
			subjobstates = commander.q_api.getMasterJobStatus(j.queueId,
					status, id, sites, bPrintId, bPrintSite, bMerge, bKill,
					bResubmit, bExpunge);
		else
			return;

		out.printOutln("Checking the masterjob " + j.queueId);
		out.printOutln("The job " + j.queueId + " is in status: " + j.status);

		HashMap<String, List<Job>> stateCount = new HashMap<String, List<Job>>();

		ArrayList<String> sitesIn = new ArrayList<String>();
		ArrayList<String> statesIn = new ArrayList<String>();

		ArrayList<String> allStates = new ArrayList<String>();
		ArrayList<String> allSites = new ArrayList<String>();

		if (subjobstates != null) {

			out.printOutln("It has the following subjobs:");

			for (Job sj : subjobstates) {
				// count the states the subjobs have

				String key = sj.status;

				if (bPrintSite) {

					String site = "";
					if (sj.execHost != null && sj.execHost.contains("@"))
						site = sj.execHost
								.substring(sj.execHost.indexOf('@') + 1);

					if (site.length() > 0)
						key += "/" + site;

					if (sitesIn.size() <= 0)
						if (!allSites.contains(site))
							allSites.add(site);

				}
				if (stateCount.containsKey(key))
					stateCount.get(key).add(sj);
				else {

					stateCount.put(key, new ArrayList<Job>(Arrays.asList(sj)));
				}

				if (statesIn.size() <= 0 && !allStates.contains(sj.status))
					allStates.add(sj.status);
			}
			if (statesIn.size() <= 0)
				statesIn = allStates;
			if (sitesIn.size() <= 0)
				sitesIn = allSites;

			//
			if (sitesIn.size() > 0) {
				for (String site : sitesIn)
					printSubJobs(stateCount, statesIn, site);
			} else
				printSubJobs(stateCount, statesIn, null);

			out.printOutln();
			out.printOutln("In total, there are " + subjobstates.size()
					+ " subjobs");

		}

	}

	private void printSubJobs(final HashMap<String, List<Job>> stateCount,
			final List<String> showStatus, String site) {
		String key = "";
		if (site != null && site.length() > 0)
			key = "/" + site;

		for (String state : showStatus) {

			List<Job> subjobs = stateCount.get(state + key);

			if (subjobs != null && subjobs.size() > 0) {
				String ret = padSpace(16) + "Subjobs in " + state;
				if (bPrintSite)
					ret += " (" + site + ")";
				ret += ": " + subjobs.size();
				if (bPrintId) {
					ret += " (ids: ";
					for (Job sj : subjobs)
						ret += sj.queueId + ",";
					ret = ret.substring(0, ret.length() - 1) + ")";
				}
				out.printOutln(ret);
			}
		}
	}

	/**
	 * printout the help info
	 */
	public void printHelp() {

		out.printOutln();
		out.printOutln(helpUsage("masterjob",
				"<jobId> [-options] [merge|kill|resubmit|expunge]"));
		out.printOutln(helpStartOptions());
		out.printOutln(helpOption("-status <status>",
				"display only the subjobs with that status"));
		out.printOutln(helpOption("-id <id>",
				"display only the subjobs with that id"));
		out.printOutln(helpOption("-site <id>",
				"display only the subjobs on that site"));
		out.printOutln(helpOption("-printid",
				"print also the id of all the subjobs"));
		out.printOutln(helpOption("-printsite",
				"split the number of jobs according to the execution site"));
		out.printOutln();
		out.printOutln(helpOption("merge",
				"collect the output of all the subjobs that have already finished"));
		out.printOutln(helpOption("kill", "kill all the subjobs"));
		out.printOutln(helpOption("resubmit",
				"resubmit all the subjobs selected"));
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
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * nonimplemented command's silence trigger, submit is never silent
	 */
	public void silent() {
		// ignore
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
	public JAliEnCommandmasterjob(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);

		try {
			if (alArguments.size() > 0) {

				{
					try {
						jobId = Integer.parseInt(alArguments.get(0));
					} catch (NumberFormatException e) {
						throw new JAliEnCommandException();
					}
				}

				final OptionParser parser = new OptionParser();

				parser.accepts("status").withRequiredArg();
				parser.accepts("id").withRequiredArg();
				parser.accepts("site").withRequiredArg();

				parser.accepts("printid");
				parser.accepts("printsite");

				final OptionSet options = parser.parse(alArguments
						.toArray(new String[] {}));

				if (options.has("status") && options.hasArgument("status")) {
					final StringTokenizer st = new StringTokenizer(
							(String) options.valueOf("status"), ",");
					while (st.hasMoreTokens()) {
						String state = st.nextToken();
						status.add(state);
						if ("ERROR_ALL".equals(state))
							status.addAll(new ArrayList<String>(Arrays.asList(
									"EXPIRED", "ERROR_IB", "ERROR_V",
									"ERROR_E", "FAILED", "ERROR_SV", "ERROR_A",
									"ERROR_VN")));
					}
				}

				if (options.has("id") && options.hasArgument("id")) {
					final StringTokenizer st = new StringTokenizer(
							(String) options.valueOf("id"), ",");
					while (st.hasMoreTokens())
						try {
							id.add(Integer.parseInt(st.nextToken()));
						} catch (NumberFormatException e) {
							// ignore
						}
				}

				if (options.has("site") && options.hasArgument("site")) {
					final StringTokenizer st = new StringTokenizer(
							(String) options.valueOf("site"), ",");
					while (st.hasMoreTokens())
						sites.add(st.nextToken());
				}

				bPrintId = options.has("printid");
				bPrintSite = options.has("printsite");

				String flag = alArguments.get(alArguments.size() - 1);

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
		} catch (OptionException e) {
			printHelp();
			throw e;
		}

	}

}
