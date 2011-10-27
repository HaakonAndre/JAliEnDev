package alien.shell.commands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import alien.api.taskQueue.TaskQueueApiUtils;
import alien.perl.commands.AlienTime;
import alien.taskQueue.Job;

/**
 * @author ron
 * @since June 9, 2011
 */
public class JAliEnCommandps extends JAliEnBaseCommand {

	/**
	 * marker for -l argument
	 */
	private boolean bL = false;

	/**
	 * marker for -M argument
	 */
	private boolean bM = false;

	/**
	 * id of the job to get the JDL for
	 */
	private int getJDL = 0;

	/**
	 * id of the job to get the trace for
	 */
	private int getTrace = 0;

	private List<String> states = new ArrayList<String>();

	private List<String> users = new ArrayList<String>();

	private List<String> sites = new ArrayList<String>();

	private List<String> nodes = new ArrayList<String>();

	private List<String> mjobs = new ArrayList<String>();

	private List<String> jobid = new ArrayList<String>();
	
	private String orderByKey = "queueId";

	private int limit = 0;

	public void execute() throws Exception {

		if (getJDL != 0) {
			String jdl = TaskQueueApiUtils.getJDL(getJDL);
			if (jdl != null)
				out.printOutln(jdl);
		} else if (getTrace != 0) {
			String tracelog = TaskQueueApiUtils.getTraceLog(getTrace);
			if (tracelog != null)
				out.printOutln(tracelog);
		} else {

			if (states.size() == 0)
				states.addAll(defJobStates());
			if (users.size() == 0)
				users.add(commander.getUsername());

			List<Job> ps = TaskQueueApiUtils.getPS(states, users, sites, nodes,
					mjobs, jobid, bM, orderByKey, limit);

			if (ps != null) {
				for (Job j : ps) {

					String owner = (j.getOwner() != null) ? j.getOwner() : "";

					String status = (j.status != null) ? abbrvStatus(j.status)
							: "";
					String name = (j.name != null) ? j.name.substring(j.name
							.lastIndexOf('/') + 1) : "";

					if (bL) {
						String site = (j.site != null) ? j.site : "";
						String node = (j.node != null) ? j.node : "";
						out.printOutln(padLeft(String.valueOf(owner), 10)
								+ padSpace(17)
								+ padLeft(String.valueOf(j.queueId), 10)
								+ padSpace(2) + padLeft(String.valueOf("___"), 3)
								+ padSpace(2)
								+ padLeft(String.valueOf(site), 30)
								+ padSpace(2)
								+ padLeft(String.valueOf(node), 30)
								+ padSpace(2)
								+ padLeft(String.valueOf(status), 3)
								+ padSpace(2)
								+ padLeft(String.valueOf(name), 32));
					} else
						out.printOutln(padLeft(String.valueOf(owner), 10)
								+ padSpace(1)
								+ padLeft(String.valueOf(j.queueId), 10)
								+ padSpace(2) + padLeft(String.valueOf("___"), 3)
								+ padSpace(2)
								+ padLeft(String.valueOf(status), 3)
								+ padSpace(2)
								+ padLeft(String.valueOf(name), 32));

				}
			}
		}
	}

	private static String abbrvStatus(String status) {
		if (status == null)
			return "-";
		if (status.equals("INDERTING"))
			return "I";
		if (status.equals("WAITING"))
			return "W";
		if (status.equals("EXPIRED"))
			return "EX";
		else if (status.equals("ASSIGEND"))
			return "A";
		else if (status.equals("QUEUED"))
			return "Q";
		else if (status.equals("STARTED"))
			return "S";
		else if (status.equals("RUNNING"))
			return "R";
		else if (status.equals("DONE"))
			return "D";
		else if (status.equals("ERROR_A"))
			return "EA";
		else if (status.equals("ERROR_S"))
			return "ES";
		else if (status.equals("ERROR_I"))
			return "EI";
		else if (status.equals("ERROR_IB"))
			return "EIB";
		else if (status.equals("ERROR_E"))
			return "EE";
		else if (status.equals("ERROR_R"))
			return "ER";
		else if (status.equals("ERROR_V"))
			return "EV";
		else if (status.equals("ERROR_VN"))
			return "EVN";
		else if (status.equals("ERROR_VT"))
			return "EVT";
		return status;
	}

	/**
	 * printout the help info
	 */
	public void printHelp() {
		
		out.printOutln();
		out.printOutln(helpUsage("ps","[-options]"));
		out.printOutln(helpStartOptions());
		out.printOutln(helpOption("-F {l}","(output format)"));
		out.printOutln(helpOption("-f <flags/status>"));
		out.printOutln(helpOption("-u <userlist>"));
		out.printOutln(helpOption("-s <sitelist>"));
		out.printOutln(helpOption("-n <nodelist>"));
		out.printOutln(helpOption("-m <masterjoblist>"));
		out.printOutln(helpOption("-o <sortkey>"));
		out.printOutln(helpOption("-j <jobidlist>"));
		out.printOutln(helpOption("-l <query-limit>"));
		
		out.printOutln();
		out.printOutln(helpOption("-M","show only masterjobs"));
		out.printOutln(helpOption("-X","active jobs in extended format"));
		out.printOutln(helpOption("-A","select all owned jobs of you"));
		out.printOutln(helpOption("-W","select all jobs which are waiting for execution of you"));
		out.printOutln(helpOption("-E","select all jobs which are in error state of you"));
		out.printOutln(helpOption("-D","select all done jobs of you"));
		out.printOutln(helpOption("-R","select all running jobs of you"));
		out.printOutln(helpOption("-Q","select all queued jobs of you"));
		out.printOutln(helpOption("-a","select jobs of all users"));
		out.printOutln(helpOption("-b","do only black-white output [black-white anyway, so ignored]"));
		out.printOutln(helpOption("-jdl <jobid>","display the job jdl"));
		out.printOutln(helpOption("-trace <jobid> <tag>*","display the job trace information (not working yet!)")); // TODO:

	}

	/**
	 * cat cannot run without arguments
	 * 
	 * @return <code>false</code>
	 */
	public boolean canRunWithoutArguments() {
		return true;
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
	public JAliEnCommandps(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);

		try {

			final OptionParser parser = new OptionParser();

			parser.accepts("F").withRequiredArg();
			parser.accepts("f").withRequiredArg();
			parser.accepts("u").withRequiredArg();
			parser.accepts("s").withRequiredArg();
			parser.accepts("n").withRequiredArg();
			parser.accepts("m").withRequiredArg();
			parser.accepts("o").withRequiredArg();
			parser.accepts("j").withRequiredArg();
			parser.accepts("l").withRequiredArg();
			parser.accepts("q").withRequiredArg();

			parser.accepts("M");
			parser.accepts("X");
			parser.accepts("A");
			parser.accepts("W");
			parser.accepts("E");
			parser.accepts("a");
			parser.accepts("b");
			parser.accepts("jdl").withRequiredArg();
			parser.accepts("trace").withRequiredArg();

			final OptionSet options = parser.parse(alArguments
					.toArray(new String[] {}));

			if (options.has("jdl") && options.hasArgument("jdl")) {
				try {
					getJDL = Integer.parseInt((String) options.valueOf("jdl"));
				} catch (NumberFormatException e) {
					out.printErrln("Illegal job ID.");
					getJDL = -1;
				}

			} else if (options.has("trace") && options.hasArgument("trace")) {
				try {
					getTrace = Integer.parseInt((String) options
							.valueOf("trace"));
				} catch (NumberFormatException e) {
					out.printErrln("Illegal job ID.");
					getTrace = -1;
				}

			} else {

				if (options.has("f") && options.hasArgument("f")) {
					final StringTokenizer st = new StringTokenizer(
							(String) options.valueOf("f"), ",");
					while (st.hasMoreTokens())
						states.add(st.nextToken());
				}

				if (options.has("u") && options.hasArgument("u")) {
					final StringTokenizer st = new StringTokenizer(
							(String) options.valueOf("u"), ",");
					while (st.hasMoreTokens())
						users.add(st.nextToken());
				}

				if (options.has("s") && options.hasArgument("s")) {
					final StringTokenizer st = new StringTokenizer(
							(String) options.valueOf("s"), ",");
					while (st.hasMoreTokens())
						sites.add(st.nextToken());
					states.add("%");
				}

				if (options.has("n") && options.hasArgument("n")) {
					final StringTokenizer st = new StringTokenizer(
							(String) options.valueOf("n"), ",");
					while (st.hasMoreTokens())
						nodes.add(st.nextToken());
					states.add("%");
				}

				if (options.has("m") && options.hasArgument("m")) {
					final StringTokenizer st = new StringTokenizer(
							(String) options.valueOf("m"), ",");
					while (st.hasMoreTokens())
						mjobs.add(st.nextToken());
					states.add("%");
				}

				if (options.has("j") && options.hasArgument("j")) {
					final StringTokenizer st = new StringTokenizer(
							(String) options.valueOf("j"), ",");
					while (st.hasMoreTokens())
						jobid.add(st.nextToken());
					states.add("%");
				}

				if (options.has("l") && options.hasArgument("l")) {
					try {
						int lim = Integer.parseInt((String) options
								.valueOf("l"));
						if (lim > 0)
							limit = lim;
					} catch (NumberFormatException e) {
						// ignore
					}
				}


				if (options.has("X")
						|| (options.has("F") && options.hasArgument("F") && "l"
								.equals(options.valueOf("F"))))
					bL = true;
				
				if ((options.has("o") && options.hasArgument("o")))
					orderByKey = (String) options.valueOf("o");

				if (options.has("A")) {
					states.addAll(allJobStates());
					users.add(commander.getUsername());
				}
				if (options.has("W")) {
					states.addAll(prerunJobStates());
					users.add(commander.getUsername());
				}
				if (options.has("W")) {
					states.addAll(errorJobStates());
					users.add(commander.getUsername());
				}
				if (options.has("D")) {
					states.addAll(doneJobStates());
					users.add(commander.getUsername());
				}
				if (options.has("R")) {
					states.addAll(runningJobStates());
					users.add(commander.getUsername());
				}
				if (options.has("Q")) {
					states.addAll(queuedJobStates());
					users.add(commander.getUsername());
				}
				

				bM = options.has("M");

				if (options.has("a")) {
					users.add("%");
				}
			}
		} catch (OptionException e) {
			printHelp();
			throw e;
		}
	}
	
	

	private static List<String> defJobStates() {
		return Arrays.asList(new String[] { "INSERTING", "WAITING", "ASSIGEND",
				"QUEUED", "STARTED", "RUNNING" });
	}

	private static List<String> allJobStates() {
		return Arrays
				.asList(new String[] { "INSERTING", "WAITING", "EXPIRED",
						"ASSIGEND", "QUEUED", "STARTED", "RUNNING", "DONE",
						"ERROR_%" });
	}

	private static List<String> runningJobStates() {
		return Arrays.asList(new String[] { "RUNNING" });
	}

	private static List<String> errorJobStates() {
		return Arrays.asList(new String[] { "ERROR_%" });
	}

	private static List<String> doneJobStates() {
		return Arrays.asList(new String[] { "DONE" });
	}

	private static List<String> queuedJobStates() {
		return Arrays.asList(new String[] { "QUEUED" });
	}

	private static List<String> prerunJobStates() {
		return Arrays
				.asList(new String[] { "WAITING", "INSERTING", "ASSIGNED" });
	}
}
