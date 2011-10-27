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
public class JAliEnCommandmasterjob extends JAliEnBaseCommand {

	/**
	 * marker for -a argument
	 */
	private boolean bA = false;

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

	private int limit = 0;

	public void execute() throws Exception {
		
		out.printErrln("--- not implemented yet ---");

	}

	/**
	 * printout the help info
	 */
	public void printHelp() {
		
		out.printOutln();
		out.printOutln(helpUsage("masterjob", "<jobId> [-status <status>] [-site] [-printid] [-id <id>] [merge|kill|resubmit]"));
		out.printOutln(helpStartOptions());
		out.printOutln(helpOption("-status <status>", "display only the subjobs with that status"));
		out.printOutln(helpOption("-id <id>","display only the subjobs with that id"));
		out.printOutln(helpOption("-site <id>","display only the subjobs on that site"));
		out.printOutln(helpOption("-printid","print also the id of all the subjobs"));
		out.printOutln(helpOption("-printsite","split the number of jobs according to the execution site"));
		out.printOutln(helpOption("merge","collect the output of all the subjobs that have already finished"));
		out.printOutln(helpOption("kill","kill all the subjobs"));
		out.printOutln(helpOption("resubmit","resubmit all the subjobs selected"));
		out.printOutln(helpOption("expunge","delete completely the subjobs"));
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

				bA = options.has("a");

				if (options.has("X")
						|| (options.has("F") && options.hasArgument("F") && "l"
								.equals(options.valueOf("F"))))
					bL = true;

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
