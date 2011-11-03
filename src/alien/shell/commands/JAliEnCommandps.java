package alien.shell.commands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import alien.shell.ShellColor;
import alien.taskQueue.Job;
import alien.taskQueue.JobStatus;

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
	 * id of the job to get the JDL for
	 */
	private int getJDL = 0;

	/**
	 * id of the job to get the trace for
	 */
	private int getTrace = 0;

	private List<JobStatus> states = new ArrayList<JobStatus>();

	private List<String> users = new ArrayList<String>();

	private List<String> sites = new ArrayList<String>();

	private List<String> nodes = new ArrayList<String>();

	private List<String> mjobs = new ArrayList<String>();

	private List<String> jobid = new ArrayList<String>();

	private String orderByKey = "queueId";

	private int limit = 0;

	public void run() {
		
		if (getJDL != 0) {
			String jdl = commander.q_api.getJDL(getJDL);
			if (jdl != null){
				if(bColour)
					out.printOutln( ShellColor.jobStateRed() + jdl + ShellColor.reset());
				else
					out.printOutln(jdl);
			}
		} else if (getTrace != 0) {
			String tracelog = commander.q_api.getTraceLog(getTrace);
			if (tracelog != null)
				if(bColour)
					out.printOutln(ShellColor.jobStateBlue() + tracelog + ShellColor.reset());
				
			out.printOutln("--- not implemented yet ---");
			
		} else {

			if (users.size() == 0)
				users.add(commander.getUsername());

			List<Job> ps = commander.q_api.getPS(states, users, sites, nodes,
					mjobs, jobid, orderByKey, limit);

			if (ps != null) {
				for (Job j : ps) {

					String owner = (j.getOwner() != null) ? j.getOwner() : "";
					
					String jId = bColour ? ShellColor.bold() + j.queueId +
							ShellColor.reset() : String.valueOf(j.queueId);

					String name = (j.name != null) ? j.name.substring(j.name
							.lastIndexOf('/') + 1) : "";

					if (bL) {
						String site = (j.site != null) ? j.site : "";
						String node = (j.node != null) ? j.node : "";
						out.printOutln(padLeft(String.valueOf(owner), 10)
								+ padSpace(4)
								+ padLeft(jId, 10)
								+ padSpace(2)
								+ printPriority(j.status(),j.priority)
								+ padSpace(2)
								+ padLeft(String.valueOf(site), 38)
								+ padSpace(2)
								+ padLeft(String.valueOf(node), 40)
								+ padSpace(2)
								+ abbrvStatus(j.status())
								+ padSpace(2)
								+ padLeft(String.valueOf(name), 30));
					} else
						out.printOutln(padLeft(String.valueOf(owner), 10)
								+ padSpace(1)
								+ padLeft(jId, 10)
								+ padSpace(2)
								+ printPriority(j.status(),j.priority)
								+ padSpace(2)
								+ abbrvStatus(j.status())
								+ padSpace(2)
								+ padLeft(String.valueOf(name), 32));

				}
			}
		}
	}

	private String printPriority(final JobStatus status, final int priority) {

		if (JobStatus.INSERTING == status || JobStatus.WAITING ==status) {
			if (bColour) {
				String cTag = "";
				if (priority <= 0)
					cTag = ShellColor.jobStateBlueError();
				else if (priority < 70)
					cTag = ShellColor.jobStateBlue();
				else
					cTag = ShellColor.jobStateGreen();
				return cTag + padLeft( String.valueOf(priority),3) + ShellColor.reset();
			}
			return padLeft( String.valueOf(priority),3) ;
		}
		return "___";
	}

	private String abbrvStatus(JobStatus status) {
		
		if(status==null)
			return padLeft("?",3);

		if (JobStatus.KILLED == status) {
			System.out.println("status is indeed: " + status);
			if (bColour)
				return ShellColor.jobStateRed() + padLeft("  K",3) + ShellColor.reset();
			return padLeft("  K",3);
		} else if (JobStatus.RUNNING == status) {
			if (bColour)
				return ShellColor.jobStateGreen() + padLeft("  R",3) + ShellColor.reset();
			return padLeft("  R",3);
		} else if (JobStatus.STARTED == status) {
			if (bColour)
				return ShellColor.jobStateGreen() + padLeft(" ST",3) + ShellColor.reset();
			return padLeft(" ST",3);
		} else if (JobStatus.DONE == status) {
			return padLeft("  D",3);
		} else if (JobStatus.WAITING == status) {
			if (bColour)
				return ShellColor.jobStateBlue() + padLeft("  W",3) + ShellColor.reset();
			return padLeft("  W",3);
		} else if (JobStatus.OVER_WAITING == status) {
			return padLeft("  OW",3);
		} else if (JobStatus.EXPIRED == status) {
			return padLeft(" XP",3);
		} else if (JobStatus.INSERTING == status) {
			if (bColour)
				return ShellColor.jobStateYellow() + padLeft("  I",3) + ShellColor.reset();
			return padLeft("  I",3);
		} else if (JobStatus.SPLIT == status)
			return padLeft("  S",3);
		else if (JobStatus.SPLITTING == status)
			return padLeft(" SP",3);
		else if (JobStatus.SAVING == status) {
			if (bColour)
				return ShellColor.jobStateGreen() + padLeft(" SV",3) + ShellColor.reset();
			return padLeft(" SV",3);
		} else if (JobStatus.SAVED == status)
			return padLeft("SVD",3);
		else {
			String e = "";
			
			switch (status){
				case ERROR_A  : e = " EQ"; break;
				case ERROR_E  : e = " EE"; break;
				case ERROR_I  : e = " EI"; break;
				case ERROR_IB : e = "EIB"; break;
				case ERROR_M  : e = " EM"; break;
				case ERROR_RE : e = "ERE"; break;
				case ERROR_S  : e = " ES"; break;
				case ERROR_SV : e = "ESV"; break;
				case ERROR_V  : e = " EV"; break;
				case ERROR_VN : e = "EVN"; break;
				case ERROR_VT : e = "EVT"; break;
				case ERROR_SPLT:e = "ESP"; break;
				case ERROR_W  : e = " EW"; break;
				case FAILED   : e = " FF"; break;
				case ZOMBIE   : e = "  Z"; break;
				default: e = status.toString();
			}
			
			if (bColour)
				return ShellColor.jobStateRedError() + padLeft(e,3) + ShellColor.reset();
			
			return padLeft(e,3);
		}

	}

	/**
	 * printout the help info
	 */
	public void printHelp() {

		out.printOutln();
		out.printOutln(helpUsage("ps", "[-options]"));
		out.printOutln(helpStartOptions());
		out.printOutln(helpOption("-F l | -Fl | -L", "long output format"));
		out.printOutln(helpOption("-f <flags|status>"));
		out.printOutln(helpOption("-u <userlist>"));
		out.printOutln(helpOption("-s <sitelist>"));
		out.printOutln(helpOption("-n <nodelist>"));
		out.printOutln(helpOption("-m <masterjoblist>"));
		out.printOutln(helpOption("-o <sortkey>"));
		out.printOutln(helpOption("-j <jobidlist>"));
		out.printOutln(helpOption("-l <query-limit>"));

		out.printOutln();
		out.printOutln(helpOption("-M", "show only masterjobs"));
		out.printOutln(helpOption("-X", "active jobs in extended format"));
		out.printOutln(helpOption("-A", "select all owned jobs of you"));
		out.printOutln(helpOption("-W",
				"select all jobs which are waiting for execution of you"));
		out.printOutln(helpOption("-E",
				"select all jobs which are in error state of you"));
		out.printOutln(helpOption("-a", "select jobs of all users"));
		out.printOutln(helpOption("-b", "do only black-white output"));
		out.printOutln(helpOption("-jdl <jobid>", "display the job jdl"));
		out.printOutln(helpOption("-trace <jobid> <tag>*",
				"display the job trace information (not working yet!)")); // TODO:
		out.printOutln();

	}

	/**
	 * cat cannot run without arguments
	 * 
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

	/**
	 * nonimplemented command's silence trigger, submit is never silent
	 */
	@Override
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
			parser.accepts("Fl");
			parser.accepts("L");
			
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
					decodeFlagsAndStates((String) options.valueOf("f"));
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
					states.add(JobStatus.ANY);
				}

				if (options.has("n") && options.hasArgument("n")) {
					final StringTokenizer st = new StringTokenizer(
							(String) options.valueOf("n"), ",");
					while (st.hasMoreTokens())
						nodes.add(st.nextToken());
					states.add(JobStatus.ANY);
				}

				if (options.has("m") && options.hasArgument("m")) {
					final StringTokenizer st = new StringTokenizer(
							(String) options.valueOf("m"), ",");
					while (st.hasMoreTokens())
						mjobs.add(st.nextToken());
					states.add(JobStatus.ANY);
				}

				if (options.has("j") && options.hasArgument("j")) {
					final StringTokenizer st = new StringTokenizer(
							(String) options.valueOf("j"), ",");
					while (st.hasMoreTokens())
						jobid.add(st.nextToken());
					states.add(JobStatus.ANY);
					users.add("%");
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

				if (options.has("X")){
					bL = true;
					states.addAll(flag_r());
					states.addAll(flag_s());
				}
					
					
				if(options.has("Fl") || options.has("L") || (options.has("F") && "l".equals(options.valueOf("F"))) )
					bL = true;
				

				if ((options.has("o") && options.hasArgument("o")))
					orderByKey = (String) options.valueOf("o");

//
//		        case 'M':
//		          st_masterjobs = "\\\\0";

				if (options.has("A")) {
					states.add(JobStatus.ANY);
					users.add(commander.getUsername());
				}
				if (options.has("E")) {
					states.addAll(flag_f());
					users.add(commander.getUsername());
				}
				if (options.has("W")) {
					states.addAll(flag_s());
					users.add(commander.getUsername());
				}

				if(options.has("M"))
					mjobs.add("0");


				if (options.has("a")) {
					users.add("%");
				}
			}

			if(options.has("b"))
				bColour = false;

		} catch (OptionException e) {
			printHelp();
			throw e;
		}
	}

	private void decodeFlagsAndStates(final String line) {
		
		if(line==null || line.length()<1)
			return;

		boolean all = false;

		if (line.startsWith("-")) {
			if ((line.length() == 1) || ("-a".equals(line)))
				all = true;
			else{
				char[] flags = line.toCharArray();
				for (char f: flags) {
					if(f=='r'){
						all = true;
						break;
					}else if(f=='r')
						states.addAll(flag_r());
					else if(f=='q')
						states.addAll(flag_q());
					else if(f=='f')
						states.addAll(flag_f());
					else if(f=='d')
						states.addAll(flag_d());
					else if(f=='t')
						states.addAll(flag_t());
					else if(f=='s')
						states.addAll(flag_s());
				}
			}
		} else {
			final StringTokenizer st = new StringTokenizer(line, ",");
			while (st.hasMoreTokens()) {
				String o = st.nextToken();
				if (o.length() < 1)
					continue;
				
				if ("%".equals(o)) {
					all = true;
					break;
				}
				
				states.add(JobStatus.getStatus(o));
				System.out.println("added status: " + states);
			}
		}
		
		if (all)
			states = Arrays.asList(JobStatus.ANY);
	}

	private static Set<JobStatus> flag_f() {
		return JobStatus.errorneousStates();
	}

	private static Set<JobStatus> flag_d() {
		return JobStatus.doneStates();
	}

	private static Set<JobStatus> flag_t() {
		return JobStatus.finalStates();
	}

	private static Set<JobStatus> flag_s() {
		return JobStatus.waitingStates();
	}

	private static Set<JobStatus> flag_q() {
		return JobStatus.queuedStates();
	}

	private static Set<JobStatus> flag_r() {
		return JobStatus.runningStates();
	}
}
