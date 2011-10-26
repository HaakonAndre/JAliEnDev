package alien.shell.commands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

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
	 * marker for -a argument
	 */
	private final boolean bA;
	
	/**
	 * marker for -r argument
	 */
	private final boolean bR;
	
	/**
	 * marker for -f argument
	 */
	private final boolean bF;
	
	/**
	 * marker for -d argument
	 */
	private final boolean bD;
	
	/**
	 * marker for -t argument
	 */
	private final boolean bT;

	/**
	 * marker for -q argument
	 */
	private final boolean bQ;

	/**
	 * marker for -s argument
	 */
	private final boolean bS;
	

	private List<String> states = new ArrayList<String>();
	
	private List<String> users = new ArrayList<String>();
	
	private List<String> sites = new ArrayList<String>();
	
	private List<String> nodes = new ArrayList<String>();
	
	private List<String> mjobs = new ArrayList<String>();
	
	private List<String> jobid = new ArrayList<String>();
	
	private int  limit = 0;
	
	

	public void execute() throws Exception {
		
		if(bA)
			states.addAll(allJobStates());
		if(bR)
			states.addAll(runningJobStates());
		if(bF)
			states.addAll(errorJobStates());
		if(bD)
			states.addAll(doneJobStates());
		if(bT)
			states.addAll(finalJobStates());
		if(bQ)
			states.addAll(queuedJobStates());
		if(bS)
			states.addAll(prerunJobStates());
		
		if(states.size()==0)
			states.addAll(defJobStates());
		
		if(users.size()==0)
			users.add(commander.getUsername());


		List<Job> ps = TaskQueueApiUtils.getPS(states, users, sites, nodes, mjobs, jobid, limit);
		
		String Whatever = "  0 ";
		if(ps!=null)
			for(Job j: ps){
				out.printOutln("  "+j.getOwner() + " "+ j.queueId + "  " + Whatever+ "  "+  abbrvStatus(j.status));
			}

	}

	private static String abbrvStatus(String status){
		if(status==null)
			return "-";
		if(status.equals("INDERTING"))
			return "I";
		if(status.equals("WAITING"))
			return "W";
		else if(status.equals("ASSIGEND"))
			return "A";
		else if(status.equals("QUEUED"))
			return "Q";
		else if(status.equals("STARTED"))
			return "S";
		else if(status.equals("RUNNING"))
			return "R";
		else if(status.equals("DONE"))
			return "D";
		else if(status.equals("ERROR_A"))
			return "EA";
		else if(status.equals("ERROR_S"))
			return "ES";
		else if(status.equals("ERROR_I"))
			return "EI";
		else if(status.equals("ERROR_IB"))
			return "EIB";
		else if(status.equals("ERROR_E"))
			return "EE";
		else if(status.equals("ERROR_R"))
			return "ER";
		else if(status.equals("ERROR_V"))
			return "EV";
		else if(status.equals("ERROR_VN"))
			return "EVN";
		else if(status.equals("ERROR_VT"))
			return "EVT";
		return status;
	}
	
	

	/**
	 * printout the help info
	 */
	public void printHelp() {

		out.printOutln(AlienTime.getStamp() + "Usage: ps2 <flags|status> <users> <sites> <nodes> <masterjobs> <order> <jobid> <limit> <sql>");		
		out.printOutln("	<flags> 	: -a all jobs");
		out.printOutln("			: -r all running jobs");
		out.printOutln("			: -f all failed/error jobs");
		out.printOutln("			: -d all done jobs");
		out.printOutln("			: -t all final state jobs (done/error)");
		out.printOutln("			: -q all queued jobs (queued/assigned)");
		out.printOutln("			: -s all pre-running jobs (inserting/waiting/assigned/queued/over_quota_*)");
		out.printOutln("			: -arfdtqs combinations");
		out.printOutln("			: default '-' = 'all non final-states'");
		out.printOutln();
		out.printOutln("	<status> 	: <status-1>[,<status-N]*");
		out.printOutln("			:  INSERTING,WAITING,OVER_WAITING,ASSIGEND,QUEUED,STARTED,RUNNING,DONE,ERROR_%[A,S,I,IB,E,R,V,VN,VT]");
		out.printOutln("			: default '-' = 'as specified by <flags>'");
		out.printOutln();
		out.printOutln("	<users> 	: <user-1>[,<user-N]*");
		out.printOutln("			: % to wildcard all users");
		out.printOutln();
		out.printOutln("	<sites> 	: <site-1>[,<site-N]*");
		out.printOutln("			: default '%' or '-' to all sites");
		out.printOutln();
		out.printOutln("	<nodes> 	: <node-1>[,<node-N]*");
		out.printOutln("			: default '%' or '-' to all nodes");
		out.printOutln();
		out.printOutln("	<mjobs> 	: <mjob-1>[,<mjob-N]*");
		out.printOutln("			: default '%' or '-' to all jobs");
		out.printOutln("			: <sort-key>");
		out.printOutln("			: default '-' or 'queueId'");
		out.printOutln("	<jobid> 	: <jobid-1>[,<jobid-N]*");
		out.printOutln("			: default '%' or '-' to use the specified <flags>");
		out.printOutln();
		out.printOutln("	<limit> 	: <n> - maximum number of queried jobs");
		out.printOutln("			: regular users: default limit = 2000;");
		out.printOutln("			: admin        : default limit = unlimited;");
		out.printOutln();
		out.printOutln("Usage: ps2 -trace <jobid> 	: get the job trace");
		out.printOutln("Usage: ps2 -jdl   <jobid> 	: get the job JDL");
		out.printOutln();

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
      //ignore
	}

	/**
	 * Constructor needed for the command factory in commander
	 * @param commander 
	 * @param out 
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandps(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) {
		super(commander, out, alArguments);
		
final OptionParser parser = new OptionParser();

		parser.accepts("a");
		parser.accepts("r");
		parser.accepts("f");
		parser.accepts("d");
		parser.accepts("t");
		parser.accepts("q");
		parser.accepts("s");
		
		final OptionSet options = parser.parse(alArguments.toArray(new String[]{}));
		

		bA = options.has("a");
		bR = options.has("r");
		bF = options.has("f");
		bD = options.has("d");
		bT = options.has("t");
		bQ = options.has("q");
		bS = options.has("s");
		
		if(options.nonOptionArguments().get(0) !=null){
			final StringTokenizer st = new StringTokenizer(
					options.nonOptionArguments().get(0), ",");
			while (st.hasMoreTokens())
				states.add(st.nextToken());
		}
		
		if(options.nonOptionArguments().get(1) !=null){
			final StringTokenizer st = new StringTokenizer(
					options.nonOptionArguments().get(1), ",");
			while (st.hasMoreTokens())
				users.add(st.nextToken());
		}
		
		if(options.nonOptionArguments().get(2) !=null){
			final StringTokenizer st = new StringTokenizer(
					options.nonOptionArguments().get(2), ",");
			while (st.hasMoreTokens())
				sites.add(st.nextToken());
		}
		
		if(options.nonOptionArguments().get(3) !=null){
			final StringTokenizer st = new StringTokenizer(
					options.nonOptionArguments().get(3), ",");
			while (st.hasMoreTokens())
				nodes.add(st.nextToken());
		}
		
		if(options.nonOptionArguments().get(4) !=null){
			final StringTokenizer st = new StringTokenizer(
					options.nonOptionArguments().get(3), ",");
			while (st.hasMoreTokens())
				mjobs.add(st.nextToken());
		}
		
		if(options.nonOptionArguments().get(5) !=null){
			final StringTokenizer st = new StringTokenizer(
					options.nonOptionArguments().get(3), ",");
			while (st.hasMoreTokens())
				jobid.add(st.nextToken());
		}
		
		if(options.nonOptionArguments().get(6) !=null){
			try{
				int lim = Integer.parseInt(options.nonOptionArguments().get(6));
				if(lim>0)
					limit = lim;
			}
			catch(NumberFormatException e){
				//ignore
			}
		}

	}
	
	private static List<String> defJobStates(){
		return Arrays.asList(new String[]{"INSERTING","WAITING"
				,"ASSIGEND","QUEUED","STARTED","RUNNING"});
	}
	
	
	private static List<String> allJobStates(){
		return Arrays.asList(new String[]{"INSERTING","WAITING"
				,"ASSIGEND","QUEUED","STARTED","RUNNING","DONE","ERROR_%"});
	}
	
	private static List<String> runningJobStates(){
		return Arrays.asList(new String[]{"RUNNING"});
	}
	
	private static List<String> errorJobStates(){
		return Arrays.asList(new String[]{"ERROR_%"});
	}
	
	private static List<String> doneJobStates(){
		return Arrays.asList(new String[]{"DONE"});
	}

	private static List<String> finalJobStates(){
		return Arrays.asList(new String[]{"DONE","ERROR_%"});
	}	
	
	private static List<String> queuedJobStates(){
		return Arrays.asList(new String[]{"QUEUED"});
	}
	
	private static List<String> prerunJobStates(){
		return Arrays.asList(new String[]{"WAITING","INSERTING","ASSIGNED"});
	}
}
