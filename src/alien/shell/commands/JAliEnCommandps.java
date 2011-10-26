package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import alien.perl.commands.AlienTime;
import alien.taskQueue.Job;
import alien.taskQueue.TaskQueueUtils;

/**
 * @author ron
 * @since June 9, 2011
 */
public class JAliEnCommandps extends JAliEnBaseCommand {
	
	/**
	 * marker for -r argument
	 */
	private final boolean bR;
	

	public void execute() throws Exception {
		
		List<Job> ps = TaskQueueUtils.getPS(bR);
		
		for(Job j: ps){
			System.out.println("j");
		}

	}


	/**
	 * printout the help info
	 */
	public void printHelp() {

		out.printOutln(AlienTime.getStamp() + "Usage: ps2 <flags|status> <users> <sites> <nodes> <masterjobs> <order> <jobid> <limit> <sql>");		
		out.printOutln("<flags> 	: -a all jobs");
		out.printOutln("			: -r all running jobs");
		out.printOutln("			: -f all failed/error jobs");
		out.printOutln("			: -d all done jobs");
	
/*		Usage: ps2 <flags|status> <users> <sites> <nodes> <masterjobs> <order> <jobid> <limit> <sql>
		 <flags> 	: -a all jobs
		         	: -r all running jobs
		         	: -f all failed/error jobs 
		         	: -d all done jobs 
		         	: -t all final state jobs (done/error) 
		         	: -q all queued jobs (queued/assigned) 
		         	: -s all pre-running jobs (inserting/waiting/assigned/queued/over_quota_*) 
		         	: -arfdtqs combinations
		         	: default '-' = 'all non final-states'

		 <status>	: <status-1>[,<status-N]*
		         	:  INSERTING,WAITING,OVER_WAITING,ASSIGEND,QUEUED,STARTED,RUNNING,DONE,ERROR_%[A,S,I,IB,E,R,V,VN,VT]
		         	: default '-' = 'as specified by <flags>'

		 <users> 	: <user-1>[,<user-N]*
		         	: % to wildcard all users

		 <sites> 	: <site-1>[,<site-N]*
		         	: default '%' or '-' to all sites

		 <nodes> 	: <node-1>[,<node-N]*
		         	: default '%' or '-' to all nodes

		 <mjobs> 	: <mjob-1>[,<mjob-N]*
		         	: default '%' or '-' to all jobs
		         	: <sort-key>
		         	: default '-' or 'queueId'

		 <jobid> 	: <jobid-1>[,<jobid-N]*
		         	: default '%' or '-' to use the specified <flags>

		 <limit> 	: <n> - maximum number of queried jobs
		         	: regular users: default limit = 2000;
		         	: admin        : default limit = unlimited;

		 <sql>   	: only for admin role: SQL statement
	Usage: ps2 -trace <jobid> 	: get the job trace
	Usage: ps2 -jdl   <jobid> 	: get the job JDL
*/
	
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
		
		parser.accepts("r");
		
		final OptionSet options = parser.parse(alArguments.toArray(new String[]{}));

		
		bR = options.has("r");
	}
}
