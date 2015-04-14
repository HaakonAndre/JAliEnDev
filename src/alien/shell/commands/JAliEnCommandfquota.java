package alien.shell.commands;

import java.util.ArrayList;

import alien.quotas.FileQuota;
import joptsimple.OptionException;

public class JAliEnCommandfquota extends JAliEnBaseCommand {

	public void run() { 
		String username = commander.user.getName();
		
		//Quota q = QuotaUtilities.getJobQuota( username );
		FileQuota q = commander.q_api.getFileQuota();
		if( q == null ){
			out.printErrln("No jobs quota found for user " + username );
			return;
		}
/*		out.setField( "user", new String( q.user ) );
		out.setField( "priority", Float.toString( q.priority ) );
		out.setField( "maxParallelJobs", Float.toString( q.maxparallelJobs ) );
		out.setField( "computed priority", Float.toString( q.computedpriority ) );
		out.setField( "maxUnfinishedJobs", Float.toString( q.maxUnfinishedJobs ) );
		out.setField( "maxTotalRunningTime", Float.toString( q.maxTotalRunningTime ) );
		*/			
	}

	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln(helpUsage("fquota",""));		
		out.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}
	
	public JAliEnCommandfquota(JAliEnCOMMander commander, UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
	}
}
