package alien.shell.commands;

import java.util.ArrayList;
import alien.quotas.QuotaUtilities;
import alien.quotas.Quota;

import joptsimple.OptionException;

public class JAliEnCommandjquota extends JAliEnBaseCommand {

	@Override
	public void run() { 
		String username = commander.user.getName();
		System.out.println("________" + username);
		Quota q = QuotaUtilities.getJobQuota( username );		
		if( q == null ){
			out.printErrln("No jobs quota found for user " + username );
			return;
		}
		out.setField( "user", new String( q.user ) );
		out.setField( "priority", Float.toString( q.priority ) );
		out.setField( "maxParallelJobs", Float.toString( q.maxparallelJobs ) );
		out.setField( "computed priority", Float.toString( q.computedpriority ) );
		out.setField( "maxUnfinishedJobs", Float.toString( q.maxUnfinishedJobs ) );
		out.setField( "maxTotalRunningTime", Float.toString( q.maxTotalRunningTime ) );
	}

	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln(helpUsage("jquota",""));		
		out.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}
	
	public JAliEnCommandjquota(JAliEnCOMMander commander, UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
	}

}
