package alien.shell.commands;

import java.util.ArrayList;
import alien.quotas.QuotaUtilities;
import alien.quotas.Quota;
import java.util.Arrays;

import joptsimple.OptionException;

public class JAliEnCommandjquota extends JAliEnBaseCommand {
	private boolean isAdmin;
	private String command;
	private final ArrayList<String> allowed_fields = new ArrayList<String>( 
								Arrays.asList( 
											"maxUnfinishedJobs", 
											"maxTotalCpuCost", 
											"maxTotalRunningTime" ) );
	
	@Override
	public void run() { 
		if( !this.command.equals("list") && !(isAdmin && this.command.equals("set") ) ){
			out.printErrln("Wrong command passed" );
			printHelp();
			return;
		}
		
		String username = commander.user.getName();
		
		if( command.equals("list") ){
			Quota q = commander.q_api.getJobsQuota();
			if( q == null ){
				out.printErrln("No jobs quota found for user " + username );
				return;
			}
			out.printOutln( q.toString() );
			return;
		}
		
		if( command.equals("set") ){
			;
		}						
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
		this.isAdmin = commander.getUser().canBecome("admin");
		if( alArguments.size() > 0 )
			this.command = alArguments.get(0);
	}

}
