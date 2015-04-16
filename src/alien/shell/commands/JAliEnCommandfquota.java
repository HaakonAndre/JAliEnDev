package alien.shell.commands;

import java.util.ArrayList;

import alien.quotas.FileQuota;
import joptsimple.OptionException;

public class JAliEnCommandfquota extends JAliEnBaseCommand {
	private boolean isAdmin;
	private String command;
	
	public void run() {
		System.out.println(this.command);
		if( !this.command.equals("list") && !(isAdmin && this.command.equals("set") ) ){
			out.printErrln("Wrong command passed" );
			printHelp();
			return;
		}
			
		String username = commander.user.getName();
		
		//Quota q = QuotaUtilities.getJobQuota( username );
		if( command.equals("list") ){
			FileQuota q = commander.q_api.getFileQuota();
			if( q == null ){
				out.printErrln("No file quota found for user " + username );
				return;
			}
			
			out.printOutln( q.toString() );
			return;
		}
		
		if( command.equals("set") ){}
		
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
		out.printOutln("fquota: Displays information about File Quotas.");
		out.printOutln(helpUsage("fquota", "list [-<options>]"));
		out.printOutln("Options:");
		out.printOutln("  -unit = B|K|M|G: unit of file size");
		if( this.isAdmin ){
			out.printOutln();
			out.printOutln("fquota set <user> <field> <value> - set the user quota for file catalogue");
			out.printOutln("  (maxNbFiles, maxTotalSize(Byte))");
			out.printOutln("  use <user>=% for all users");
		}			
		out.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}
	
	public JAliEnCommandfquota(JAliEnCOMMander commander, UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
		this.isAdmin = commander.getUser().canBecome("admin");
		this.command = alArguments.get(0);
	}
}
