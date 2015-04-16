package alien.shell.commands;

import java.util.ArrayList;
import java.util.Arrays;

import alien.quotas.FileQuota;
import joptsimple.OptionException;

public class JAliEnCommandfquota extends JAliEnBaseCommand {
	private boolean isAdmin;
	private String command;
	private String user_to_set;
	private String param_to_set;
	private Integer value_to_set;
	
	private final ArrayList<String> allowed_fields = new ArrayList<String>( 
			Arrays.asList( "maxNbFiles",
						"maxTotalSize" ) );
	
	public void run() {		
		if( !this.command.equals("list") && !(isAdmin && this.command.equals("set") ) ){
			out.printErrln("Wrong command passed" );
			printHelp();
			return;
		}
			
		String username = commander.user.getName();
				
		if( command.equals("list") ){
			FileQuota q = commander.q_api.getFileQuota();
			if( q == null ){
				out.printErrln("No file quota found for user " + username );
				return;
			}
			
			out.printOutln( q.toString() );
			return;
		}
		
		if( command.equals("set") ){
			if( this.param_to_set==null || 
					this.value_to_set==null || 
					this.value_to_set==0 ){
				out.printErrln("Error in parameter or value" );
				printHelp();
				return;
			}
			// run the update
		}
				
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
		if( alArguments.size() > 0 )
			this.command = alArguments.get(0);
		if( this.command.equals("set") && alArguments.size()==4 ){
			this.user_to_set = alArguments.get(1);
			String param = alArguments.get(2);
			if( !this.allowed_fields.contains( param ) )
				return;
			this.param_to_set = param;
			try{
				this.value_to_set = Integer.parseInt( alArguments.get(3) );
			}
			catch( Exception e ){}
		}
	}
}
