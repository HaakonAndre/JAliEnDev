package alien.shell.commands;

import java.util.ArrayList;

import alien.catalogue.FileSystemUtils;
import alien.quotas.FileQuota;
import joptsimple.OptionException;
import alien.api.catalogue.ChownLFN;
import alien.catalogue.LFN;

// TODO check that the user has indeed permissions on the target entry, server side
// TODO validate that the user can become the target user name and the target role name (implicitly validating the strings)
// TODO recursive flag
// TODO multiple parameters to the command
// TODO parse groups
// FIXME passes empty group which fails the query
// TODO check that groups/users exist

public class JAliEnCommandchown extends JAliEnBaseCommand {
	private String user;
	private String group;
	private String file;
	
	@Override
	public void run() {
		if( this.user==null || this.file==null ){
			out.printErr("No user or file entered");
			return;
		}
			
		//boolean result = false;
		String path = FileSystemUtils.getAbsolutePath(
				commander.user.getName(),
				commander.getCurrentDir().getCanonicalName(),this.file);
		// run chown command
		LFN lfn = commander.c_api.chownLFN( path, this.user, this.group );		
		
		if( lfn == null )
			out.printErr("Failed to chown file");		
	}

	@Override
	public void printHelp() {
		out.printOutln( "Usage: chown <user>[.<group>] <file>" );
	}

	@Override
	public boolean canRunWithoutArguments() {		
		return false;
	}
	
	public JAliEnCommandchown(JAliEnCOMMander commander, UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
		
		// check 2 arguments
		if( alArguments.size() != 2 )
			return;
		
		// get user/group				
		String[] usergrp = alArguments.get(0).split("\\.");
		System.out.println(usergrp);
		this.user = usergrp[0];
		if( usergrp.length == 2 )
			this.group = usergrp[1];
		
		// get file
		this.file = alArguments.get(1);		
	}

}
