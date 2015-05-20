package alien.shell.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import alien.catalogue.FileSystemUtils;
import alien.quotas.FileQuota;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
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
	private boolean recursive;
	
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
		HashMap<String, Boolean> results = commander.c_api.chownLFN( path, this.user, this.group, this.recursive );		
		
		if( results == null ){
			out.printErr("Failed to chown file(s)");
			return;
		}
		
		for( String file : results.keySet() ){
			if(!results.get(file))
				out.printErrln( file + ": unable to chown");
		}
	}

	@Override
	public void printHelp() {
		out.printOutln();		
		out.printOutln( "Usage: chown -R <user>[.<group>] <file>" );
		out.printOutln();
		out.printOutln( "Changes an owner or a group for a file" );
		out.printOutln( "-R : do a recursive chown" );
		out.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {		
		return false;
	}
	
	public JAliEnCommandchown(JAliEnCOMMander commander, UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
		try{
			final OptionParser parser = new OptionParser();		
			parser.accepts("R");		
	
			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));
	
			List<String> params = optionToString(options.nonOptionArguments());
			
			this.recursive = options.has("R");
			
			// check 2 arguments
			if( params.size() != 2 )
				return;
			
			// get user/group				
			String[] usergrp = params.get(0).split("\\.");
			System.out.println(usergrp);
			this.user = usergrp[0];
			if( usergrp.length == 2 )
				this.group = usergrp[1];
			
			// get file
			this.file = params.get(1);
		}catch(OptionException e) {
			//printHelp();
			throw e;
		}
	}

}
