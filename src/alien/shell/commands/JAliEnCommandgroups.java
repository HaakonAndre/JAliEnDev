package alien.shell.commands;

import java.util.Set;
import java.util.ArrayList;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class JAliEnCommandgroups extends JAliEnBaseCommand {

	@Override
	public void run() {
	}

	@Override
	public void printHelp() {
		out.printOutln("groups: shows the groups current user is a member of.");
	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}
	
	public JAliEnCommandgroups(JAliEnCOMMander commander, UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
		Set<String> roles = commander.getUser().getRoles();
		String username = commander.getUser().getName();
		String maingroup = commander.getUser().userRole();
		out.printOutln("User: " + username + ", main group: " + maingroup);
		out.printOut("Member of groups: ");
		for( String role : roles ){
			out.printOut( role + " " );
		}
		out.printOutln();
	}

}
