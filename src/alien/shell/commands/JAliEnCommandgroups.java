package alien.shell.commands;

import java.util.Set;
import java.util.ArrayList;

import joptsimple.OptionException;

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
		out.printOutln("User: " + username + roles.toString());
	}

}
