package alien.shell.commands;

import java.util.ArrayList;
import java.util.Set;

import joptsimple.OptionException;

/**
 * @author costing
 */
public class JAliEnCommandgroups extends JAliEnBaseCommand {

	@Override
	public void run() {
		final Set<String> roles = commander.getUser().getRoles();
		final String username = commander.getUser().getName();
		final String maingroup = commander.getUser().getDefaultRole();
		out.printOutln("User: " + username + ", main group: " + maingroup);
		out.printOut("Member of groups: ");
		for (final String role : roles)
			out.printOut(role + " ");
		out.printOutln();
	}

	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln("groups [<username>]");
		out.printOutln("shows the groups current user is a member of.");
		out.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

	/**
	 * @param commander
	 * @param out
	 * @param alArguments
	 * @throws OptionException
	 */
	public JAliEnCommandgroups(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
	}

}
