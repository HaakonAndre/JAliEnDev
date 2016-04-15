package alien.shell.commands;

import java.util.ArrayList;
import java.util.Set;

/**
 *
 */
public class JAliEnCommandgroupmembers extends JAliEnBaseCommand {
	private String group;

	@Override
	public void run() {
		if (this.group == null || this.group.equals("")) {
			out.printErrln("No group name passed");
			return;
		}
		final Set<String> users = commander.q_api.getGroupMembers(this.group);
		out.printOut("Members of " + this.group + ": ");
		for (final String user : users)
			out.printOut(user + " ");
		out.printOutln();
	}

	@Override
	public void printHelp() {
		out.printOutln("groupmembers <group> : displays group members ");
		out.printOutln();

	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * @param commander
	 * @param out
	 * @param alArguments
	 */
	public JAliEnCommandgroupmembers(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) {
		super(commander, out, alArguments);
		if (alArguments.size() == 1)
			this.group = alArguments.get(0);
	}

}
