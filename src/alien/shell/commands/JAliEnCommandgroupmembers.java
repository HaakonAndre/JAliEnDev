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
			commander.printErrln("No group name passed");
			return;
		}
		final Set<String> users = commander.q_api.getGroupMembers(this.group);
		commander.printOut("Members of " + this.group + ": ");
		for (final String user : users)
			commander.printOut(user + " ");
		commander.printOutln();
	}

	@Override
	public void printHelp() {
		commander.printOutln("groupmembers <group> : displays group members ");
		commander.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * @param commander
	 * @param alArguments
	 */
	public JAliEnCommandgroupmembers(final JAliEnCOMMander commander, final ArrayList<String> alArguments) {
		super(commander, alArguments);
		if (alArguments.size() == 1)
			this.group = alArguments.get(0);
	}

}
