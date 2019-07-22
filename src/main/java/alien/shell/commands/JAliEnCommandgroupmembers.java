package alien.shell.commands;

import java.util.List;
import java.util.Set;

/**
 *
 */
public class JAliEnCommandgroupmembers extends JAliEnBaseCommand {
	private String group;

	@Override
	public void run() {
		if (this.group == null || this.group.equals("")) {
			commander.setReturnCode(1, "No group name passed");
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
	public JAliEnCommandgroupmembers(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);
		if (alArguments.size() == 1)
			this.group = alArguments.get(0);
	}

}
