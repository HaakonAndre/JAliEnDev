package alien.shell.commands;

import java.util.ArrayList;

import alien.quotas.FileQuota;
import alien.quotas.Quota;
import joptsimple.OptionException;

// TODO: check user permissions
// TODO: use effective user on server side

/**
 *
 */
public class JAliEnCommandjquota extends JAliEnBaseCommand {
	private boolean isAdmin;
	private String command;
	// FIXME the user is ignored and the value is applied to the default role
	private String user_to_set;
	private String param_to_set;
	private Long value_to_set;

	@Override
	public void run() {
		if (!this.command.equals("list") && !(isAdmin && this.command.equals("set"))) {
			out.printErrln("Wrong command passed");
			printHelp();
			return;
		}

		final String username = commander.user.getName();

		if (command.equals("list")) {
			final Quota q = commander.q_api.getJobsQuota();
			if (q == null) {
				out.printErrln("No jobs quota found for user " + username);
				return;
			}
			out.printOutln(q.toString());
			return;
		}

		if (command.equals("set")) {
			if (this.param_to_set == null) {
				out.printErrln("Error in parameter name");
				return;
			}
			if (this.value_to_set == null || this.value_to_set.intValue() == 0) {
				out.printErrln("Error in value");
				printHelp();
				return;
			}
			// run the update
			if (commander.q_api.setJobsQuota(this.param_to_set, this.value_to_set.toString()))
				out.printOutln("Result: ok, " + this.param_to_set + "=" + this.value_to_set.toString() + " for user=" + username);
			else
				out.printOutln("Result: failed to set " + this.param_to_set + "=" + this.value_to_set.toString() + " for user=" + username);
		}
	}

	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln("jquota: Displays information about Job Quotas.");
		out.printOutln("Usage:");
		out.printOutln("  jquota list <user>                - list the user quota for job");
		out.printOutln("                                     use just 'jquota list' for all users");
		out.printOutln();
		out.printOutln("  jquota set <user> <field> <value> - set the user quota for job");
		out.printOutln("                                      (maxUnfinishedJobs, maxTotalCpuCost, maxTotalRunningTime)");
		out.printOutln("                                      use <user>=% for all users");
	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * @param commander
	 * @param out
	 * @param alArguments
	 * @throws OptionException
	 */
	public JAliEnCommandjquota(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
		this.isAdmin = commander.getUser().canBecome("admin");
		if (alArguments.size() == 0)
			return;
		this.command = alArguments.get(0);
		System.out.println(alArguments);
		if (this.command.equals("set") && alArguments.size() == 4) {
			this.user_to_set = alArguments.get(1);
			final String param = alArguments.get(2);
			if (FileQuota.canUpdateField(param))
				return;
			this.param_to_set = param;
			try {
				this.value_to_set = Long.valueOf(alArguments.get(3));
			} catch (@SuppressWarnings("unused") final Exception e) {
				// FIXME invalid numeric values are ignored
			}
		}
	}

}
