package alien.shell.commands;

import java.util.ArrayList;
import java.util.Map;

import joptsimple.OptionException;
import alien.api.taskQueue.GetUptime.UserStats;
import alien.api.taskQueue.TaskQueueApiUtils;

/**
 * @author ron
 * @since Oct 27, 2011
 */
public class JAliEnCommanduptime extends JAliEnBaseCommand {

	@Override
	public void run() {
		final Map<String, UserStats> stats = TaskQueueApiUtils.getUptime();
		
		if (stats==null)
			return;
		
		final UserStats totals = new UserStats();
		
		for (final UserStats u: stats.values()){
			totals.add(u);
		}
		
		out.printOutln(totals.runningJobs+" running jobs, "+totals.waitingJobs+" waiting jobs, "+stats.size()+" active users");
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln(helpUsage("uptime", ""));
		out.printOutln(helpStartOptions());
		out.printOutln();
	}

	/**
	 * mkdir cannot run without arguments
	 * 
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

	/**
	 * Constructor needed for the command factory in commander
	 * 
	 * @param commander
	 * @param out
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommanduptime(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);

	}
}
