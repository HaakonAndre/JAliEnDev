package alien.shell.commands;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.Map;

import joptsimple.OptionException;
import alien.api.taskQueue.GetUptime.UserStats;
import alien.api.taskQueue.TaskQueueApiUtils;

/**
 * @author ron
 * @since Oct 27, 2011
 */
public class JAliEnCommandw extends JAliEnBaseCommand {

	private static final String format  = "%3d. %-20s | %12s | %12s\n";
	private static final String formatH = "     %-20s | %12s | %12s\n";
	
	private static final String separator = "--------------------------+--------------+--------------\n";
	
	@Override
	public void run() {
		final Map<String, UserStats> stats = TaskQueueApiUtils.getUptime();
		
		if (stats==null)
			return;
		
		final UserStats totals = new UserStats();
		
		final StringBuilder sb = new StringBuilder();
		
		final Formatter formatter = new Formatter(sb);
		
		formatter.format(formatH, "Account name", "Active jobs", "Waiting jobs");
		
		sb.append(separator);
		
		int i = 0;
		
		for (final Map.Entry<String, UserStats> entry: stats.entrySet()){
			final String username = entry.getKey();
			final UserStats us = entry.getValue();
			
			i++;
			
			formatter.format(format, Integer.valueOf(i), username, String.valueOf(us.runningJobs), String.valueOf(us.waitingJobs));
			
			totals.add(us);
		}
		
		sb.append(separator);
		
		formatter.format(formatH, "TOTAL", String.valueOf(totals.runningJobs), String.valueOf(totals.waitingJobs));
		
		formatter.close();
		
		out.printOut(sb.toString());
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
	public JAliEnCommandw(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);

	}
}
