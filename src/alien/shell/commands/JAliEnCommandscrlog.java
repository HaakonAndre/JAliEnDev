package alien.shell.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import alien.perl.commands.AlienTime;

/**
 * @author ron
 * @since June 8, 2011
 */
public class JAliEnCommandscrlog extends JAliEnBaseCommand {

	/**
	 * marker for -c argument
	 */
	private boolean bC = false;

	/**
	 * the HashMap for the log screens
	 */
	private static HashMap<Integer, List<String>> scrlogs = new HashMap<Integer, List<String>>(
			10);

	private Integer logno = new Integer(-1);

	/**
	 * execute the sclog
	 */
	@Override
	public void run() {
		if (logno.intValue() != -1) {
			if (bC)
				scrlogs.put(logno, new ArrayList<String>());
			else if (scrlogs.get(logno) != null) {
				System.out.println(":" + logno + " [screenlog pasting]");
				for (String logline : scrlogs.get(logno)) {
					System.out.println(logline);
				}
			}
			else
				System.out.println(":" + logno + " [screenlog is empty]");
		}
	}

	/**
	 * get the directory listing of the ls
	 * @param logno 
	 * @param line 
	 * 
	 */
	protected static void addScreenLogLine(Integer logno, String line) {
		if (scrlogs.get(logno) == null)
			scrlogs.put(logno, new ArrayList<String>());
		// ArrayList<String> buf = (ArrayList<String>) scrlogs.get(logno);
		// buf.add(line);
		// scrlogs.put(logno,buf);
		scrlogs.get(logno).add(line);
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		System.out.println(AlienTime.getStamp() + "Usage: scrlog [-c] <no>");
		System.out
				.println("You have 0-9 log screens, that you can fill and display");
		System.out
				.println("call '<command> &<no>' to log <command> to screen <no> in background");
		System.out.println("default '<command> &' will go to numer 0");
		System.out.println("scrlog <no> to display the log");
		System.out.println("scrlog -c <no> to clear the log");
		System.out.println("scrlog -c <no> will clear log number 0");
	}

	/**
	 * ls can run without arguments
	 * 
	 * @return <code>true</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

	/**
	 * Constructor needed for the command factory in commander
	 * 
	 * out.printOutln( the arguments of the command
	 * @param commander 
	 * @param out 
	 * @param alArguments 
	 * @throws OptionException 
	 */
	public JAliEnCommandscrlog(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException{
		super(commander, out, alArguments);

		try {

			final OptionParser parser = new OptionParser();
			parser.accepts("c");

			final OptionSet options = parser.parse(alArguments
					.toArray(new String[] {}));

			if (options.nonOptionArguments().size() != 1)
				printHelp();
			else
				try {
					logno = Integer.valueOf(options.nonOptionArguments().get(0));
				} catch (NumberFormatException n) {
					//ignore
				}

			bC = options.has("c");

			if (logno.intValue() > 9)
				printHelp();
		} catch (OptionException e) {
			printHelp();
			throw e;
		}
	}

}
