package alien.shell.commands;

import java.util.ArrayList;

import alien.perl.commands.AlienTime;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandtime extends JAliEnBaseCommand {

	@Override
	public void run() {

		if (alArguments.size() < 2) {
			printHelp();
			return;
		}

		final ArrayList<String> args = new ArrayList<>();
		args.addAll(alArguments);
		int times = 0;
		try {
			times = Integer.parseInt(alArguments.get(0));
			args.remove(alArguments.get(0));
		} catch (@SuppressWarnings("unused") final NumberFormatException e) {
			printHelp();
		}

		final StringBuilder command = new StringBuilder(alArguments.get(1));
		args.remove(alArguments.get(1));

		JAliEnBaseCommand comm = null;
		try {
			comm = JAliEnCOMMander.getCommand(command.toString(), new Object[] { commander, out, args });
		} catch (final Exception e) {
			e.printStackTrace();
			return;
		}
		// comm.silent();

		final ArrayList<Long> timings = new ArrayList<>(times);

		for (int t = 0; t < times; t++) {
			final long lStart = System.currentTimeMillis();

			comm.run();
			timings.add(Long.valueOf(System.currentTimeMillis() - lStart));
		}

		long total = 0;

		for (final Long t : timings)
			total += t.longValue();

		for (final String s : args)
			command.append(' ').append(s);
		if (out.isRootPrinter())

			out.setField("value", "\"" + command + "\" with #" + times + " tries,\tavr/total msec:\t\t" + (total / times) + "/" + total);

		else
			out.printOutln("\"" + command + "\" with #" + times + " tries,\tavr/total msec:\t\t" + (total / times) + "/" + total);
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {

		out.printOutln(AlienTime.getStamp() + "Usage: time <times>  <command> [command_arguments] ");
	}

	/**
	 * cat cannot run without arguments
	 *
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * Constructor needed for the command factory in commander
	 *
	 * @param commander
	 * @param out
	 *
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandtime(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) {
		super(commander, out, alArguments);
	}
}
