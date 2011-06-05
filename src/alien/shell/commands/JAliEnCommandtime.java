package alien.shell.commands;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import alien.perl.commands.AlienTime;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandtime extends JAliEnBaseCommand {

	public void execute() throws Exception {

		ArrayList<String> args = new ArrayList<String>();
		args.addAll(alArguments);
		String command = alArguments.get(0);
		args.remove(alArguments.get(0));
		int times = Integer.parseInt(alArguments.get(1));
		args.remove(alArguments.get(1));
		JAliEnBaseCommand comm = (JAliEnBaseCommand) JAliEnCOMMander.getCommand(
				command, new Object[] { args });
		comm.silent();

		ArrayList<Long> timings = new ArrayList<Long>(times);
		for (int t = 0; t < times; t++) {
			long lStart = System.currentTimeMillis();

			comm.execute();
			timings.add(System.currentTimeMillis() - lStart);
		}
		long total = 0;
		for(Long t: timings)
			total += t;
		System.out.println("We executed: "+ times + " command " + command + ", avr msecs: " + (total/times) + ", total msecs:" +total);

	}

	/**
	 * printout the help info
	 */
	public void printHelp() {

		System.out.println(AlienTime.getStamp() + "Usage: cat  ... ");
		System.out.println("		-g : get file by GUID");
		System.out.println("		-s : se,se2,!se3,se4,!se5");
		System.out.println("		-o : outputfilename");
	}

	/**
	 * cat cannot run without arguments
	 * 
	 * @return <code>false</code>
	 */
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * nonimplemented command's silence trigger, cd is never silent
	 */
	public void silent() {

	}

	/**
	 * Constructor needed for the command factory in JAliEnCOMMander
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandtime(final ArrayList<String> alArguments) {
		super(alArguments);
	}
}
