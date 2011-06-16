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
public class JAliEnCommandcat extends JAliEnBaseCommand {

	public void execute() throws Exception {

		ArrayList<String> args = new ArrayList<String>();
		args.addAll(alArguments);
		JAliEnCommandget get = (JAliEnCommandget) JAliEnCOMMander.getCommand(
				"get", new Object[] { args });
		get.silent();
		get.execute();
		File out = get.getOutputFile();
		if (out != null && out.isFile() && out.canRead()) {
			FileInputStream fstream = new FileInputStream(out);

			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;

			while ((strLine = br.readLine()) != null) {

				System.out.println(strLine);
			}

		}
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
	 * @return <code>false</code>
	 */
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * nonimplemented command's silence trigger, cat is never silent
	 */
	public void silent() {

	}

	/**
	 * Constructor needed for the command factory in JAliEnCOMMander
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandcat(final ArrayList<String> alArguments){
		super(alArguments);
	}
}
