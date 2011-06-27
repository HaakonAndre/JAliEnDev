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

		JAliEnCommandget get = (JAliEnCommandget) JAliEnCOMMander.getCommand(
				"get", new Object[] { commander, out, alArguments });
		get.silent();
		get.execute();
		File fout = get.getOutputFile();
		if (fout != null && fout.isFile() && fout.canRead()) {
			FileInputStream fstream = new FileInputStream(fout);

			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String file = "";
			String line;
			while ((line = br.readLine()) != null) {
				file += line+"\n";
			}

			out.printOutln(file);

		} else
			out.printErrln("Not able to get the file.");
	}

	/**
	 * printout the help info
	 */
	public void printHelp() {

		out.printOutln(AlienTime.getStamp() + "Usage: cat  ... ");
		out.printOutln("		-g : get file by GUID");
		out.printOutln("		-s : se,se2,!se3,se4,!se5");
		out.printOutln("		-o : outputfilename");
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
	 * nonimplemented command's silence trigger, cat is never silent
	 */
	public void silent() {

	}

	/**
	 * Constructor needed for the command factory in JAliEnCOMMander
	 * @param commander 
	 * @param out 
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandcat(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) {
		super(commander, out, alArguments);
	}
}
