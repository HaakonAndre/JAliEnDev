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

		ArrayList<String> args = new ArrayList<String>(alArguments.size()+1);
		args.add("-t");
		args.addAll(alArguments);
		
		JAliEnCommandcp cp = (JAliEnCommandcp) JAliEnCOMMander.getCommand(
				"cp", new Object[] { commander, out, args });
		cp.silent();
		cp.execute();
		File fout = cp.getOutputFile();
		if(fout==null)
			return;
		
		if (fout.isFile() && fout.canRead()) {
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
		out.printOutln();
		out.printOutln(helpUsage("cat","[-options] [<filename>]"));
		out.printOutln(helpStartOptions());
		out.printOutln(helpOption("-g","get file by GUID"));
		out.printOutln(helpOption("-o","outputfilename"));
		out.printOutln();
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
