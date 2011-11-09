package alien.shell.commands;

import java.io.File;
import java.util.ArrayList;

import lazyj.Utils;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandcat extends JAliEnBaseCommand {

	@Override
	public void run() {
		File fout = catFile();

		if (fout!=null && fout.exists() && fout.isFile() && fout.canRead()) {
			final String content  =  Utils.readFile(fout.getAbsolutePath());
			if (content!=null){
				out.printOutln(content);
			}
			else
				if (!isSilent())
					out.printErrln("Could not read the contents of "+fout.getAbsolutePath());
		} 
		else
			if (!isSilent())
				out.printErrln("Not able to get the file " + alArguments.get(0));
	}

	/**
	 * @return file handle for downloaded file
	 */
	public File catFile() {

		ArrayList<String> args = new ArrayList<String>(alArguments.size() + 1);
		args.add("-t");
		args.addAll(alArguments);

		JAliEnCommandcp cp;
		try {
			cp = (JAliEnCommandcp) JAliEnCOMMander.getCommand("cp",
					new Object[] { commander, out, args });
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		cp.silent();
		
		try {
			
			cp.start();
			while (cp.isAlive()) {
				Thread.sleep(500);
				if (!isSilent())
					out.pending();
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return cp.getOutputFile();
	}

			


	/**
	 * printout the help info
	 */
	@Override
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
	@Override
	public boolean canRunWithoutArguments() {
		return false;
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
