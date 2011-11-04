package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandfind extends JAliEnBaseCommand {

	private List<LFN> lfns = null;

	/**
	 * returns the LFNs that were the result of the find
	 * 
	 * @return the output file
	 */
	public List<LFN> getLFNs() {
		return lfns;
	}

	/**
	 * execute the get
	 */
	@Override
	public void run() {

		if (alArguments.size() < 2 || alArguments.size() > 3) {
			printHelp();
			return;
		}
		int flags = 0;
		try {
			if (alArguments.size() == 3)
				flags = Integer.parseInt(alArguments.get(2));
		}
		catch (NumberFormatException e) {
			// ignore
		}

		lfns = commander.c_api.find(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDir()
			.getCanonicalName(), alArguments.get(0)), alArguments.get(1), flags);

		if (lfns != null && !isSilent()) {
			for (final LFN lfn : lfns) {
				out.printOutln(lfn.getCanonicalName());
			}
		}

	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {

		out.printOutln();
		out.printOutln(helpUsage("find","<path> <pattern>  flags"));
		out.printOutln();
		out.printOutln(helpUsage("Possible flags are coming soon..."));
		
		out.printOutln();
	
	}

	/**
	 * get cannot run without arguments
	 * 
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * Constructor needed for the command factory in commander
	 * @param commander 
	 * @param out 
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandfind(JAliEnCOMMander commander, UIPrintWriter out, final ArrayList<String> alArguments){
		super(commander, out, alArguments);
	}

}
