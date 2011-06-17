package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

import alien.api.catalogue.CatalogueApiUtils;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.perl.commands.AlienTime;

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
	public void execute() {

		if(alArguments.size()<2 || alArguments.size()>3){
			printHelp();
		return;
	}
		int flags = 0;
		try{
			if(alArguments.size()==3)
				flags = Integer.parseInt(alArguments.get(2));
		} catch(NumberFormatException e){}

				lfns = CatalogueApiUtils.find(FileSystemUtils
						.getAbsolutePath(commander.user.getName(),
								commander.getCurrentDir().getCanonicalName(),alArguments.get(0)), alArguments.get(1), flags);
		
		if(lfns!=null && !silent){
			for (LFN lfn : lfns){
				out.printOutln(lfn.getCanonicalName());
			}
		}
		
	}

	/**
	 * printout the help info
	 */
	public void printHelp() {

		out.printOutln(AlienTime.getStamp() + "Usage: find <path> <pattern>  flags ");
		out.printOutln("Possible flags are:");
		out.printOutln("		coming soon");
	}

	/**
	 * get cannot run without arguments
	 * 
	 * @return <code>false</code>
	 */
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * the command's silence trigger
	 */
	private boolean silent = false;

	/**
	 * set command's silence trigger
	 */
	public void silent() {
		silent = true;
	}

	/**
	 * Constructor needed for the command factory in commander
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandfind(JAliEnCOMMander commander, UIPrintWriter out, final ArrayList<String> alArguments){
		super(commander, out, alArguments);
	}

}
