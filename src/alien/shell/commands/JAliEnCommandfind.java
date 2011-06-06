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
						.getAbsolutePath(JAliEnCOMMander.user.getName(),
								JAliEnCOMMander.getCurrentDir().getCanonicalName(),alArguments.get(0)), alArguments.get(1), flags);
		
		if(lfns!=null && !silent){
			for (LFN lfn : lfns){
				System.out.println(lfn.getCanonicalName());
			}
		}
		
	}

	/**
	 * printout the help info
	 */
	public void printHelp() {

		System.out.println(AlienTime.getStamp() + "Usage: find <path> <pattern>  flags ");
		System.out.println("Possible flags are:");
		System.out.println("		coming soon");
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
	 * Constructor needed for the command factory in JAliEnCOMMander
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandfind(final ArrayList<String> alArguments) {
		super(alArguments);
	}

}
