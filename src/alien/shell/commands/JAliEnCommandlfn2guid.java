package alien.shell.commands;

import java.util.ArrayList;
import java.util.Set;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.perl.commands.AlienTime;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandlfn2guid extends JAliEnBaseCommand {

	/**
	 * entry the call is executed on, either representing a LFN 
	 */
	private String lfnName = null;

	/**
	 * execute the lfn2guid
	 */
	public void run() {

			LFN lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(
					commander.user.getName(), commander.getCurrentDir()
							.getCanonicalName(), lfnName));
			
			if(lfn==null)
				out.printErrln("Could not get the LFN [" + lfnName + "].");
			else if(lfn.isDirectory())
				out.printErrln("The LFN is a directory [" + lfn.getCanonicalName() + "].");
			else{
				if(lfn.guid!=null)
					out.printOutln(padRight(lfn.getCanonicalName(), 80) + lfn.guid);
				else
					out.printErrln("Could not get the GUID for [" + lfn.getCanonicalName() + "].");
			}
	
	}

	/**
	 * printout the help info
	 */
	public void printHelp() {
		
		out.printOutln();
		out.printOutln(helpUsage("lfn2guid","<filename>"));
		out.printOutln();
	}

	/**
	 * lfn2guid cannot run without arguments
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
	 * @param commander 
	 * @param out 
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException 
	 */
	public JAliEnCommandlfn2guid(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException {
		super(commander, out,alArguments);
		
		if(alArguments.size()!=1)
			throw new JAliEnCommandException();
		
		lfnName = alArguments.get(0);
		
		
	}

}
