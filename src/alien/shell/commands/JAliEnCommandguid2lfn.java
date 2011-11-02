package alien.shell.commands;

import java.util.ArrayList;
import java.util.Set;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.perl.commands.AlienTime;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandguid2lfn extends JAliEnBaseCommand {

	/**
	 * entry the call is executed on, either representing a LFN 
	 */
	private String guidName = null;

	/**
	 * execute the lfn2guid
	 */
	public void run() {

			GUID guid = commander.c_api.getGUID(guidName);
			
			if(guid==null)
				out.printErrln("Could not get the GUID [" + guidName + "].");
			else{
				// TODO: DOES NOT WORK! we don't get the LFNs
				if(guid.getLFNs()!=null && guid.getLFNs().iterator().hasNext())
					out.printOutln(padRight(guid.guid+"", 40) + guid.getLFNs().iterator().next());
				else
					out.printErrln("Could not get the GUID for [" + guid.guid + "].");
			}
	
	}

	/**
	 * printout the help info
	 */
	public void printHelp() {
		
		out.printOutln();
		out.printOutln(helpUsage("guid2lfn","<GUID>"));
		out.printOutln();
	}

	/**
	 * guid2lfn cannot run without arguments
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
	public JAliEnCommandguid2lfn(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException {
		super(commander, out,alArguments);
		
		if(alArguments.size()!=1)
			throw new JAliEnCommandException();
		
		guidName = alArguments.get(0);
		
		
	}

}
