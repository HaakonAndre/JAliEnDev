package alien.shell.commands;

import java.util.ArrayList;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class JAliEnCommandmd5sum extends JAliEnBaseCommand {
	private ArrayList<String> alPaths = null;
	
	@Override
	public void run() {
		for( String lfnName : this.alPaths ){
			LFN lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(
					commander.user.getName(), commander.getCurrentDir()
							.getCanonicalName(), lfnName));
		}

	}

	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln(helpUsage("md5sum","<filename1> [<filename2>] ..."));		
		out.printOutln();

	}

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
	public JAliEnCommandmd5sum(JAliEnCOMMander commander, UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
		
		final OptionParser parser = new OptionParser();					
		final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));
		
		alPaths = new ArrayList<>(options.nonOptionArguments().size());
		alPaths.addAll(optionToString(options.nonOptionArguments()));
	}
	
}
