package alien.shell.commands;

import java.util.ArrayList;
import java.util.StringTokenizer;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.user.UsersHelper;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandmv extends JAliEnBaseCommand {
	
	

	private String source = null;
	
	private String target = null;
	
	
	@Override
	public void run() {

		LFN sLFN = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(
					commander.user.getName(), commander.getCurrentDir()
							.getCanonicalName(), source), false);

		if(sLFN!=null){
			
			String fullTarget = FileSystemUtils.getAbsolutePath(
					commander.user.getName(), commander.getCurrentDir()
					.getCanonicalName(), target);
			
			LFN tLFN = commander.c_api.getLFN(fullTarget, false);
			
			if(tLFN==null){
			
				tLFN = commander.c_api.moveLFN(sLFN.getCanonicalName(), fullTarget);
				if (out.isRootPrinter())
					out.setReturnArgs(deserializeForRoot(1));
				
			}else{
				out.printErrln("File already exists.");
				if (out.isRootPrinter())
					out.setReturnArgs(deserializeForRoot(0));
			}
			
		}else{
			out.printErrln("No such directory.");
			if (out.isRootPrinter())
				out.setReturnArgs(deserializeForRoot(0));
		}

	}

	/**
	 * printout the help info, none for this command
	 */
	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln(helpUsage("mv"," <LFN>  <newLFN> > " +
				""));
		out.printOutln();
	}

	/**
	 * cd can run without arguments 
	 * @return <code>true</code>
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
	public JAliEnCommandmv(JAliEnCOMMander commander, UIPrintWriter out, final ArrayList<String> alArguments){
		super(commander, out,alArguments);
		try {
			final OptionParser parser = new OptionParser();

			final OptionSet options = parser.parse(alArguments
					.toArray(new String[] {}));

			if (options.nonOptionArguments().size() != 2) {
				printHelp();
				return;
			}

			source = options.nonOptionArguments().get(0);
			target = options.nonOptionArguments().get(1);
			
		} catch (OptionException e) {
			printHelp();
			throw e;
		}
	}
}
