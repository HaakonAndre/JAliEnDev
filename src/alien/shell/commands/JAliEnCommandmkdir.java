package alien.shell.commands;

import java.util.ArrayList;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import alien.api.catalogue.CatalogueApiUtils;
import alien.catalogue.FileSystemUtils;
import alien.perl.commands.AlienTime;

/**
 * @author ron
 * @since June 6, 2011
 */
public class JAliEnCommandmkdir extends JAliEnBaseCommand {

	/**
	 * marker for -p argument
	 */
	private boolean bP = false;
	private ArrayList<String> alPaths = null;
	
	public void execute() {
		
		for (String path: alPaths){

			if(bP){
				if(CatalogueApiUtils.createCatalogueDirectory(commander.user, FileSystemUtils.getAbsolutePath(
						commander.user.getName(),
						commander.getCurrentDir().getCanonicalName(),path),true)==null)
					out.printErrln("Could not create directory (or non-existing parents): " + path);
			}
			else {
				if(CatalogueApiUtils.createCatalogueDirectory(commander.user, FileSystemUtils.getAbsolutePath(
						commander.user.getName(),
						commander.getCurrentDir().getCanonicalName(),path))==null)
					out.printErrln("Could not create directory: " + path);
			}
		}
	}

	/**
	 * printout the help info
	 */
	public void printHelp() {

		out.printOutln();
		out.printOutln(helpUsage("mkdir","[-options] <directory> [<directory>[,<directory>]]"));
		out.printOutln(helpStartOptions());
		out.printOutln(helpOption("-p","create parents as needed"));
		out.printOutln(helpOption("-silent","execute command silently"));
		out.printOutln();
	}

	/**
	 * mkdir cannot run without arguments
	 * 
	 * @return <code>false</code>
	 */
	public boolean canRunWithoutArguments() {
		return false;
	}


	/**
	 * nonimplemented command's silence trigger, cd is never silent
	 */
	public void silent() {
		//ignore
	}

	/**
	 * Constructor needed for the command factory in commander
	 * @param commander 
	 * @param out 
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException 
	 */
	public JAliEnCommandmkdir(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);

		try {
			final OptionParser parser = new OptionParser();

			parser.accepts("p");

			final OptionSet options = parser.parse(alArguments
					.toArray(new String[] {}));

			alPaths = new ArrayList<String>(options.nonOptionArguments().size());
			alPaths.addAll(options.nonOptionArguments());

			bP = options.has("p");
		} catch (OptionException e) {
			printHelp();
			throw e;
		}
	}
}
