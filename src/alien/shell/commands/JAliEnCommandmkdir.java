package alien.shell.commands;

import java.util.ArrayList;

import alien.catalogue.FileSystemUtils;
import alien.perl.commands.AlienTime;
import alien.ui.api.CatalogueApiUtils;

/**
 * @author ron
 * @since June 6, 2011
 */
public class JAliEnCommandmkdir extends JAliEnBaseCommand {

	/**
	 * marker for -p argument
	 */
	private boolean bP = false;

	
	public void execute() {
		ArrayList<String> alPaths = new ArrayList<String>(alArguments.size());
		for (String arg : alArguments){
			if ("-p".equals(arg))
				bP = true;
			else
				alPaths.add(arg);
		}
		for (String path: alPaths){
			if(bP)
				if(CatalogueApiUtils.createCatalogueDirectory(JAliEnCOMMander.user, FileSystemUtils.getAbsolutePath(
						JAliEnCOMMander.user.getName(),
						JAliEnCOMMander.getCurrentDir().getCanonicalName(),path),true)==null)
					System.err.println("Could not create directory (or non-existing parents): " + path);
		
			else 
				if(CatalogueApiUtils.createCatalogueDirectory(JAliEnCOMMander.user, FileSystemUtils.getAbsolutePath(
						JAliEnCOMMander.user.getName(),
						JAliEnCOMMander.getCurrentDir().getCanonicalName(),path))==null)
					System.err.println("Could not create directory: " + path);
				
		}
	}

	/**
	 * printout the help info
	 */
	public void printHelp() {
		System.out.println(AlienTime.getStamp()
				+ "Usage: mkdir [-ps] <directory> [<directory>] ...");
		System.out.println("		-p : create parents as needed");
		System.out.println("		-s : silent");

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

	}

	/**
	 * Constructor needed for the command factory in JAliEnCOMMander
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandmkdir(final ArrayList<String> alArguments){
		super(alArguments);
	}
}
