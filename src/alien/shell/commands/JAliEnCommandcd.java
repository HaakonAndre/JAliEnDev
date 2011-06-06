package alien.shell.commands;

import java.util.ArrayList;

import alien.api.catalogue.CatalogueApiUtils;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.user.UsersHelper;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandcd extends JAliEnBaseCommand {

	public void execute() {

		LFN newDir = null;

		if (alArguments.size() > 0)
			newDir = CatalogueApiUtils.getLFN(FileSystemUtils.getAbsolutePath(
					JAliEnCOMMander.user.getName(),
					JAliEnCOMMander.getCurrentDir().getCanonicalName(),
					alArguments.get(0)));
		else
			newDir = CatalogueApiUtils.getLFN(UsersHelper
					.getHomeDir(JAliEnCOMMander.user.getName()));

		if (newDir != null)
			JAliEnCOMMander.curDir = newDir;
		else
			System.err.println("No such directory.");

	}

	/**
	 * printout the help info, none for this command
	 */
	public void printHelp() {

	}

	/**
	 * cd can run without arguments 
	 * @return <code>true</code>
	 */
	public boolean canRunWithoutArguments() {
		return true;
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
	public JAliEnCommandcd(final ArrayList<String> alArguments){
		super(alArguments);
	}
}
