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

		if (alArguments!= null && alArguments.size() > 0)
			newDir = CatalogueApiUtils.getLFN(FileSystemUtils.getAbsolutePath(
					commander.user.getName(),
					commander.getCurrentDir().getCanonicalName(),
					alArguments.get(0)));
		else
			newDir = CatalogueApiUtils.getLFN(UsersHelper
					.getHomeDir(commander.user.getName()));

		if (newDir != null){
			commander.curDir = newDir;
			out.setReturnArgs(deserializeForRoot(1));
		}
		else{
			out.printErrln("No such directory.");
			out.setReturnArgs(deserializeForRoot(0));
		}

	}

	/**
	 * printout the help info, none for this command
	 */
	public void printHelp() {
		//ignore
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
	 * serialize return values for gapi/root
	 * 
	 * @return serialized return
	 */
	public String deserializeForRoot(int state) {
		
		return RootPrintWriter.columnseparator 
				+ RootPrintWriter.fielddescriptor + "__result__" + RootPrintWriter.fieldseparator + state;
	}
	
	
	/**
	 * Constructor needed for the command factory in commander
	 * @param commander 
	 * @param out 
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandcd(JAliEnCOMMander commander, UIPrintWriter out, final ArrayList<String> alArguments){
		super(commander, out,alArguments);
	}
}
