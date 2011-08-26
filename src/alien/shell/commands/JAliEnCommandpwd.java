package alien.shell.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.StringTokenizer;

import alien.api.catalogue.CatalogueApiUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.se.SEUtils;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandpwd extends JAliEnBaseCommand {

	/**
	 * execute the pwd
	 */
	public void execute() {
		out.printOutln(commander.curDir.getCanonicalName());
	}

	/**
	 * printout the help info
	 */
	public void printHelp() {
		// ignore
	}

	/**
	 * get cannot run without arguments
	 * 
	 * @return <code>false</code>
	 */
	public boolean canRunWithoutArguments() {
		return true;
	}

	/**
	 * set command's silence trigger
	 */
	public void silent() {
		// ignore
	}


	/**
	 * Constructor needed for the command factory in commander
	 * 
	 * @param commander
	 * @param out
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandpwd(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) {
		super(commander, out, alArguments);

	}
}
