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
 * @since August 26, 2011
 */
public class JAliEnCommandpwd extends JAliEnBaseCommand {

	/**
	 * execute the pwd
	 */
	public void run() {
		out.printErrln(commander.curDir.getCanonicalName());
		out.setReturnArgs(deserializeForRoot());
	}
	public void execute() {}

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
	 * serialize return values for gapi/root
	 * 
	 * @return serialized return
	 */
	public String deserializeForRoot() {
		
		return RootPrintWriter.columnseparator 
				+ RootPrintWriter.fielddescriptor + "__result__" + RootPrintWriter.fieldseparator + "1";
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
