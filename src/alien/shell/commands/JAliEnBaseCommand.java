package alien.shell.commands;

import java.util.ArrayList;
import java.util.logging.Logger;

import joptsimple.OptionException;

import alien.catalogue.GUIDUtils;
import alien.config.ConfigUtils;

/**
 * @author ron
 * @since June 4, 2011
 */
public abstract class JAliEnBaseCommand {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils
			.getLogger(GUIDUtils.class.getCanonicalName());
	
	/**
	 * The JAliEnCOMMander
	 */
	protected JAliEnCOMMander commander ;
	
	
	/**
	 * The UIPrintWriter to return stdout+stderr
	 */
	protected UIPrintWriter out ;
	
	
	protected final ArrayList<String> alArguments;

	
	/**
	 * Constructor based on the array received from the request 
	 * @param commander 
	 * @param out 
	 * @param alArguments 
	 * @throws OptionException 
	 */
	public JAliEnBaseCommand(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException{
		this.commander = commander;
		this.out = out;
		this.alArguments = alArguments;

	}
	
	/**
	 * Abstract class to execute the command
	 * @throws Exception 
	 * 
	 */
	public abstract void execute() throws Exception;

	/**
	 * Abstract class to printout the help info of the command
	 * 
	 */
	public abstract void printHelp();

	/**
	 * Abstract class to check if this command can run without arguments
	 * @return true if this command can run without arguments
	 */
	public abstract boolean canRunWithoutArguments();
	
	/**
	 * Abstract class to to set the command to silent mode
	 * 
	 */
	public abstract void silent();
		
	
	
	/**
	 * serialize return values for gapi/root 
	 * @return serialized return
	 */
	public String deserializeForRoot(){
		return RootPrintWriter.columnseparator + RootPrintWriter.fielddescriptor + "__result__"
				 + RootPrintWriter.fieldseparator;
	}
	
	/**
	 * @param s
	 * @param n
	 * @return left-padded string
	 */
	public static String padLeft(final String s, final int n) {
	    return String.format("%1$#" + n + "s", s);  
	}
	
	/**
	 * @param s
	 * @param n
	 * @return right-padded string
	 */
	public static String padRight(String s, int n) {
	     return String.format("%1$-" + n + "s", s);  
	}
	
	/**
	 * @param n
	 * @return n count spaces as String
	 */
	public static String padSpace(int n) {
		String s = "";
		for(int a=0;a<n;a++)
			s += " ";
		return s;
	}
	
	/**
	 * @param n
	 * @return n count tabs as String
	 */
	public static String padTab(int n) {
		String s = "";
		for(int a=0;a<n;a++)
			s += "\t";
		return s;
	}
	
}
