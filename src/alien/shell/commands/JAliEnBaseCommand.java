package alien.shell.commands;

import java.util.ArrayList;
import java.util.logging.Logger;

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
	
	

	
	protected final static String textnormal = "\033[0m";
	protected final static String  textblack = "\033[49;30m";
	protected final static String  textred = "\033[49;31m";
	protected final static String  textrederror = "\033[47;31;1m";
	protected final static String  textblueerror = "\033[47;34;1m";
	protected final static String  textgreen = "\033[49;32m";
	protected final static String  textyellow = "\033[49;33m";
	protected final static String  textblue = "\033[49;34m";
	protected final static String  textbold = "\033[1m";
	protected final static String  textunbold = "\033[0m";

	/**
	 * marker for -Colour argument
	 */
	protected boolean bColour;
	
	
	/**
	 * 
	 */
	protected final ArrayList<String> alArguments;

	private final static int padHelpUsage = 20;
	
	private final static int padHelpOption = 21;
	
	
	/**
	 * Constructor based on the array received from the request 
	 * @param commander 
	 * @param out 
	 * @param alArguments 
	 */
	public JAliEnBaseCommand(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments){
		this.commander = commander;
		this.out = out;
		this.alArguments = alArguments;
		this.bColour = out.colour();

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
	 * @param name
	 * @return usage tag for help
	 */
	protected String helpUsage(final String name){
		return helpUsage(name,"");
	}
	
	/**
	 * @param name
	 * @param desc 
	 * @return usage tag for help
	 */
	protected String helpUsage(final String name, String desc){
		if(desc!=null && desc.length()>0)
			desc = padSpace(3) + desc;
		else
			desc = "";
		return padRight("usage: " + name + desc, padHelpUsage);
	}
	
	/**
	 * @return
	 */
	protected String helpStartOptions(){
		return "\noptions:";
	}
	
	
	/**
	 * @param opt 
	 * @return option tag for help
	 */
	protected String helpOption(final String opt){
		return helpOption(opt, "");
	}
	
	/**
	 * @param opt 
	 * @param desc 
	 * @return option tag for help
	 */
	protected String helpOption(final String opt, String desc){
		if(desc!=null && desc.length()>0)
			desc = "  :  " + desc;
		else
			desc = "";
		return padSpace(padHelpUsage) + padRight(opt,padHelpOption) + desc;
	}

	/**
	 * @param opt 
	 * @param desc 
	 * @return option tag for help
	 */
	protected String helpParameter(final String desc){
		return padSpace(padHelpUsage) + desc;
	}
	
	
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
