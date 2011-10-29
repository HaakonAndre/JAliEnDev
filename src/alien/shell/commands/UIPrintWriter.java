package alien.shell.commands;


/**
 * @author ron
 * @since July 15, 2011
 */
abstract class UIPrintWriter {

	

	/**
	 * Print set output black'n white
	 */
	abstract protected void blackwhitemode();

	/**
	 * Print set output mode to color
	 */
	abstract protected void colourmode();

	/**
	 * Print get the color output mode
	 * @return state of the color mode
	 */
	abstract protected boolean colour();
	
	
	/**
	 * Print empty stdout line 
	 */
	abstract protected void printOutln();
	
	/**
	 * Print stdout after appending line feed 
	 * @param line 
	 */
	abstract protected void printOutln(String line);

	/**
	 * Print empty stderr line 
	 */
	abstract protected void printErrln();
	
	/**
	 * Print stderr after appending line feed 
	 * @param line 
	 */
	abstract protected void printErrln(String line);
	
	/**
	 * Set the env for the client (needed for gapi)
	 * @param cDir 
	 * @param user 
	 * @param cRole 
	 */
	abstract protected void setenv(String cDir, String user, String cRole);
	
	
	/**
	 * Flush a set of lines as one transaction
	 */
	abstract protected void flush();
	
	
	abstract protected void pending();
	
	abstract protected void degraded();
	
	
	/**
	 * identify the RootPrinter from above 
	 * @return if it is a RootPrinter
	 */
	protected boolean isRootPrinter(){
		return false;
	}

	
	/**
	 * dummy for RootPrinter
	 * @param args 
	 */
	protected void setReturnArgs(String args) {
		//void
	}
}
