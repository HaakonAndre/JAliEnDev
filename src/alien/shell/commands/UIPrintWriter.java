package alien.shell.commands;


/**
 * @author ron
 * @since July 15, 2011
 */
abstract class UIPrintWriter {

	/**
	 * Print empty stdout line 
	 */
	abstract protected void printOutln();
	
	/**
	 * Print stdout after appending line feed 
	 */
	abstract protected void printOutln(String line);

	/**
	 * Print empty stderr line 
	 */
	abstract protected void printErrln();
	
	/**
	 * Print stderr after appending line feed 
	 */
	abstract protected void printErrln(String line);
	
	/**
	 * Set the env for the client (needed for gapi)
	 */
	abstract protected void setenv(String cDir, String user, String cDirtiled);
	
	
	/**
	 * Flush a set of lines as one transaction
	 */
	abstract protected void flush();
	
	
	abstract protected void pending();
	
	/**
	 * identify the RootPrinter from above 
	 * @return if it is a RootPrinter
	 */
	protected boolean isRootPrinter(){
		return false;
	}

	
	/**
	 * dummy for RootPrinter
	 */
	protected void setReturnArgs(String args) {
		//void
	}
}
