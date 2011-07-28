package alien.shell.commands;


abstract class UIPrintWriter {

	/**
	 * Print stdout after appending line feed 
	 */
	abstract protected void printOutln(String line);

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
