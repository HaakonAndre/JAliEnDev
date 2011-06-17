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
	 * Flush a set of lines as one transaction
	 */
	abstract protected void flush();
}
