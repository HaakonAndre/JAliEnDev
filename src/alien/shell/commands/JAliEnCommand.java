package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

import alien.catalogue.LFN;
import alien.user.AliEnPrincipal;

public abstract class JAliEnCommand {
	
	/**
	 * user from the request
	 */
	protected AliEnPrincipal principal;
	
	/**
	 * current directory received from the request
	 */
	protected LFN currentDirectory ;

	/**
	 * current directory received from the request, checked to be absolute path
	 */
	protected LFN absoluteCurrentDirectory ;


	/**
	 * the argument list for the command received through the request
	 */
	protected List<String> alArguments ;

	/**
	 * debug level
	 */
	protected int iDebug = 0;
	
	/**
	 * Constructor based on the array received from the request <br>
	 * The minimum size of the array is 3:
	 * 		<ul>
	 * 			<li>the username that issued the command</li>
	 * 			<li>current directory from where the user issued the command</li>
	 * 			<li>the command</li>
	 * 			<li>extra arguments for the command, this parameter is not mandatory</li>
	 * 		</ul>
	 * @param pAlienUser AliEn Principal received from the https request. <br>
	 * 			It can be different than the user received from SOAP request if the user is using su 
	 * @param al array containg the user that issued the command, the current directory from where the user issued the command,
	 * 			the command and its arguments
	 * @throws Exception
	 */
	public JAliEnCommand(AliEnPrincipal principal, LFN currentDirectory, final ArrayList<String> alArguments) throws Exception{

		this.principal = principal;
		this.currentDirectory = currentDirectory;
		this.alArguments = alArguments;
		
	}

	/**
	 * @return the current directory of the user that issued the command
	 */
	public LFN getsCurrentDirectory() {
		return currentDirectory;
	}

	/**
	 * @return the arguments of the command issued by the user. This can be null or empty
	 */
	public List<String> getAlArguments() {
		return alArguments;
	}


	/**
	 * to be implemented by all AliEn commands
	 * @return the command output
	 * @throws Exception 
	 */
	public abstract void executeCommand() throws Exception;


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
	
}
