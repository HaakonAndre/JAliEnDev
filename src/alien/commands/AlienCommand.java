package alien.commands;

import java.util.List;

import java.security.Principal;

import lazyj.Log;

import alien.user.AliEnPrincipal;

/**
 * @since 2011, 12 May
 * @author Alina Grigoras <br>
 * Abstract class to implement Alien shell commands <br>
 * A commands expects at least 3 paramenters:
 * <ul> 
 * 		<li>the username that issued the command</li>
 * 		<li>current directory from where the user issued the command</li>
 * 		<li>the command</li>
 * 		<li>extra arguments for the command, this parameter is not mandatory</li>
 * </ul>
 */
public abstract class AlienCommand {
	/**
	 * the principal received from http request
	 */
	protected AliEnPrincipal pAlienUser;

	/**
	 * the username received from SOAP Request
	 */
	protected String sUsername ;

	/**
	 * current directory received from SOAP request
	 */
	protected String sCurrentDirectory ;

	/**
	 * command received from SOAP request
	 */
	protected String sCommand ;

	/**
	 * the argument list for the command received through the SOAP request
	 */
	protected List<?> alArguments ;

	/**
	 * Constructor based on the array received from the request <br>
	 * The minimum size of the array is 3:
	 * 		<ul>
	 * 			<li>the username that issued the command</li>
	 * 			<li>current directory from where the user issued the command</li>
	 * 			<li>the command</li>
	 * 			<li>extra arguments for the command, this parameter is not mandatory</li>
	 * 		</ul>
	 * @param the array containing all the information required to run the command
	 * @throws Exception
	 */
	public AlienCommand(final AliEnPrincipal pAlienUser, final List<?> al) throws Exception{
		Log.log(Log.FINER, "Entering AliEn Command constructor with 2 params");
		
		if(pAlienUser == null)
			throw new SecurityException("No Alien Principal! We hane no credentials");

		if(al.size() < 3){
			throw new Exception("Alien Command did not receive minimum number of arguments (in this order): username, current directory, command (+ arguments)? ");
		}
		
		this.pAlienUser = pAlienUser;

		String sLocalUsername = (String) al.get(0);
		String sLocalCurrentDirectory = (String) al.get(1);
		String sLocalCommand = (String) al.get(2);

		Log.log(Log.FINER, "Inside AliEn command constructor with the values: "+sLocalUsername+" / "+sLocalCurrentDirectory+" / "+sLocalCommand);
		
		int alSize = al.size();
		
		List<?> alLocalArguments = null; 

		if(alSize > 3){
			alLocalArguments = al.subList(3, alSize);
		}


		this.sUsername = sLocalUsername;
		this.sCurrentDirectory = sLocalCurrentDirectory;
		this.sCommand = sLocalCommand;
		this.alArguments = alLocalArguments;
		
		Log.log(Log.FINER, "Exiting AliEn Command constructor with 2 params");
	}

	/**
	 * @param sUsername
	 * @param sCurrentDirectory
	 * @param sCommand
	 * @param alArguments
	 * @throws Exception
	 */
	public AlienCommand (final AliEnPrincipal pAlienPrincipal, final String sUsername, final String sCurrentDirectory, final String sCommand, final List<?> alArguments) throws Exception{
		Log.log(Log.FINER, "Entering AliEn Command constructor with 5 params");
		
		if(sUsername == null || sUsername.length() == 0)
			throw new Exception("Empty username");

		if(sCurrentDirectory == null || sCurrentDirectory.length() == 0)
			throw new Exception("Empty current directory");

		if(sCommand == null || sCommand.length() == 0)
			throw new Exception("Empty command");

		this.pAlienUser = pAlienPrincipal;
		this.sUsername = sUsername;
		this.sCurrentDirectory = sCurrentDirectory;
		this.sCommand = sCommand;
		this.alArguments = alArguments;
	
		Log.log(Log.FINER, "Inside AliEn command constructor with the values: "+sUsername+" / "+sCurrentDirectory+" / "+sCommand);
		
		Log.log(Log.FINER, "Exiting AliEn Command constructor with 5 params");
	}

	/**
	 * @return the username that issued the command
	 */
	public String getsUsername() {
		return sUsername;
	}

	/**
	 * @return the current directory of the user that issued the command
	 */
	public String getsCurrentDirectory() {
		return sCurrentDirectory;
	}

	/**
	 * @return the command issued by the user
	 */
	public String getsCommand() {
		return sCommand;
	}

	/**
	 * @return the arguments of the command issued by the user. This can be null or empty
	 */
	public List<?> getAlArguments() {
		return alArguments;
	}


	/**
	 * @return alien principal received from http request
	 */
	public Principal getpAlienUser() {
		return pAlienUser;
	}

	/**
	 * to be implemented by all AliEn commands
	 * @return the command output
	 */
	public abstract Object executeCommand() throws Exception;


	@Override
	public String toString() {
		String sToString  = "AlienCommand { \n";

		sToString += "		Username = "+this.sUsername+" \n";
		sToString += "		Current directory = "+this.sCurrentDirectory+" \n";
		sToString += "		Command = "+this.sCommand+" \n";	
		sToString += "		User principal = "+this.pAlienUser.getName()+" \n";
		sToString += "		Arguments = ";	

		if(this.alArguments != null){
			for(Object o : this.alArguments){
				sToString += o.toString()+" ";
			}
		}
		
		sToString += " } \n";

		return sToString;
	}
}
