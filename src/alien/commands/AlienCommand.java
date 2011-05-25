package alien.commands;

import java.util.List;

import java.security.Principal;

import alien.user.AliEnPrincipal;

/**
 * @since 2011, 12 May
 * @author Alina Grigoras
 * Abstract class to implement Alien shell commands <br />
 * A commands expects at least 3 paramenters: <br />
 * 	- the username that issued the command <br />
 * 	- current directory from where the user issued the command <br />
 * 	- the command <br />
 * 	- extra arguments for the command, this parameter is not mandatory <br />
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
	 * 
	 */
	protected List<?> alArguments ;

	/**
	 * Constructor based on the array received from the request <br />
	 * The minimum size of the array is 3: <br />
	 * 	- the username that issued the command <br />
	 * 	- current directory from where the user issued the command <br />
	 * 	- the command <br />
	 * 	- extra arguments for the command, this parameter is not mandatory <br />
	 * @param the array containing all the information required to run the command
	 * @throws Exception
	 */
	public AlienCommand(final AliEnPrincipal pAlienUser, final List<?> al) throws Exception{

		if(pAlienUser == null)
			throw new SecurityException("No Alien Principal! We hane no credentials");

		if(al.size() < 3){
			throw new Exception("Alien Command did not receive minimum number of arguments (in this order): username, current directory, command (+ arguments)? ");
		}
		else{

			this.pAlienUser = pAlienUser;

			String sLocalUsername = (String) al.get(0);
			String sLocalCurrentDirectory = (String) al.get(1);
			String sLocalCommand = (String) al.get(2);

			int alSize = al.size();
			
			List<?> alLocalArguments = null; 

			if(alSize > 3){
				alLocalArguments = (List<?>) al.subList(3, alSize);
			}


			this.sUsername = sLocalUsername;
			this.sCurrentDirectory = sLocalCurrentDirectory;
			this.sCommand = sLocalCommand;
			this.alArguments = alLocalArguments;
		}
	}

	/**
	 * @param sUsername
	 * @param sCurrentDirectory
	 * @param sCommand
	 * @param alArguments
	 * @throws Exception
	 */
	public AlienCommand (final AliEnPrincipal pAlienPrincipal, final String sUsername, final String sCurrentDirectory, final String sCommand, final List<?> alArguments) throws Exception{
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
