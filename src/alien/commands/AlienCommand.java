package alien.commands;

import java.util.ArrayList;

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
	 * 
	 */
	protected String sUsername ;
	
	/**
	 * 
	 */
	protected String sCurrentDirectory ;
	
	/**
	 * 
	 */
	protected String sCommand ;
	
	protected ArrayList<Object> alArguments ;
	
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
	public AlienCommand(final ArrayList<Object> al) throws Exception{
		if(al.size() < 3){
			throw new Exception("Alien Command did not receive minimum number of arguments (in this order): username, current directory, command (+ arguments)? ");
		}
		else{
			String sLocalUsername = (String) al.get(0);
			String sLocalCurrentDirectory = (String) al.get(1);
			String sLocalCommand = (String) al.get(2);
			
			int alSize = al.size();
			
			ArrayList<Object> alLocalArguments = null; 
			
			if(alSize > 3){
				alLocalArguments = (ArrayList<Object>) al.subList(3, alSize -1);
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
	public AlienCommand (final String sUsername, final String sCurrentDirectory, final String sCommand, final ArrayList<Object> alArguments) throws Exception{
		if(sUsername == null || sUsername.length() == 0)
			throw new Exception("Empty username");
		
		if(sCurrentDirectory == null || sCurrentDirectory.length() == 0)
			throw new Exception("Empty current directory");
		
		if(sCommand == null || sCommand.length() == 0)
			throw new Exception("Empty command");
		
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
	public ArrayList<Object> getAlArguments() {
		return alArguments;
	}

	/**
	 * @return the command output
	 */
	public abstract Object executeCommand();
}
