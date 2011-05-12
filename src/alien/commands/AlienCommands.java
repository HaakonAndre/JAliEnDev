package alien.commands;

import java.security.Principal;
import java.util.ArrayList;

import lazyj.Log;

/**
 * @author Alina Grigoras
 * Alien commands factory
 */
public class AlienCommands {
	/**
	 * Returns a implementation of the command
	 * @param Alien principal received from request
	 * @param array of arguments received from the request
	 * @return AlienCommand implementation for the requested command
	 * @throws Exception
	 */
	public static AlienCommand getAlienCommand(Principal p, ArrayList<Object> al) throws Exception{
		if(p == null)
			throw new SecurityException("No Alien Principal! We hane no credentials");
		
		if(al.size() < 3){
			throw new Exception("Alien Command did not receive minimum number of arguments (in this order): username, current directory, command (+ arguments)? ");
		}
		else{
			String sLocalCommand = (String) al.get(2);
			
			if("ls".equals(sLocalCommand)){
				Log.log(Log.INFO, "Command received = \""+sLocalCommand+"\"");	
				return new AlienCommandls(p, al);
			}
			else return null;
		}
	}

	/**
	 * @param Alien principal
	 * @param array of arguments received from the soap request
	 * @return the name of the requested command
	 * @throws Exception
	 */
	public static String getAlienCommandString(Principal p, ArrayList<Object> al) throws Exception {
		if(p == null)
			throw new SecurityException("No Alien Principal! We hane no credentials");
		
		if(al.size() < 3){
			throw new Exception("Alien Command did not receive minimum number of arguments (in this order): username, current directory, command (+ arguments)? ");
		}
		else{
			String sLocalCommand = (String) al.get(2);
			Log.log(Log.INFO, "Command received = "+sLocalCommand);
			return sLocalCommand;
		}
	}
}
