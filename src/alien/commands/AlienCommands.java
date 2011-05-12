package alien.commands;

import java.security.Principal;
import java.util.ArrayList;

public class AlienCommands {
	public static AlienCommand getAlienCommand(Principal p, ArrayList<Object> al) throws Exception{
		if(p == null)
			throw new SecurityException("No Alien Principal! We hane no credentials");
		
		if(al.size() < 3){
			throw new Exception("Alien Command did not receive minimum number of arguments (in this order): username, current directory, command (+ arguments)? ");
		}
		else{
			String sLocalCommand = (String) al.get(2);
			
			if("ls".equals(sLocalCommand))
				return new AlienCommandls(p, al);
			else return null;
		}
	}
}
