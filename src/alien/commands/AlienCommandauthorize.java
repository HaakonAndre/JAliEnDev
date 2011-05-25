package alien.commands;

import java.security.Principal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class AlienCommandauthorize extends AlienCommand {
	public AlienCommandauthorize(final Principal p, final ArrayList<Object> al) throws Exception {
		super(p, al);
	}

	public AlienCommandauthorize (final Principal p, final String sUsername, final String sCurrentDirectory, final String sCommand, final List alArguments) throws Exception {
		super(p, sUsername, sCurrentDirectory, sCommand, alArguments);
	}
	
	@Override
	public HashMap<String, ArrayList<String>> executeCommand() throws Exception{
		HashMap<String, ArrayList<String>> hmReturn = new HashMap<String, ArrayList<String>>();

		ArrayList<String> alrcValues = new ArrayList<String>();
		ArrayList<String> alrcMessages = new ArrayList<String>();

		//we need to have at least 2 parameters
		
		if(this.alArguments != null && this.alArguments.size() >= 2){
			//first argument must be access string
			
			String sAccess = (String) this.alArguments.get(0);
			
			HashMap<String, String> hminfo = (HashMap<String, String>) this.alArguments.get(1);
			
		}
		else{
			throw new Exception("Invalid authorize command arguments");		
		}
		
		return hmReturn;
	}

}
