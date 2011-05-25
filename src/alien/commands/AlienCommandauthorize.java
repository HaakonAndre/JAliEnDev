package alien.commands;

import java.security.Principal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import alien.soap.services.AuthenEngine;
import alien.user.AliEnPrincipal;

import lazyj.Log;

public class AlienCommandauthorize extends AlienCommand {
	public AlienCommandauthorize(final AliEnPrincipal p, final ArrayList<Object> al) throws Exception {
		super(p, al);
	}

	public AlienCommandauthorize (final AliEnPrincipal p, final String sUsername, final String sCurrentDirectory, final String sCommand, final List alArguments) throws Exception {
		super(p, sUsername, sCurrentDirectory, sCommand, alArguments);
	}
	
	@Override
	public HashMap<String, ArrayList<String>> executeCommand() throws Exception{
		HashMap<String, ArrayList<String>> hmReturn = new HashMap<String, ArrayList<String>>();

		ArrayList<String> alrcValues = new ArrayList<String>();
		ArrayList<String> alrcMessages = new ArrayList<String>();
		alrcMessages.add("This is just a simple log\n");
		
		//we need to have at least 2 parameters
		
		if(this.alArguments != null && this.alArguments.size() >= 2){
			//first argument must be access string
			String sAccess = (String) this.alArguments.get(0);
			
			String sJobId = null;
			
			if(this.alArguments.size() == 3){
				sJobId = (String) this.alArguments.get(2);
				if(sJobId.startsWith("-debug")) 
					sJobId = "0";
			}
			
			if("registerenvs".equals(sAccess)){
				ArrayList<String> alInfo = (ArrayList<String>) this.alArguments.get(1);
				
				
			}
			else{
			
				HashMap<String, String> hmInfo = (HashMap<String, String>) this.alArguments.get(1);
				//site=CERN, lfn=test_deps1.jdl, wishedSE=0, excludeSE=
				
				AuthenEngine au = new AuthenEngine();
				alrcValues = (ArrayList<String>) au.authorizeEnvelope(this.pAlienUser, this.sUsername, this.sCurrentDirectory , sAccess, hmInfo, sJobId);
			}
			
		}
		else{
			throw new Exception("Invalid authorize command arguments");		
		}
		
		hmReturn.put("rcvalues", alrcValues);
		hmReturn.put("rcmessages", alrcMessages);
		
		return hmReturn;
	}

}
