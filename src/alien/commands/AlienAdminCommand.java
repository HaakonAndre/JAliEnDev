package alien.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import lazyj.Log;

import alien.user.AliEnPrincipal;

public class AlienAdminCommand  extends AlienCommand{

	public AlienAdminCommand(AliEnPrincipal pAlienUser, List<?> al)
			throws Exception {
		super(pAlienUser, al);
		if(!pAlienUser.canBecome("admin") || super.sUsername!="admin")
			throw new SecurityException("You need to be admin to execute this command");
	}


	public AlienAdminCommand (final AliEnPrincipal pAlienPrincipal, final String sUsername, final String sCurrentDirectory, final String sCommand, 
			final int iDebugLevel,final List<?> alArguments) throws Exception{
            super(pAlienPrincipal,sUsername,sCurrentDirectory,sCommand,iDebugLevel,alArguments);
        	if(!pAlienUser.canBecome("admin") || super.sUsername!="admin")
    			throw new SecurityException("You need to be admin to execute this command");
	}


	@Override
	public Object executeCommand() throws Exception {
		return null;
	}
	
	


}
