package alien.commands;

import java.util.List;

import alien.user.AliEnPrincipal;

public class AlienAdminCommand  extends AlienCommand{

	public AlienAdminCommand(AliEnPrincipal pAlienUser, List<?> al)
			throws Exception {
		super(pAlienUser, al);
		if(!pAlienUser.canBecome("admin") || super.sUsername!="admin")
			throw new SecurityException("You need to be admin to execute this command");
	}

	
	@Override
	public Object executeCommand() throws Exception {
		return null;
	}


}
