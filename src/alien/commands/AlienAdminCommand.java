package alien.commands;

import java.util.List;

import alien.user.AliEnPrincipal;

public class AlienAdminCommand extends AlienCommand {
	
	private static final String admin = "admin";

	public AlienAdminCommand(AliEnPrincipal pAlienUser, List<?> al)
			throws Exception {
		super(pAlienUser, al);

		if (!"admin".equals(sUsername))
			throw new PerlSecurityException(
					"You need to be ["+admin+"] to execute this command");
	}

	public AlienAdminCommand(final AliEnPrincipal pAlienPrincipal,
			final String sUsername, final String sCurrentDirectory,
			final String sCommand, final int iDebugLevel,
			final List<?> alArguments) throws Exception {
		super(pAlienPrincipal, sUsername, sCurrentDirectory, sCommand,
				iDebugLevel, alArguments);
		if (!"admin".equals(sUsername))
			throw new PerlSecurityException(
					"You need to be ["+admin+"] to execute this command");

	}

	@Override
	public Object executeCommand() throws Exception {
		return null;
	}

}
