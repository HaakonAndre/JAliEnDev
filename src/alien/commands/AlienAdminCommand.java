package alien.commands;

import java.util.List;

import alien.user.AliEnPrincipal;

public class AlienAdminCommand extends AlienCommand {

	public AlienAdminCommand(AliEnPrincipal pAlienUser, List<?> al)
			throws Exception {
		super(pAlienUser, al);
		if (!pAlienUser.canBecome("admin"))
			throw new PerlSecurityException(
					"CANBECOME: You need to be [admin] to execute this command");

		try {

			System.out.println("the sUsername: " + sUsername);

			System.out.println("super sUsername: " + super.sUsername);
			System.out.println("this sUsername: " + this.sUsername);
		} catch (NullPointerException e) {

		}

		if (this.sUsername != "admin")
			throw new PerlSecurityException(
					"USERNAME: You need to be [admin] to execute this command");
		if (!pAlienUser.canBecome("admin") || super.sUsername != "admin")
			throw new PerlSecurityException(
					"You need to be [admin] to execute this command");
	}

	public AlienAdminCommand(final AliEnPrincipal pAlienPrincipal,
			final String sUsername, final String sCurrentDirectory,
			final String sCommand, final int iDebugLevel,
			final List<?> alArguments) throws Exception {
		super(pAlienPrincipal, sUsername, sCurrentDirectory, sCommand,
				iDebugLevel, alArguments);
		if (!pAlienUser.canBecome("admin") || super.sUsername != "admin")
			throw new PerlSecurityException(
					"You need to be [admin] to execute this command");

	}

	@Override
	public Object executeCommand() throws Exception {
		return null;
	}

}
