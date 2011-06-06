package alien.perl.commands;

import java.util.List;

import alien.user.AliEnPrincipal;

/**
 * @author ron
 *
 */
public class AlienAdminCommand extends AlienCommand {
	
	private static final String admin = "admin";

	/**
	 * @param pAlienUser
	 * @param al
	 * @throws Exception
	 */
	public AlienAdminCommand(AliEnPrincipal pAlienUser, List<?> al)
			throws Exception {
		super(pAlienUser, al);

		if (!"admin".equals(sUsername))
			throw new PerlSecurityException(
					"You need to be ["+admin+"] to execute this command");
	}

	/**
	 * @param pAlienPrincipal
	 * @param sUsername
	 * @param sCurrentDirectory
	 * @param sCommand
	 * @param iDebugLevel
	 * @param alArguments
	 * @throws Exception
	 */
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
