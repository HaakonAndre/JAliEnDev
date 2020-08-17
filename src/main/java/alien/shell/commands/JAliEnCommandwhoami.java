package alien.shell.commands;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import alien.user.AliEnPrincipal;

/**
 * Some info about the current identity
 * 
 * @author costing
 * @since Aug 17, 2020
 */
public class JAliEnCommandwhoami extends JAliEnBaseCommand {

	@Override
	public void run() {
		final AliEnPrincipal user = commander.getUser();

		// simple shell printout
		commander.printOutln(user.getName());

		// all other current user info is available to the API only
		commander.printOut("username", user.getName());
		commander.printOut("role", user.getDefaultRole());
		commander.printOut("roles", String.join(",", user.getRoles()));

		if (user.getUserCert() != null) {
			final ZonedDateTime userNotAfter = user.getUserCert()[0].getNotAfter().toInstant().atZone(ZoneId.systemDefault());

			commander.printOut("certificate_expires", String.valueOf(userNotAfter.toEpochSecond()));
			commander.printOut("certificate_dn", user.getUserCert()[0].getSubjectDN().toString());
		}

		if (user.getRemoteEndpoint() != null)
			commander.printOut("connected_from", user.getRemoteEndpoint().toString());
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("whoami", ""));
		commander.printOutln();
	}

	/**
	 * this command can run without arguments
	 *
	 * @return <code>true</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

	/**
	 * Constructor needed for the command factory in commander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandwhoami(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);
	}
}
