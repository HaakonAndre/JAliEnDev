package alien.shell.commands;

import java.util.ArrayList;

import alien.user.AliEnPrincipal;
import alien.user.UserFactory;
import joptsimple.OptionException;

/**
 * @author ron
 * @since Oct 30, 2011
 */
public class JAliEnCommanduser extends JAliEnBaseCommand {

	private final String user;

	@Override
	public void run() {
		java.security.cert.X509Certificate[] cert = commander.user.getUserCert();
		AliEnPrincipal switchUser;

		if (commander.user.canBecome(user)) {
			if ((switchUser = UserFactory.getByUsername(user)) != null)
				commander.user = switchUser;
			else
				if ((switchUser = UserFactory.getByRole(user)) != null)
					commander.user = switchUser;
				else {
					if (out.isRootPrinter())
						out.setField("message", "User " + user + " cannot be found. Abort");
					else
						out.printErrln("User " + user + " cannot be found. Abort");
				}

			commander.user.setUserCert(cert);
		}
		else
			if (out.isRootPrinter())
				out.setField("message", "Switching user " + commander.user.getName() + " to [" + user + "] failed");
			else
				out.printErrln("Switching user " + commander.user.getName() + " to [" + user + "] failed");

	}

	/**
	 * printout the help info, none for this command
	 */
	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln(helpUsage("user", "<user name>"));
		out.printOutln();
		out.printOutln(helpParameter("Change effective role as specified."));
	}

	/**
	 * role can not run without arguments
	 *
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * Constructor needed for the command factory in commander
	 *
	 * @param commander
	 * @param out
	 *
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommanduser(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);

		if (alArguments.size() == 1)
			user = alArguments.get(0);
		else
			throw new JAliEnCommandException();

	}
}
