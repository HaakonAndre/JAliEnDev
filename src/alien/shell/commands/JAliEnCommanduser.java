package alien.shell.commands;

import java.util.ArrayList;

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
		if (commander.user.canBecome(user))
			commander.user = UserFactory.getByUsername(user);
		else
			if (out.isRootPrinter())
				out.setField("message", "Permission denied.");
			else
				out.printErrln("Permission denied.");

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
