package alien.shell.commands;

import java.util.ArrayList;

import joptsimple.OptionException;
import alien.catalogue.access.AuthorizationFactory;
import alien.user.AliEnPrincipal;
import alien.user.UserFactory;

/**
 * @author ron
 * @since Oct 30, 2011
 */
public class JAliEnCommanduser extends JAliEnBaseCommand {
	
	private final String user;
	
	public void run() {
		
		if(AuthorizationFactory.getDefaultUser().canBecome(user)){
			commander.user = UserFactory.getByUsername(user);
			commander.role = AliEnPrincipal.userRole();
		}
		else
			out.printErrln("Permission denied.");
	}

	/**
	 * printout the help info, none for this command
	 */
	public void printHelp() {
		out.printOutln();
		out.printOutln(helpUsage("user","<user name>"));
		out.printOutln();
		out.printOutln(helpParameter("Change effective role as specified."));
	}

	/**
	 * role can not run without arguments 
	 * @return <code>false</code>
	 */
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * nonimplemented command's silence trigger, role is never silent
	 */
	public void silent() {
		//ignore
	}

	
	/**
	 * Constructor needed for the command factory in commander
	 * @param commander 
	 * @param out 
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException 
	 */
	public JAliEnCommanduser(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException {
		super(commander, out,alArguments);

		if(alArguments.size()==1)
			user = alArguments.get(0);
		else
			throw new JAliEnCommandException();

	}
}
