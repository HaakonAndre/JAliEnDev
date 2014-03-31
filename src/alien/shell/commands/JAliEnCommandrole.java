package alien.shell.commands;

import java.util.ArrayList;

import joptsimple.OptionException;
import alien.catalogue.access.AuthorizationFactory;
import alien.user.AliEnPrincipal;

/**
 * @author ron
 * @since Oct 30, 2011
 */
public class JAliEnCommandrole extends JAliEnBaseCommand {
	
	private String role;
	
	@Override
	public void run() {
		
		if(AuthorizationFactory.getDefaultUser().hasRole(role)){
			commander.role = role;
		}
		else
		{
			if(out.isRootPrinter())
				out.setField("Error ", "Permission denied.");
			else
				out.printErrln("Permission denied.");
		}
	}

	/**
	 * printout the help info, none for this command
	 */
	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln(helpUsage("role","[<role name>]"));
		out.printOutln();
		out.printOutln(helpParameter("Change effective role as specified."));
		out.printOutln(helpParameter("Without role name, changes to default user group [" + AliEnPrincipal.userRole()
				+ "]."));
	}

	/**
	 * role can run without arguments 
	 * @return <code>true</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return true;
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
	public JAliEnCommandrole(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException {
		super(commander, out,alArguments);

		System.out.println("Role called with: [" + alArguments + "]");
		
		if(alArguments.size()==1)
			role = alArguments.get(0);
		else if (alArguments.size()<1)
			role = AliEnPrincipal.userRole();
		else 
			throw new JAliEnCommandException();
	}
}
