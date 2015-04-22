package alien.shell.commands;

import java.util.ArrayList;

import joptsimple.OptionException;
import alien.quotas.Quota;

// TODO : implement top command

public class JAliEnCommandtop  extends JAliEnBaseCommand {
	@Override
	public void run() { 
		String username = commander.user.getName();
		
			
	}

	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln(helpUsage("top",""));		
		out.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}
	
	public JAliEnCommandtop(JAliEnCOMMander commander, UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
	}
}
