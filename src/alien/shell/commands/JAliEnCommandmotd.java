package alien.shell.commands;

import java.util.ArrayList;

public class JAliEnCommandmotd extends JAliEnBaseCommand {

	public JAliEnCommandmotd(JAliEnCOMMander commander, UIPrintWriter out,
			ArrayList<String> alArguments) {
		super(commander, out, alArguments);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void execute() throws Exception {

		String sMotdMessage = 
				"\n###############################################################\n" +
				"# AliEn Service Message: All is well in the Grid world. Enjoy!#\n"+
				"##############################################################\n"+
				"* Operational problems: Latchezar.Betev@cern.ch\n"+
				"* Bug reports: http://savannah.cern.ch/bugs/?group=alien&func=additem\n"+
				"###############################################################\n";

		out.printOutln(sMotdMessage);

	}

	@Override
	public void printHelp() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean canRunWithoutArguments() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public void silent() {
		// TODO Auto-generated method stub

	}

}
