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
		System.err.println("trece prin execute");
		String sMotdMessage = 
				"###############################################################" +
				"# AliEn Service Message: All is well in the Grid world. Enjoy!#"+
				"##############################################################"+
				"* Operational problems: Latchezar.Betev@cern.ch"+
				"* Bug reports: http://savannah.cern.ch/bugs/?group=alien&func=additem"+
				"###############################################################";

		out.printOutln(sMotdMessage);

	}

	@Override
	public void printHelp() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean canRunWithoutArguments() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void silent() {
		// TODO Auto-generated method stub

	}

}
