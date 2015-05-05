package alien.shell.commands;

import java.util.ArrayList;

import joptsimple.OptionException;

public class JAliEnCommandlistSEDistance extends JAliEnBaseCommand {

	@Override
	public void run() {
		// TODO Auto-generated method stub

	}

	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln("listSEDistance: Returns the closest working SE for a particular site. Usage");
		out.printOutln();
		out.printOutln(" listSEDistance [<site>] [read [<lfn>]|write]");
		out.printOutln();
		out.printOutln();
		out.printOutln(" Options:");
		out.printOutln("   <site>: site name. Default: current site");
		out.printOutln("   [read|write]: action. Default write. In the case of read, if an lfn is specified, use only SE that contain that file");
		out.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {		
		return true;
	}

	public JAliEnCommandlistSEDistance(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
	}
}
