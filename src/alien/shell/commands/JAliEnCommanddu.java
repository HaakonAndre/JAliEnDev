package alien.shell.commands;

import java.util.ArrayList;

import joptsimple.OptionException;

public class JAliEnCommanddu extends JAliEnBaseCommand {

	@Override
	public void run() {
		// TODO Auto-generated method stub

	}

	@Override
	public void printHelp() {
		out.printOutln("Gives the disk space usge of a directory");
		out.printOutln("Usage: du [-hf] <dir>");		        
		out.printOutln();
		out.printOutln("Options:");
		out.printOutln("	-h: Give the output in human readable format");
		out.printOutln("	-f: Count only files (ignore the size of collections)");
		out.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

	public JAliEnCommanddu(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
	}
}
