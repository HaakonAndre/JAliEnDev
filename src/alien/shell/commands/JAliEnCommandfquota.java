package alien.shell.commands;

import java.util.ArrayList;

import joptsimple.OptionException;

public class JAliEnCommandfquota extends JAliEnBaseCommand {

	@Override
	public void run() {
		// TODO Auto-generated method stub

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

	public JAliEnCommandfquota(JAliEnCOMMander commander, UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
	}
}
