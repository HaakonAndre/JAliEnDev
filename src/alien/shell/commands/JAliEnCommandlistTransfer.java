package alien.shell.commands;

import java.util.ArrayList;

import joptsimple.OptionException;

public class JAliEnCommandlistTransfer extends JAliEnBaseCommand {
	private String status;
	private String site;
	private String user;
	private String id;
	private boolean master;
	private boolean verbose;
	private boolean summary;
	private boolean all_status;
	private boolean jdl;
	private int count;	
	
	@Override
	public void run() {
		// TODO Auto-generated method stub

	}

	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln("listTransfer: returns all the transfers that are waiting in the system");
		out.printOutln("        Usage:");
		out.printOutln("                listTransfer [-status <status>] [-user <user>] [-id <queueId>] [-verbose] [-master] [-summary] [-all_status] [-jdl] [-destination <site>]  [-list=<number(all transfers by default)>]");
		out.printOutln();
		
	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

	public JAliEnCommandlistTransfer(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
	}
}
