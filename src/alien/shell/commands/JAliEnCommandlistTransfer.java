package alien.shell.commands;

import java.util.ArrayList;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class JAliEnCommandlistTransfer extends JAliEnBaseCommand {
	private String status;
	private String toSE;
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
		
		try {
			final OptionParser parser = new OptionParser();
			parser.accepts("list").withRequiredArg();
			parser.accepts("status").withRequiredArg();
			parser.accepts("user").withRequiredArg();
			parser.accepts("id").withRequiredArg();
			parser.accepts("verbose");
			parser.accepts("master");
			parser.accepts("summary");
			parser.accepts("all_status");
			parser.accepts("jdl");
			parser.accepts("destination").withRequiredArg();
			
			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			this.count = Integer.parseInt( (String)options.valueOf("list") );
			this.status = (String) options.valueOf("status");
			this.user = (String) options.valueOf("user");
			this.id = (String)options.valueOf("id");
			this.verbose = options.has("verbose");
			this.master = options.has("master");
			this.summary = options.has("summary");
			this.all_status = options.has("all_status");
			this.jdl = options.has("jdl");
			this.toSE = (String) options.valueOf("destination");
			
		} 
		catch (OptionException e) {
			printHelp();
			throw e;
		}
		catch(NumberFormatException e){
			out.printErrln("Please provide a number for -list argument");
			printHelp();
			throw e;
		}
	}
}
