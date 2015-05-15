package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

import alien.io.TransferDetails;
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
		out.printOutln("TransferId\tStatus\tUser\tDestination\tSize" +
				( this.jdl ? "\tSource" : "" ) +
				"\tAttempts");
		List<TransferDetails> transfers = commander.c_api.listTransfer();
		if( transfers == null )
			return;
		for( TransferDetails t : transfers ){
			out.printOutln( t.transferId + "\t" + t.status + "\t" + t.user + "\t" +
						t.destination + "\t" + t.size + 
						( this.jdl ? "\t" + t.jdl : "" ) + 
						"\t" + t.attempts);
		}
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
			
			
			if( options.has("list") ){
				int cnt = Integer.parseInt( (String)options.valueOf("list") );
				if( cnt < 0 ) throw new NumberFormatException();
				this.count = cnt;
			}
			else
				this.count = -1;
			
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
			throw e;
		}
		catch(NumberFormatException e){
			out.printErrln("Please provide a valid number for -list argument");			
			throw e;
		}
	}
}
