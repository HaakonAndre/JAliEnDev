package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

import alien.io.TransferDetails;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 *
 */
public class JAliEnCommandlistTransfer extends JAliEnBaseCommand {
	private String status;
	private String toSE;
	private String user;
	private Integer id;
	private boolean verbose;
	private boolean jdl;
	private int count;
	private boolean sort_desc;

	@Override
	public void run() {
		out.printOutln("TransferId\t   Status\t   User\t\tDestination      \tSize" + (this.jdl ? "\t\tSource" : "") + "\t\tAttempts" + (this.verbose ? "\t\tError reason" : ""));
		if (this.count == 0) {
			out.printOutln();
			out.printOutln("Total: 0 transfers");
			return;
		}
		final List<TransferDetails> transfers = commander.c_api.listTransfer(this.toSE, this.user, this.status, this.id, this.count, this.sort_desc);
		if (transfers == null)
			return;
		for (final TransferDetails t : transfers)
			out.printOutln(t.transferId + "\t   " + t.status + "\t   " + t.user + "\t" + t.destination + String.format("%14d", Long.valueOf(t.size)) + // t.size
																																						// +
					(this.jdl && t.jdl != null ? "\t\t" + t.jdl : "\t\t") + "\t" + t.attempts + (this.verbose && t.reason != null ? "\t" + t.reason : ""));
		out.printOutln();
		out.printOutln("Total: " + transfers.size() + " transfers");
	}

	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln("listTransfer: returns all the transfers that are waiting in the system");
		out.printOutln("        Usage:");
		out.printOutln(
				"                listTransfer [-status <status>] [-user <user>] [-id <queueId>] [-verbose] [-master] [-summary] [-all_status] [-jdl] [-destination <site>]  [-list=<number(all transfers by default)>] [-desc]");
		out.printOutln();

	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

	/**
	 * @param commander
	 * @param out
	 * @param alArguments
	 * @throws OptionException
	 */
	public JAliEnCommandlistTransfer(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);

		try {
			final OptionParser parser = new OptionParser();
			parser.accepts("list").withRequiredArg();
			parser.accepts("status").withRequiredArg();
			parser.accepts("user").withRequiredArg();
			parser.accepts("id").withRequiredArg();
			parser.accepts("verbose");
			parser.accepts("summary");
			parser.accepts("all_status");
			parser.accepts("jdl");
			parser.accepts("desc");
			parser.accepts("destination").withRequiredArg();

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			if (options.has("list")) {
				final int cnt = Integer.parseInt((String) options.valueOf("list"));
				if (cnt < 0)
					throw new NumberFormatException();
				this.count = cnt;
			}
			else
				this.count = -1;

			this.status = (String) options.valueOf("status");
			this.user = (String) options.valueOf("user");
			if (options.has("id"))
				this.id = new Integer((String) options.valueOf("id"));
			this.verbose = options.has("verbose");
			this.jdl = options.has("jdl");
			this.sort_desc = options.has("desc");
			this.toSE = (String) options.valueOf("destination");
		} catch (final OptionException e) {
			throw e;
		} catch (final NumberFormatException e) {
			out.printErrln("Please provide a valid number for -list argument");
			throw e;
		}
	}
}
