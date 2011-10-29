package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

import alien.taskQueue.Job;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author ron
 * @since June 9, 2011
 */
public class JAliEnCommandmasterjob extends JAliEnBaseCommand {

	/**
	 * marker for -M argument
	 */
	private boolean bMerge = false;
	
	/**
	 * marker for -a argument
	 */
	private boolean bKill = false;

	/**
	 * marker for -l argument
	 */
	private boolean bResubmit = false;

	/**
	 * marker for -M argument
	 */
	private boolean bExpunge = false;

	/**
	 * marker for -M argument
	 */
	private boolean bPrintId = false;

	/**
	 * marker for -M argument
	 */
	private boolean bPrintSite = false;
	

	private final String jobId;

	private String id = "";
	
	private String status = "";
	
	private String site = "";
	
	
	public void run() {
		

		List<Job> ps = commander.q_api.getMasterjob(jobId, status, id, site, bPrintId, bPrintSite, bMerge, bKill, bResubmit, bExpunge);
		
		
		out.printErrln("--- not implemented yet ---");

	}

	/**
	 * printout the help info
	 */
	public void printHelp() {
		
		out.printOutln();
		out.printOutln(helpUsage("masterjob", "<jobId> [-options] [merge|kill|resubmit|expunge]"));
		out.printOutln(helpStartOptions());
		out.printOutln(helpOption("-status <status>", "display only the subjobs with that status"));
		out.printOutln(helpOption("-id <id>","display only the subjobs with that id"));
		out.printOutln(helpOption("-site <id>","display only the subjobs on that site"));
		out.printOutln(helpOption("-printid","print also the id of all the subjobs"));
		out.printOutln(helpOption("-printsite","split the number of jobs according to the execution site"));
		out.printOutln();
		out.printOutln(helpOption("merge","collect the output of all the subjobs that have already finished"));
		out.printOutln(helpOption("kill","kill all the subjobs"));
		out.printOutln(helpOption("resubmit","resubmit all the subjobs selected"));
		out.printOutln(helpOption("expunge","delete completely the subjobs"));
		out.printOutln();
		out.printOutln(helpParameter("You can combine kill and resubmit with '-status <status>' and '-id <id>'."));
		out.printOutln(helpParameter("For instance, if you do something like 'masterjob <jobId> -status ERROR_IB resubmit',"));
		out.printOutln(helpParameter(" all the subjobs with status ERROR_IB will be resubmitted"));
		out.printOutln();
	}

	/**
	 * cat cannot run without arguments
	 * 
	 * @return <code>false</code>
	 */
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * nonimplemented command's silence trigger, submit is never silent
	 */
	public void silent() {
		// ignore
	}

	/**
	 * Constructor needed for the command factory in commander
	 * 
	 * @param commander
	 * @param out
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandmasterjob(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);

		try {
			if (alArguments.size() > 0) {
				jobId = alArguments.get(0);

				final OptionParser parser = new OptionParser();

				parser.accepts("status").withRequiredArg();
				parser.accepts("id").withRequiredArg();
				parser.accepts("site").withRequiredArg();

				parser.accepts("kill");

				final OptionSet options = parser.parse(alArguments
						.toArray(new String[] {}));

				if (options.has("status"))
					status = (String) options.valueOf("status");

				if (options.has("id"))
					id = (String) options.valueOf("id");

				if (options.has("site"))
					site = (String) options.valueOf("site");

				String flag = alArguments.get(alArguments.size() - 1);

				if (flag != null)
					if (flag.equals("merge"))
						bMerge = true;
					else if (flag.equals("kill"))
						bKill = true;
					else if (flag.equals("resubmit"))
						bResubmit = true;
					else if (flag.equals("expunge"))
						bExpunge = true;

			} else
				throw new JAliEnCommandException();
		} catch (OptionException e) {
			printHelp();
			throw e;
		}

	}

}
