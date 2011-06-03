package alien.shell;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import jline.ArgumentCompletor;
import jline.ConsoleReader;
import alien.catalogue.LFN;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.JAliEnIAm;
import alien.shell.commands.JAliEnCommandcd;
import alien.shell.commands.JAliEnCommandls;
import alien.taskQueue.Job;
import alien.taskQueue.TaskQueueUtils;
import alien.ui.api.CatalogueApiUtils;
import alien.user.AliEnPrincipal;
import alien.user.UsersHelper;

/**
 * @author ron
 * @since Feb, 2011
 */
public class BusyBox {

	private static AliEnPrincipal user = AuthorizationFactory.getDefaultUser();
	private static final String promptPrefix = "@" + JAliEnIAm.whatsMyName()+":";
	private static final String promptSuffix = "$> ";
	private static final String myHome = "/alice/cern.ch/user/s/sschrein/";

	private LFN currentDir = null;

	private ConsoleReader reader;
	private PrintWriter out;

	public void welcome() {

		out.println("Welcome to " + JAliEnIAm.whatsMyFullName() + ".");
		out.println("May the force be with u, " + user + "!");
		out.println();
		out.println("Cheers, the Jedis");
		out.println();

	}

	public BusyBox() throws IOException {

		out = new PrintWriter(System.out);

		welcome();
		out.flush();

		currentDir = CatalogueApiUtils.getLFN(myHome);

		reader = new ConsoleReader();
		reader.setBellEnabled(false);
		reader.setDebug(new PrintWriter(new FileWriter("writer.debug", true)));
		reader.addCompletor(new ArgumentCompletor(new LinkedList()));

		String line;
		while ((line = reader.readLine(getPromptDisplay())) != null) {

			out.flush();
			
			line = line.trim();

			if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
				break;
			}

			String[] args = line.split("\\s");
			executeCommand(args);
			out.flush();

		}
	}

	private String getPromptDisplay() {
		return user.getName()
				+ promptPrefix
				+ currentDir.getCanonicalName().replace(
						UsersHelper.getHomeDir(user.getName()), "~")
				+ promptSuffix;
	}

	public void executeCommand(String args[]) {

		if (args.length > 0) {
			ArrayList<String> argList = new ArrayList<String>(
					Arrays.asList(args));
			argList.remove(args[0]);

			try {
				if (args[0].equals("pwd")) {
					System.out.println(currentDir.getCanonicalName());
				} else if (args[0].equals("cd")) {
					currentDir = (new JAliEnCommandcd(user, currentDir, argList))
							.changeDir();
				} else if (args[0].equals("role")) {
					// user = args[1];
				} else if (args[0].equals("whoami")) {
					System.out.println(user);
				} else if (args[0].equals("ps")) {
					executePS(args);
				} else if (args[0].equals("ls")) {

					(new JAliEnCommandls(user, currentDir, argList))
							.executeCommand();

				} else if (args[0].equals("help")) {
					usage();
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void usage() {
		out.println("Usage: ");
		out.println(" ps <jdl|status|trace> <jobID>");
	}

	private void executePS(String args[]) {
		if (args.length < 2) {
			out.println("no ps parameters given.");
		} else if (args[1].equals("jdl")) {
			int jobID = Integer.parseInt(args[2]);
			Job job = TaskQueueUtils.getJob(jobID);
			out.println("JDL of job id " + jobID + " :");
			String jdl = job.getJDL();
			out.println(jdl);
		} else if (args[1].equals("status")) {
			int jobID = Integer.parseInt(args[2]);
			Job job = TaskQueueUtils.getJob(jobID);
			out.println("Status of job id " + jobID + " :");
			String status = "null";
			if (job.status != null)
				status = job.status;
			out.println(status);
		} else if (args[1].equals("trace")) {
			int jobID = Integer.parseInt(args[2]);
			String trace = TaskQueueUtils.getJobTraceLog(jobID);
			out.println("TraceLog of job id " + jobID + " :");
			out.println(trace);
		}
	}

}
