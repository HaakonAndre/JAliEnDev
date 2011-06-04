package alien.shell;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import jline.ArgumentCompletor;
import jline.ConsoleReader;
import jline.SimpleCompletor;
import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcessBuilder;
import alien.catalogue.LFN;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.JAliEnIAm;
import alien.shell.commands.JAliEnCommandcat;
import alien.shell.commands.JAliEnCommandcd;
import alien.shell.commands.JAliEnCommandget;
import alien.shell.commands.JAliEnCommandls;
import alien.shell.commands.JAliEnCommandwhereis;
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
	private static final String promptPrefix = "@" + JAliEnIAm.whatsMyName()
			+ ":";
	private static final String promptSuffix = "$> ";
	private static final String myHome = "/alice/cern.ch/user/s/sschrein/";

	private LFN currentDir = null;
	private String mySite = "CERN";

	private ConsoleReader reader;
	private PrintWriter out;

	private static final String[] commandList = new String[] { "gbox", ".",
			"ls", "get", "cat", "whoami", "ps", "whereis" };

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

		reader.addCompletor(new SimpleCompletor(commandList));

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
				} else if (args[0].equals(".")) {
					String command = "";
					for (int c = 1; c < args.length; c++)
						command += args[c] + " ";
					syscall(command);
				} else if (args[0].equals("gbox")) {
					String command = "alien -s -e ";
					for (int c = 1; c < args.length; c++)
						command += args[c] + " ";
					syscall(command);
				} else if (args[0].equals("role")) {
					// user = args[1];
				} else if (args[0].equals("whoami")) {
					System.out.println(user);
				} else if (args[0].equals("ps")) {
					executePS(args);
				} else if (args[0].equals("whereis")) {
					(new JAliEnCommandwhereis(user, currentDir, argList))
							.executeCommand();
				} else if (args[0].equals("get")) {
					(new JAliEnCommandget(user, currentDir, mySite, argList))
							.executeCommand();
				} else if (args[0].equals("cat")) {

					(new JAliEnCommandcat(user, currentDir, mySite, argList))
							.executeCommand();

				} else if (args[0].equals("ls")) {
					(new JAliEnCommandls(user, currentDir, argList))
							.executeCommand();
				} else if (args[0].equals("timechallenge")) {
					timingChallenge(argList);
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

	private void syscall(String command) {

		String line;
		InputStream stderr = null;
		InputStream stdout = null;
		Process p = null;
		Runtime rt;
		try {
			rt = Runtime.getRuntime();
			p = rt.exec(command);
			stderr = p.getErrorStream();
			stdout = p.getInputStream();

			BufferedReader brCleanUp = new BufferedReader(
					new InputStreamReader(stdout));
			while ((line = brCleanUp.readLine()) != null)
				System.out.println(line);

			brCleanUp.close();

			brCleanUp = new BufferedReader(new InputStreamReader(stderr));
			while ((line = brCleanUp.readLine()) != null)
				System.out.println(line);

			brCleanUp.close();
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	private void timingChallenge(ArrayList<String> args) {

		System.out.println("args: " + args);
		
		
		int times = 100;
		String sTimes = args.get(0);
		args.remove(0);
		if(Integer.parseInt(sTimes)>1)
			times = Integer.parseInt(sTimes);
		
		String command = args.get(0);
		args.remove(0);
		
		ArrayList<Long> timings = new ArrayList<Long>(times);

		
		if (command.equals("ls")) {
			for (int t = 0; t < times; t++) {
				try {
					JAliEnCommandls ls = new JAliEnCommandls(user, currentDir,
							args);
					ls.executeCommand();
					timings.add(ls.timingChallenge);
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			}
		} else if (command.equals("get")) {
			for (int t = 0; t < times; t++) {
				try {
					JAliEnCommandget get = new JAliEnCommandget(user,
							currentDir, mySite, args);
					get.isATimeChallenge = true;
					get.executeCommand();
					timings.add(get.timingChallenge);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		}

		System.out.println("The row:" + timings);
		Iterator<Long> it = timings.iterator();
		long alltime = (long) 0;
		float avr = 0;
		while(it.hasNext()){
			alltime = alltime + it.next();
		}
		System.out.println("Total:" + alltime);
		avr = alltime / times;
		avr = avr /1000;
		
		System.out.println("Average:" + avr);

	}

}
