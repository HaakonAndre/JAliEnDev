package alien.shell;

import jline.*;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

import alien.config.ConfigUtils;
import alien.taskQueue.Job;
import alien.taskQueue.TaskQueueUtils;

public class BusyBox {

	private static final String promptPrefix = "jAliEn_v" + ConfigUtils.getVersion();
	private static final String promptSuffix = " > ";
	
	
	private ConsoleReader reader;

	public static void welcome() {

		System.out.println("Welcome to jAlien version "
				+ ConfigUtils.getVersion() + ".");
		System.out.println("May the force be with u!");
		System.out.println();
		System.out.println("Cheers, the Jedis");
		System.out.println();

	}

	public BusyBox() throws IOException {
		// Character mask = null;
		// String trigger = null;

		welcome();
		List completors = new LinkedList();

		reader = new ConsoleReader();
		reader.setBellEnabled(false);
		reader.setDebug(new PrintWriter(new FileWriter("writer.debug", true)));
		reader.addCompletor(new ArgumentCompletor(completors));

		String line;
		PrintWriter out = new PrintWriter(System.out);

		// while ((line = reader.readLine(prompt + "$")) != null) {
		while ((line = reader.readLine(promptPrefix + promptSuffix)) != null) {

			// out.println("==>\"" + line + "\"");
			out.flush();

			// // If we input the special word then we will mask
			// // the next line.
			// if ((trigger != null) && (line.compareTo(trigger) == 0)) {
			// line = reader.readLine("password> ", mask);
			// }
			if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
				break;
			}

			String[] args = line.split("\\s");
			executeCommand(args);
		}
	}

	public void executeCommand(String args[]) {

		if (args.length > 0) {
			if (args[0].equals("none")) {
			} else if (args[0].equals("ps")) {
			  executePS(args);
			} else {
				usage();
			}
		}
		//
		// if (args.length == 3) {
		// mask = new Character(args[2].charAt(0));
		// trigger = args[1];
		// }

	}

	public static void usage() {
		System.out.println("Usage: ");
		System.out.println(" ps <jdl|status|trace> <jobID>");
	}

	private void executePS(String args[]){
		if (args[1].equals("jdl")) {
			int jobID = Integer.parseInt(args[2]);
			Job job = TaskQueueUtils.getJob(jobID);
			System.out.println("JDL of job id "+jobID+" :");
			System.out.println(job.jdl);
		} else if (args[1].equals("status")) {
			int jobID = Integer.parseInt(args[2]);
			Job job = TaskQueueUtils.getJob(jobID);
			System.out.println("Status of job id "+jobID+" :");
			System.out.println(job.status);
		} else if (args[1].equals("trace")) {
			int jobID = Integer.parseInt(args[2]);
			String trace = TaskQueueUtils.getJobTraceLog(jobID);
			System.out.println("TraceLog of job id "+jobID+" :");
			System.out.println(trace);
		}
	}
	
}
