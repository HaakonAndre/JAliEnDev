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
	private PrintWriter out;

	public void welcome() {

		out.println("Welcome to jAlien version "
				+ ConfigUtils.getVersion() + ".");
		out.println("May the force be with u!");
		out.println();
		out.println("Cheers, the Jedis");
		out.println();

	}

	public BusyBox() throws IOException {
		
		out = new PrintWriter(System.out);


		welcome();
		out.flush();


		reader = new ConsoleReader();
		reader.setBellEnabled(false);
		reader.setDebug(new PrintWriter(new FileWriter("writer.debug", true)));
		List completors = new LinkedList();
		reader.addCompletor(new ArgumentCompletor(completors));

		
		String line;

		while ((line = reader.readLine(promptPrefix + promptSuffix)) != null) {

			out.flush();

			if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
				break;
			}

			String[] args = line.split("\\s");
			executeCommand(args);
			out.flush();

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
	}

	public void usage() {
		out.println("Usage: ");
		out.println(" ps <jdl|status|trace> <jobID>");
	}
	
	
	
	private void executePS(String args[]){
		if (args.length < 2) {
			out.println("no ps parameters given.");
		} else if (args[1].equals("jdl")) {
			int jobID = Integer.parseInt(args[2]);
			Job job = TaskQueueUtils.getJob(jobID);
			out.println("JDL of job id "+jobID+" :");
			String jdl = job.getJDL();
			out.println(jdl);
		} else if (args[1].equals("status")) {
			int jobID = Integer.parseInt(args[2]);
			Job job = TaskQueueUtils.getJob(jobID);
			out.println("Status of job id "+jobID+" :");
			String status = "null";
			if(job.status != null) status = job.status;
			out.println(status);
		} else if (args[1].equals("trace")) {
			int jobID = Integer.parseInt(args[2]);
			String trace = TaskQueueUtils.getJobTraceLog(jobID);
			out.println("TraceLog of job id "+jobID+" :");
			out.println(trace);
		}
	}
	
}
