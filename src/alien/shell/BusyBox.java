package alien.shell;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import jline.ConsoleReader;
import jline.SimpleCompletor;
import alien.config.JAliEnIAm;
import alien.shell.commands.JAliEnCOMMander;
import alien.taskQueue.Job;
import alien.taskQueue.TaskQueueUtils;

/**
 * @author ron
 * @since Feb, 2011
 */
public class BusyBox {
	
	private static final String promptPrefix = "@" + JAliEnIAm.whatsMyName()
			+ ":";
	private static final String promptSuffix = "$> ";

	private ConsoleReader reader;
	private PrintWriter out;

	
	/**
	 * print welcome
	 */
	public void welcome() {

		out.println("Welcome to " + JAliEnIAm.whatsMyFullName() + ".");
		out.println("May the force be with u, " + JAliEnCOMMander.getUsername() + " !");
		out.println();
		out.println("Cheers, AJs aka ACS");
		out.println();

	}

	/**
	 * the JAliEn busy box
	 * @throws IOException 
	 */
	public BusyBox() throws IOException {
		

		out = new PrintWriter(System.out);

		welcome();
		out.flush();


		reader = new ConsoleReader();
		reader.setBellEnabled(false);
		reader.setDebug(new PrintWriter(new FileWriter("writer.debug", true)));

		reader.addCompletor(new SimpleCompletor(JAliEnCOMMander.getCommandList()));
//		reader.addCompletor(new GridFileCompletor());
//		reader.addCompletor(new FileNameCompletor());


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

	/**
	 * get the formatted prompt for user+dir
	 */
	private String getPromptDisplay() {
		return JAliEnCOMMander.getUsername()
				+ promptPrefix +
				JAliEnCOMMander.getCurrentDirTilded()
				+ promptSuffix;
	}

	/**
	 * execute a command 
	 * @param args arguments of the command, first one is the command
	 */
	public void executeCommand(String args[]) {
	
		if (!"".equals(args[0])) {
			if (args[0].equals(".")) {
				String command = "";
				for (int c = 1; c < args.length; c++)
					command += args[c] + " ";
				syscall(command);
			} else if (args[0].equals("gbox")) {
				String command = "alien -s -e ";
				for (int c = 1; c < args.length; c++)
					command += args[c] + " ";
				syscall(command);
			} else if (args[0].equals("help")) {
				usage();
			} else
				JAliEnCOMMander.run(args);

		}
	}

	/**
	 * print some help message 
	 */
	public void usage() {
		out.println("JAliEn Grid Client, started in 2010, Version: " + JAliEnIAm.whatsVersion());
		out.println("Press <tab><tab> to see the available commands.");
	}

	@SuppressWarnings("unused")
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

	/**
	 * do a call to the underlying system shell
	 */
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
//
//	private void timingChallenge(ArrayList<String> args) {
//
//		System.out.println("args: " + args);
//		
//		
//		int times = 100;
//		String sTimes = args.get(0);
//		args.remove(0);
//		if(Integer.parseInt(sTimes)>1)
//			times = Integer.parseInt(sTimes);
//		
//		String command = args.get(0);
//		args.remove(0);
//		
//		ArrayList<Long> timings = new ArrayList<Long>(times);
//
//		
//		if (command.equals("ls")) {
//			for (int t = 0; t < times; t++) {
//				try {
//					JAliEnCommandls ls = new JAliEnCommandls(user, currentDir,
//							args);
//					ls.executeCommand();
//					timings.add(ls.timingChallenge);
//				} catch (Exception e1) {
//					e1.printStackTrace();
//				}
//			}
//		} else if (command.equals("get")) {
//			for (int t = 0; t < times; t++) {
//				try {
//					JAliEnCommandget get = new JAliEnCommandget(user,
//							currentDir, mySite, args);
//					get.isATimeChallenge = true;
//					get.executeCommand();
//					timings.add(get.timingChallenge);
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//			}
//
//		}
//
//		System.out.println("The row:" + timings);
//		Iterator<Long> it = timings.iterator();
//		long alltime = (long) 0;
//		float avr = 0;
//		while(it.hasNext()){
//			alltime = alltime + it.next();
//		}
//		avr = alltime / times;
//		avr = avr /1000;
//		
//		System.out.println(command + " with " +times+" tries, average: " + avr + ", total: " + alltime);
//
//	}

}
