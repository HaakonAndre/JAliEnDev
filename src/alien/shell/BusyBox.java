package alien.shell;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;

import jline.ConsoleReader;
import jline.SimpleCompletor;
import alien.config.JAliEnIAm;
import alien.shell.commands.JAliEnShPrintWriter;
import alien.taskQueue.Job;
import alien.taskQueue.TaskQueueUtils;

/**
 * @author ron
 * @since Feb, 2011
 */
public class BusyBox {

	private static final String promptPrefix = "/" + JAliEnIAm.myPromptName()
			+ " ";
	private static final String promptSuffix = " > ";

	private ConsoleReader reader;
	private PrintWriter out;

	private String currentDir;

	/**
	 * print welcome
	 */
	public void welcome() {
		out.println("Hello " + callJAliEnGetString("whoami") + ",");
		out.println(JAliEnIAm.whatsMyFullName() + ", have a cup!");
		out.println();
		out.println("Cheers, AJs aka ACS");
		out.println();

	}

	private Socket s = null;

	private InputStream is;

	private OutputStream os;

	
	
	private void connect(String addr, int port, String password) {



		if (addr!=null && port != 0 && password!=null) {

			try {
				s = new Socket(addr, port);

				is = s.getInputStream();
				os = s.getOutputStream();
				
				os.write(password.getBytes());
				os.flush();
			} catch (IOException e) {
				System.err.println("Could not connect to API Service.");
			}
		}
	}

	
	@SuppressWarnings("unused")
	private void reconnect(){
		// TODO:
	}
	
	
	/**
	 * the JAliEn busy box
	 * @param addr 
	 * @param port 
	 * @param password 
	 * 
	 * @throws IOException
	 */
	public BusyBox(String addr,int port, String password) throws IOException {

		connect(addr, port, password);

		if (s != null) {
			out = new PrintWriter(System.out);

			currentDir = callJAliEnGetString("cdir");

			welcome();
			out.flush();

			reader = new ConsoleReader();
			reader.setBellEnabled(false);
			reader.setDebug(new PrintWriter(
					new FileWriter("writer.debug", true)));

			reader.addCompletor(new SimpleCompletor(
					callJAliEnGetString("commandlist")));
			// reader.addCompletor(new GridFileCompletor());
			// reader.addCompletor(new FileNameCompletor());

			String line;
			while ((line = reader.readLine(getPromptDisplay())) != null) {

				out.flush();
				line = line.trim();

				if (line.equalsIgnoreCase("quit")
						|| line.equalsIgnoreCase("exit")) 
					break;
				

				executeCommand(line);

			}
		}
	}

	/**
	 * get the formatted prompt for user+dir
	 */
	private String getPromptDisplay() {
		return callJAliEnGetString("whoami") + promptPrefix
				+ callJAliEnGetString("cdirtiled") + promptSuffix;
	}

	private String callJAliEnGetString(String line) {
		try {
			line +="\n";
			callJAliEn("setshell jaliensh");

			os.write(line.getBytes());
			os.flush();
			

			BufferedReader br = new BufferedReader(new InputStreamReader(is));

			String ret = "";	
			String sLine;
			
			while ( (sLine = br.readLine()) != null ){
				if(JAliEnShPrintWriter.lastLineTag.equals(sLine))
					break;
				ret += sLine + "\n";
			}
			if(ret.length()>0)
				ret = ret.substring(0,ret.length()-1);

			return ret;

		} catch (IOException e) {
			e.printStackTrace();
		}
		return "";
	}

	private void callJAliEn(String line){
		try {

			line +="\n";
			os.write(line.getBytes());
			os.flush();

			BufferedReader br = new BufferedReader(new InputStreamReader(is));

			String sLine;
			while ( (sLine = br.readLine()) != null ){
				if(JAliEnShPrintWriter.lastLineTag.equals(sLine))
					break;
				if(sLine.startsWith(JAliEnShPrintWriter.errTag))
					System.err.println("Error: "+ sLine.substring(JAliEnShPrintWriter.errTag.length()));
				else {
					out.println(sLine);
					out.flush();
				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * execute a command
	 * @param callLine 
	 *            arguments of the command, first one is the command
	 */
	public void executeCommand(String callLine) {

		String args[] = callLine.split("\\s");

		if (!"".equals(args[0])) {
			if (args[0].equals(".")) {
				String command = "";
				for (int c = 1; c < args.length; c++)
					command += args[c] + " ";
				syscall(command);
			} else if (args[0].equals("gbbox")) {
				String command = "alien -s -e ";
				for (int c = 1; c < args.length; c++)
					command += args[c] + " ";
				syscall(command);
			} else if (args[0].equals("help")) {
				usage();
			} else {
				callJAliEn("setshell jaliensh");
				callJAliEn("setshell jaliensh");
				callJAliEn("cd " + currentDir);
				callJAliEn(callLine);
			}
		}
	}

	/**
	 * print some help message
	 */
	public void usage() {
		out.println("JAliEn Grid Client, started in 2010, Version: "
				+ JAliEnIAm.whatsVersion());
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

}
