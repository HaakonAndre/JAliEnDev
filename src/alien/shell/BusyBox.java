package alien.shell;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.StringTokenizer;

import jline.ArgumentCompletor;
import jline.Completor;
import jline.ConsoleReader;
import jline.SimpleCompletor;
import alien.JSh;
import alien.config.JAliEnIAm;
import alien.shell.commands.JShPrintWriter;
import alien.taskQueue.Job;
import alien.taskQueue.TaskQueueUtils;

/**
 * @author ron
 * @since Feb, 2011
 */
public class BusyBox {

	private final static int tryreconnect = 1;
	
	
	private int remainreconnect = tryreconnect;

	
	private static final String noSignal = String.valueOf((char) 33);

	private static int pender = 0;
	private static final String[] pends = {".   "," .  ","  . ","   ."};
	
	private static final String promptPrefix =  ShellColor.boldBlack() + JAliEnIAm.whatsMyName() + ShellColor.reset() + JAliEnIAm.myJShPrompt() + " ";
	private static final String promptSuffix = " > ";
	
	private static int commNo = 1;

	private ConsoleReader reader;
	private PrintWriter out;

	private boolean prompting = false;
	
	private String username;
	private String role;
	
	private String currentDir;
	

	
	/**
	 * 
	 * @return the current directory
	 */
	public String getCurrentDir(){
		return currentDir;
	}
	
	/**
	 * print welcome
	 */
	public void welcome() {
		out.println("Hi jedi " + username + ",");
		out.println("this is " + JAliEnIAm.whatsMyFullName() + ".");
		out.println("Have a cup! Cheers, ACS");
		out.println();
	}

	private Socket s = null;

	private InputStream is;

	private OutputStream os;
	

	
	
	private boolean connect(String addr, int port, String password) {



		if (addr!=null && port != 0 && password!=null) {

			try {
				s = new Socket(addr, port);

				is = s.getInputStream();
				os = s.getOutputStream();
				
				os.write((password+JShPrintWriter.lineTerm).getBytes());
				os.flush();
				
				return (!noSignal.equals(callJBoxGetString("setshell jaliensh")));
			} catch (IOException e) {
				return false;
			}
		}
		return false;
	}

	
	private boolean reconnect(){
		if(remainreconnect<1){
			printErrShutdown();
			callJBox("shutdown",false);
			Runtime.getRuntime().exit(1);
		}
		remainreconnect--;
		
		String whoami = this.username;
		String roleami = this.role;
		String currentDir = this.currentDir;
		
		if(JSh.reconnect())
			if(connect(JSh.getAddr(), JSh.getPort(), JSh.getPassword()))
				if(callJBox("user " + whoami))
					if(callJBox("role " + roleami))
						if(callJBox("cd " + currentDir)){
							remainreconnect = tryreconnect;
							return true;
						}
							
	
		printErrRestartJBox();
		return false;
	
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

		if (connect(addr, port, password)) {
			out = new PrintWriter(System.out);

			reader = new ConsoleReader();
			reader.setBellEnabled(false);
			reader.setDebug(new PrintWriter(
					new FileWriter("writer.debug", true)));

			Completor[] comp = new Completor[]{
		            new SimpleCompletor(callJBoxGetString("commandlist").split(" ")),
		            new GridLocalFileCompletor(this)
		        };
		    reader.addCompletor (new ArgumentCompletor(comp));

		} else {
			printInitConnError();
			throw new IOException();
		}
	}

	/**
	 * loop the prompt for the user
	 * 
	 * @throws IOException
	 */
	public void prompt() throws IOException {

		welcome();
		out.flush();
		prompting = true;

		String line;

		String prefixCNo = "0";
		while ((line = reader.readLine(promptPrefix + "[" + prefixCNo + commNo
				+ "] " + currentDir + promptSuffix)) != null) {

			if (commNo == 9)
				prefixCNo = "";

			out.flush();
			line = line.trim();

			if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
				printGoodBye();
				break;
			}

			executeCommand(line);
			commNo++;

		}
	}

	/**
	 * @param line
	 * @return response from JBox
	 */
	public String callJBoxGetString(String line) {
		do {
			try {

				if (socketThere(s)) {
					// line +="\n";
					line = line.replace(" ", JShPrintWriter.SpaceSep) + JShPrintWriter.lineTerm;

					os.write(line.getBytes());
					os.flush();

					BufferedReader br = new BufferedReader(
							new InputStreamReader(is));

					String ret = "";
					String sLine = null;
					boolean signal = false;

					while ((sLine = br.readLine()) != null) {
						
						if (sLine.startsWith(JShPrintWriter.degradedSignal)){
							printJCentralConnError();
							break;
						}
						signal = true;
						if (sLine.startsWith(JShPrintWriter.outputterminator))
							updateEnvironment(sLine);
						else if (sLine.endsWith(JShPrintWriter.streamend))
							break;
						else
							ret += sLine + "\n";
					}

					if (ret.length() > 0)
						ret = ret.substring(0, ret.length() - 1);
					if (signal)
						return ret;
				}

			} catch (Exception e) {
				//e.printStackTrace();
			}
		} while (reconnect());

		printConnError();
		return noSignal;
	}
	
	

	private boolean callJBox(final String line) {
		return callJBox(line, true);
	}

	private boolean callJBox(String line, final boolean tryReconnect) {
		do {
			try {

				if (socketThere(s)) {
					// line +="\n";
					line = line.replace(" ", JShPrintWriter.SpaceSep) + JShPrintWriter.lineTerm;

					os.write(line.getBytes());
					os.flush();

					BufferedReader br = new BufferedReader(
							new InputStreamReader(is));

					String sLine;
					boolean signal = false;
					
					while ((sLine = br.readLine()) != null) {
						
						if (sLine.startsWith(JShPrintWriter.degradedSignal)){
							printJCentralConnError();
							break;
						}
						
						signal = true;
						if (sLine.startsWith(JShPrintWriter.outputterminator))
							updateEnvironment(sLine);
						else if (sLine.endsWith(JShPrintWriter.pendSignal))
							pending(br);
						else if (sLine.endsWith(JShPrintWriter.lineTerm))
							break;
						else if (sLine.startsWith(JShPrintWriter.errTag))
							JSh.printErr("Error: " + sLine.substring(1));
						else {
							out.println(sLine);
							out.flush();
						}

					}
					
					if (signal)
						return true;
				}

			} catch (Exception e) {
				// ignore
				//e.printStackTrace();
			}
		} while (tryReconnect && reconnect());

		if(tryReconnect)
			printConnError();
		return false;
	}

	
	private void pending(BufferedReader br){
		String sLine;
		try {
			while ( (sLine = br.readLine()) != null ){
				if(!sLine.endsWith(JShPrintWriter.pendSignal))
					break;
				System.out.print("\rI/O ["+ pends[pender] + "]");
				pender++;
				if(pender>=pends.length)
					pender = 0;
			}
		} catch (IOException e) {
			// ignore
		}
		finally{
			pender = 0;
			System.out.print("\r");
		}
	}
	

	
	private void updateEnvironment(String env){

			final StringTokenizer st = new StringTokenizer(env.substring(1),JShPrintWriter.fieldseparator);
			
			if(st.hasMoreTokens())
					currentDir = st.nextToken();
			if(st.hasMoreTokens())
					username = st.nextToken();
			if(st.hasMoreTokens())
				role = st.nextToken();
	}

	
	/**
	 * execute a command
	 * @param callLine 
	 *            arguments of the command, first one is the command
	 */
	public void executeCommand(String callLine) {

		//String args[] = callLine.split(SpaceSep);
		String args[] = callLine.split(" ");
		
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
			} else if (args[0].equals("edit")) {
				
				
				System.out.println("Sorry, we're still working on [edit] ...");
//				String localFile  = callJAliEnGetString("cp -t "+ args[1]);
//				localFile = localFile.replace("\n", "").replace("Downloaded file to ", "").trim();
//				System.out.println("received: " + localFile);
//				ShellEditor.editOnConsole(localFile);
				
				
				
				
				//syscall(Functions.which("vim")+" "+localFile);
//				try {
//					Runtime.getRuntime().exec(Functions.which("vim")+" "+localFile);
//					Console console = new 
//					 new console.SystemShell();
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
			} else if (args[0].equals("help")) {
				usage();
			} else {
				callJBox(callLine);
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
	
	
	/**
	 * true once running the prompt
	 * @return are we running a prompt
	 */
	public boolean prompting(){
		return prompting;
	}
	
	
	private static boolean socketThere(Socket s){
		return (!s.isClosed() && s.isBound() &&
		s.isConnected() && !s.isInputShutdown()
		&& !s.isOutputShutdown());
	}
	
	private void printInitConnError(){
		JSh.printErr("Could not connect to JBox.");
	}
	
	private void printErrShutdown(){
		JSh.printErr("Shutting down...");
		printGoodBye();
	}
	
	
	private void printGoodBye(){
		JSh.printOut("GoodBye.");
	}
	
	private void printErrRestartJBox(){
		JSh.printErr("JBox seems to be dead, please restart it.");
	}
	
	private void printConnError(){
		JSh.printErr("Connection to JBox interrupted.");
	}

	
	private void printJCentralConnError(){
		JSh.printErr("Connection error to JCentral.");
	}


}
