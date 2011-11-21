package alien.shell;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Arrays;
import java.util.StringTokenizer;

import jline.ArgumentCompletor;
import jline.Completor;
import jline.ConsoleReader;
import jline.SimpleCompletor;
import alien.JSh;
import alien.config.JAliEnIAm;
import alien.shell.commands.JShPrintWriter;

/**
 * @author ron
 * @since Feb, 2011
 */
public class BusyBox {

	private final static int tryreconnect = 1;
	
	
	private int remainreconnect = tryreconnect;

	
	private static final String noSignal = String.valueOf((char) 33);

	private static int pender = 0;
	private static boolean pending = false;
	private static final String[] pends = {".   "," .  ","  . ","   ."};
	
	private static final String promptPrefix =  JAliEnIAm.whatsMyName()+ JAliEnIAm.myJShPrompt() + " ";

	private static final String promptColorPrefix =  ShellColor.boldBlack() + JAliEnIAm.whatsMyName() + ShellColor.reset() + JAliEnIAm.myJShPrompt() + " ";

	
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
		System.out.println("Hello " + username + ",");
		System.out.println();
		System.out.println("Welcome to " + JAliEnIAm.whatsMyFullName());
		System.out.println("Have a cup! Cheers, ACS");
		System.out.println();
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
		String currentDirTemp = this.currentDir;
		
		if(JSh.reconnect())
			if(connect(JSh.getAddr(), JSh.getPort(), JSh.getPassword()))
				if(callJBox("user " + whoami))
					if(callJBox("role " + roleami))
						if(callJBox("cd " + currentDirTemp)){
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
		this(addr,port,password,null,false);
		
	}

	/**
	 * the JAliEn busy box
	 * 
	 * @param addr
	 * @param port
	 * @param password
	 * @param username 
	 * @param startPrompt
	 * 
	 * @throws IOException
	 */
	public BusyBox(String addr,int port, final String password, final String username, final boolean startPrompt) throws IOException {
		
		out = new PrintWriter(System.out);
		
		if(startPrompt){
			this.username = username;
			welcome();
			out.flush();
			prompting = true;
		}
		
		if (!connect(addr, port, password)) {
			printInitConnError();
			throw new IOException();
		}
		
		if(startPrompt){
			new Thread(){
				@Override
				public void run() {
					try {
						prompt();
					}
					catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}.start();
		}
	}

	private static String genPromptPrefix(){
		if(JSh.doWeColor())
			return promptColorPrefix;
		
		return promptPrefix;
	}
	
	/**
	 * loop the prompt for the user
	 * 
	 * @throws IOException
	 */
	public void prompt() throws IOException {

		String line;

		reader = new ConsoleReader();
		reader.setBellEnabled(false);
		reader.setDebug(new PrintWriter(
				new FileWriter("writer.debug", true)));
		Completor[] comp = new Completor[]{
				
	            new SimpleCompletor(callJBoxGetString("commandlist").split(" ")),
	            new GridLocalFileCompletor(this)
	        };
	    reader.addCompletor (new ArgumentCompletor(comp));

		String prefixCNo = "0";
		while ((line = reader.readLine(genPromptPrefix() + "[" + prefixCNo + commNo
				+ "] " + currentDir + promptSuffix)) != null) {

			if (commNo == 9)
				prefixCNo = "";

			out.flush();
			line = line.trim();

			if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
				JSh.noAppendOnExit();
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
	public String callJBoxGetString(final String line){		
		String sline = line;
		
		checkColorSwitch(sline);
		
		do {
			try {

				if (socketThere(s)) {
					// line +="\n";
					sline = sline.replace(" ", JShPrintWriter.SpaceSep) + JShPrintWriter.lineTerm;

					os.write(sline.getBytes());
					os.flush();

					BufferedReader br = new BufferedReader(
							new InputStreamReader(is));

					StringBuilder ret = new StringBuilder();
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
						else{
							if (ret.length()>0)
								ret.append('\n');
							
							ret.append(sLine);
						}
					}

					if (signal)
						return ret.toString();
				}

			} catch (Exception e) {
				//e.printStackTrace();
			}
		} while (reconnect());

		printConnError();
		return noSignal;
	}
	
	

	/**
	 * @param line
	 * @return success of the call
	 */
	public boolean callJBox(final String line) {
		return callJBox(line, true);
	}

	private boolean callJBox(final String line, final boolean tryReconnect) {
		String sline = line;
		
		checkColorSwitch(sline);
		
		do {
			try {

				if (socketThere(s)) {
					// line +="\n";
					sline = sline.replace(" ", JShPrintWriter.SpaceSep) + JShPrintWriter.lineTerm;

					os.write(sline.getBytes());
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
						
						if (JShPrintWriter.pendSignal.equals(sLine)){
							pending=true;
							//System.out.write(("\rI/O ["+ pends[pender] + "]").getBytes());
							System.out.print("\rI/O ["+ pends[pender] + "]");
							//out.flush();
							pender++;
							if(pender>=pends.length)
								pender = 0;
							continue;
						}
						
						if(pending){
							pending=false;
							pender = 0;
							//System.out.write("\r".getBytes());
							System.out.print("\r                                        \r");
						}
						
						if (sLine.startsWith(JShPrintWriter.errTag)){
							JSh.printErr("Error: " + sLine.substring(1));
						}
						else if (sLine.startsWith(JShPrintWriter.outputterminator))
							updateEnvironment(sLine);
						else if (sLine.endsWith(JShPrintWriter.lineTerm)){
							break;
						}else {
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
				final StringBuilder command = new StringBuilder();
				
				for (int c = 1; c < args.length; c++)
					command.append(args[c]).append(' ');
				
				syscall(command.toString());
			} else if (args[0].equals("gbbox")) {
				StringBuilder command = new StringBuilder("alien -s -e ");
				for (int c = 1; c < args.length; c++)
					command.append(args[c]).append(' ');
				
				syscall(command.toString());
			} else if (isEditCommand(args[0])){
				if(args.length==2)
					editCatalogueFile(args[0],args[1]);
				else
					out.println("help for the editor is....");
			} else if(args[0].equals("shutdown"))
					shutdown();
			else if (args[0].equals("help")) {
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


	/**
	 * do a call to the underlying system shell
	 */
	private static void syscall(String command) {

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

			BufferedReader brCleanUp = new BufferedReader(new InputStreamReader(stdout));
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
	

	
	private static void checkColorSwitch(final String line){
		if("blackwhite".equals(line))
			JSh.blackwhite();
		
		else if("color".equals(line))
			JSh.color();
	}

	private static boolean socketThere(Socket s){
		return (!s.isClosed() && s.isBound() &&
		s.isConnected() && !s.isInputShutdown()
		&& !s.isOutputShutdown());
	}
	
	
	private void shutdown(){
		try {
			System.out.print("Shutting down jBox...");
			if (socketThere(s)) {
				os.write(("shutdown" + JShPrintWriter.lineTerm).getBytes());
				os.flush();
				
				//TODO: How to tell that jBox was killed successfully
//				if(socketThere(s)) 
					System.out.println("DONE.");
//				else{
//					System.out.println("ERROR.");
//					System.out.println("JBox might still be running.");
//				}
			}
		} catch (Exception e) {
				//e.printStackTrace();
		}
		JSh.printGoodBye();
		System.exit(0);
	}
	
	
	private static boolean isEditCommand(final String command) {

		return Arrays.asList(FileEditor.editors).contains(command);
	}

	private void editCatalogueFile(final String editcmd, final String LFNName) {

		String localFile = callJBoxGetString("cp -t " + LFNName);
		localFile = localFile.replace("\n", "")
				.replace("Downloaded file to ", "").trim();

		long lastMod = (new File(localFile)).lastModified();

		FileEditor editor = null;
		
		try{
			editor = new FileEditor(editcmd);
		}
		catch (IOException e){
			JSh.printErr("The editor [" + editcmd + "] was not found on your system.");
			return;
		}
		
		editor.edit(localFile);

		if ((new File(localFile)).lastModified() != lastMod)
			callJBox("cp file:" + localFile + " " + LFNName + "new");

	}
	
	
	
	
	private static void printInitConnError(){
		JSh.printErr("Could not connect to JBox.");
	}
	
	private static void printErrShutdown(){
		JSh.printErr("Shutting down...");
	}
	
	private static void printErrRestartJBox(){
		JSh.printErr("JBox seems to be dead, please restart it.");
	}
	
	private static void printConnError(){
		JSh.printErr("Connection to JBox interrupted.");
	}

	private static void printJCentralConnError(){
		JSh.printErr("Connection error to JCentral.");
	}


}
