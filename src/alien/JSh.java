package alien;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcessBuilder;
import sun.misc.Signal;
import sun.misc.SignalHandler;
import alien.config.ConfigUtils;
import alien.config.JAliEnIAm;
import alien.shell.BusyBox;
import alien.shell.ShellColor;
import alien.shell.commands.JAliEnBaseCommand;

/**
 * @author ron
 * @since Jun 21, 2011
 */
@SuppressWarnings("restriction")
public class JSh {
	
	
//	private class SigHandler implements SignalHandler{
//		public void handle(Signal sig) {
//            System.out.println("got SIGINT!");
//            if(boombox!=null)
//            	boombox.callJAliEnGetString("SIGINT");
//        }
//	}
	
	static {
		ConfigUtils.getVersion();
	}
	
	
	/**
	 * 
	 */
	static BusyBox boombox = null;
	
	private static boolean color = true;
	
	/**
	 * enable color output mode
	 */
	public static void color(){
		color = true;
	}
	
	/**
	 * disable color output mode
	 */
	public static void blackwhite(){
		color = false;
	}
	

	/**
	 * is color output mode enabled
	 * @return enabled or not
	 */
	public static boolean doWeColor(){
		return color;
	}
	
	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		 Signal.handle(new Signal("INT"), new SignalHandler () {
			    @Override
				public void handle(Signal sig) {
			      if(boombox!=null)
		            	boombox.callJBoxGetString("SIGINT");
			    }
			  });
		 Runtime.getRuntime().addShutdownHook(new Thread() {
		      @Override
			public void run() {
		    	  if(boombox!=null)
		    		  if(boombox.prompting()){
		    			if(appendOnExit)
		    			  		System.out.println("exit");  
		    	  			JSh.printGoodBye();
		    		  }
		      }
		    });
		    

		if(args.length>0 &&(("-h".equals(args[0])) || ("-help".equals(args[0])) ||
				("--h".equals(args[0])) || ("--help".equals(args[0]))
				|| ("help".equals(args[0]))))
			printHelp();
		else if (args.length>0 && ("-k".equals(args[0])))
			JSh.killJBox();
		else {

			
			if (JSh.JBoxRunning()){
				if(args.length>0 && "-e".equals(args[0])){
					color = false;
					boombox = new BusyBox(addr, port, password);
					if(boombox!=null){
						final StringTokenizer st = new StringTokenizer(joinSecondArgs(args),",");
						while (st.hasMoreTokens())
							boombox.callJBox(st.nextToken().trim());
					}
					else
						printErrConnJBox();
					
				}
				else
					boombox = new BusyBox(addr, port, password, user, true);
			}
			else
				printErrNoJBox();
		}
	}

	/**
	 * Trigger no 'exit\n' to be written out on exit
	 */
	static boolean appendOnExit = true;
	
	
    /**
     * Trigger no 'exit\n' to be written out on exit
     */
    public static void noAppendOnExit(){
    	appendOnExit = false;
    }
    
    /**
     * Trigger no 'exit\n' to be written out on exit
     */
    public static void appendOnExit(){
    	appendOnExit = true;
    }
	
	
	
	private static final String kill = "/bin/kill";
	//private static final String fuser = "/bin/fuser";

	private static String addr;
	private static String user;
	private static String password;
	private static int port = 0;
	private static int pid = 0;

	private static void startJBox() {
		if (!JSh.JBoxRunning()) {

			// APIServer.startJBox();

			final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(
					new String[] { "nohup", "./run.sh", "alien.JBox", "&" });

			pBuilder.returnOutputOnExit(false);
			pBuilder.redirectErrorStream(false);

			pBuilder.timeout(356, TimeUnit.DAYS);
			// try {
			// pBuilder.start();
			// } catch (Exception e) {
			// e.printStackTrace();
			// System.err.println("Could not start JBox.");
			// }
			System.out.println();
		}
		JSh.getJBoxPID();
	}

	private static void killJBox() {
		if (JSh.JBoxRunning()) {

			final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(new String[] { kill, pid + "" });

			pBuilder.returnOutputOnExit(true);
			pBuilder.timeout(2, TimeUnit.SECONDS);
			pBuilder.redirectErrorStream(true);
			final ExitStatus exitStatus;
			try {
				exitStatus = pBuilder.start().waitFor();
				if (exitStatus.getExtProcExitStatus() == 0)
					System.out.println("[" + pid + "] JBox killed.");
				else
					System.err.println("Could not kill the JBox, PID:" + pid);

			}
			catch (Throwable e) {
				System.err.println("Could not kill the JBox, PID:" + pid);
			}
		}
		else
			System.out.println("We didn't find any JBox running.");
	}

	private static boolean JBoxRunning() {

		if (JSh.getJBoxPID()) {
//			timeStamp("got JBOX PID: ");
//
//			if (!(new File(fuser)).exists())
//				return true;
//
//			if (port == 0) {
//				return false;
//			}
//
//			if (pid == 0)
//				return true; 
//
//			final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(
//					new String[] { fuser, port + "/tcp" });
//
//			pBuilder.returnOutputOnExit(true);
//			pBuilder.timeout(2, TimeUnit.SECONDS);
//			pBuilder.redirectErrorStream(true);
//			final ExitStatus exitStatus;
//			try {
//				exitStatus = pBuilder.start().waitFor();
//			} catch (Exception e) {
//				return false;
//			}
//			if (exitStatus.getExtProcExitStatus() == 0) {
//				String line[] = exitStatus.getStdOut().trim().split(":");
//				if (!line[0].trim().equals(port + "/tcp")
//						|| !line[1].trim().equals(pid + "")) {
//					return false;
//				}
//
//			} else {
//				return false;
//			}

			File f = new File("/proc/" + pid + "/cmdline");
			if (f.exists()) {
				String buffer = "";
				BufferedReader fi = null;
				try {
					fi = new BufferedReader(new InputStreamReader(
							new FileInputStream(f)));
					buffer = fi.readLine();
				} catch (IOException e) {
					return false;
				} finally {
					if (fi != null)
						try {
							fi.close();
						} catch (IOException e) {
							// ignore
						}
				}
				
				if (buffer!=null && buffer.contains("alien.JBox"))
					return true;
			}
		}
		return false;
	}

	private static boolean getJBoxPID() {

		File f = new File("/tmp/gclient_token_"+System.getProperty("userid"));

		if (f.exists()) {

			byte[] buffer = new byte[(int) f.length()];
			BufferedInputStream fi = null;
			try {
				fi = new BufferedInputStream(new FileInputStream(f));
				fi.read(buffer);
			} catch (IOException e) {
				port = 0;
				return false;
			} finally {
				if (fi != null)
					try {
						fi.close();
					} catch (IOException e) {
						// ignore
					}
			}

			String[] specs = new String(buffer).split("\n");

			for (String spec : specs) {
				String[] kval = spec.split("=");

				if (("Host").equals(kval[0].trim())) {
					addr = kval[1].trim();
				} else if (("Port").equals(kval[0].trim())) {
					try {
						port = Integer.parseInt(kval[1].trim());
					} catch (NumberFormatException e) {
						port = 0;
					}
				 } else if (("PID").equals(kval[0].trim())) {
						try {
							pid = Integer.parseInt(kval[1].trim());
						} catch (NumberFormatException e) {
							pid = 0;
						}
				} else if (("Passwd").equals(kval[0].trim())) {
					password = kval[1].trim();
				} else if (("User").equals(kval[0].trim())) {
					user = kval[1].trim();
				}
			}
			return true;
		}
		// else
		//	System.err.println("Token file does not exists.");
		return false;
	}
	

	/**
	 * @return BusyBox of JSh
	 * @throws IOException 
	 */
	public static BusyBox getBusyBox() throws IOException{
		getJBoxPID();
		return new BusyBox(addr, port, password);
	}

	/**
	 * @return PID of JBox
	 */
	public static int getPID(){
		return pid;
	}
	
	/**
	 * @return port of JBox
	 */
	public static int getPort(){
		return port;
	}
	
	
	/**
	 * @return addr of JBox
	 */
	public static String getAddr(){
		return addr;
	}
	
	/**
	 * @return pass of JBox
	 */
	public static String getPassword(){
		return password;
	}
	
	/**
	 * reconnect 
	 * @return success
	 */
	public static boolean reconnect(){
		
		return JSh.JBoxRunning();

	}
	
	
	private static String joinSecondArgs(final String[] args){
		final StringBuilder ret = new StringBuilder();
		
		for(int a=1;a<args.length;a++)
			ret.append(args[a]).append(' ');
		
		return ret.toString();
	}
	
	
	private static void printErrNoJBox(){
		printErr("JBox isn't running, so we won't start JSh.");
	}


	private static void printErrConnJBox(){
		printErr("Error connecting JBox.");
	}

	
	/**
	 * @param message
	 */
	public static void printErr(final String message){
		if(color)
			System.err.println(ShellColor.errorMessage() + message + ShellColor.reset());
		else 
			System.err.println(message);
	}
	
	/**
	 * @param message
	 */
	public static void printOut(final String message){
		if(color)
			System.out.println(ShellColor.infoMessage() + message + ShellColor.reset());
		else 
			System.err.println(message);
	}
	
	private static void printHelp(){
		System.out.println(JAliEnIAm.whatsMyFullName());
		System.out.println("Have a cup! Cheers, ACS");
		System.out.println();
		System.out.println(JAliEnBaseCommand.helpUsage("jsh", "[-options]"));
		System.out.println(JAliEnBaseCommand.helpStartOptions());
		System.out.println(JAliEnBaseCommand.helpOption("-e <cmd>[,<cmd>]","execute directly a comma separated list of commands"));
		System.out.println(JAliEnBaseCommand.helpOption("-h | -help", "this help"));
		System.out.println();
		System.out.println(JAliEnBaseCommand.helpParameter("more info to come."));
		System.out.println();
		
	}
	
	/**
	 * be polite 
	 */
	static void printGoodBye(){
		JSh.printOut("GoodBye.");
	}
	
}
