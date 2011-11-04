package alien.shell.commands;

import java.io.File;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import joptsimple.OptionException;
import alien.api.JBoxServer;
import alien.api.catalogue.CatalogueApiUtils;
import alien.api.taskQueue.TaskQueueApiUtils;
import alien.catalogue.LFN;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.user.AliEnPrincipal;
import alien.user.UsersHelper;


/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCOMMander extends Thread {
	
	private final static int maxTryConnect = 3;

	private int triedConnects = 0;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils
	.getLogger(JBoxServer.class.getCanonicalName());

	private final JBoxServer jbox;
	
	/**
	 *
	 */
	protected final CatalogueApiUtils c_api;
	
	/**
	 * 
	 */
	protected final TaskQueueApiUtils q_api;
	
	/**
	 * The commands that are advertised on the shell, e.g. by tab+tab
	 */
	private static final String[] commandList = new String[] {"shutdown", "ls", "get",
			"cat", "whoami", "roleami", "whereis", "cp", "cd", "rm", "time", "mkdir", "rmdir", "find",
			"scrlog", "submit", "ps","blackwhite","color", "masterjob", "user" , "role" , "kill"
			, "lfn2guid", "guid2lfn", "w", "uptime"};

	/**
	 * The commands that have a JAliEnCommand* implementation
	 */
	private static final String[] jAliEnCommandList = new String[] { "ls",
			"get", "cat", "whereis", "cp", "cd", "time", "mkdir", "find",
			"scrlog", "submit", "motd", "access", "commit", "pwd", "ps" , "rmdir", "rm",
			"masterjob","user", "role"  , "kill", "lfn2guid", "guid2lfn", "w", "uptime"};

	/**
	 * Commands to let UI talk internally with us here
	 */
	private static final String[] hiddenCommandList = new String[] { "whoami", "roleami",
			"cdir", "commandlist", "gfilecomplete", "cdirtiled" ,"blackwhite","color",
			"setshell"};

	private UIPrintWriter out = null;

	/**
	 * 
	 */
	protected AliEnPrincipal user;
	

	/**
	 * 
	 */
	protected String role;

	/**
	 * 
	 */
	protected String site;

	private String myHome;

	private HashMap<String, File> localFileCash;
	
	private boolean degraded = false;

	/**
	 * @param jbox
	 */
	public JAliEnCOMMander(final JBoxServer jbox) {
		this.jbox = jbox;

		c_api = new CatalogueApiUtils(this);

		q_api = new TaskQueueApiUtils(this);

		user = AuthorizationFactory.getDefaultUser();
		role = AliEnPrincipal.userRole();
		site = ConfigUtils.getConfig().gets("alice_close_site").trim();
		myHome = UsersHelper.getHomeDir(user.getName());
		localFileCash = new HashMap<String, File>();
		initializeJCentralConnection();

	}

	private boolean initializeJCentralConnection() {
		triedConnects++;
		try {
			curDir = c_api.getLFN(UsersHelper.getHomeDir(user.getName()));
			degraded = false;
		} catch (Exception e) {
			degraded = true;
		}
		return !degraded;
	}

	/**
	 * @param md5
	 * @param localFile
	 */
	protected void cashFile(String md5, File localFile) {
		localFileCash.put(md5, localFile);
	}

	/**
	 * @param md5
	 * @return local file name
	 */
	protected File checkLocalFileCache(String md5) {
		if (md5 != null && localFileCash.containsKey(md5))
			return localFileCash.get(md5);
		return null;
	}

	/**
	 * Debug level as the status
	 */
	protected int debug = 0;

	/**
	 * Current directory as the status
	 */
	protected LFN curDir;

	/**
	 * get list of commands
	 * 
	 * @return array of commands
	 */
	public static String getCommandList() {
		final StringBuilder commands = new StringBuilder();
		
		for (int i = 0; i < commandList.length; i++){
			if (i>0)
				commands.append(' ');
			
			commands.append(commandList[i]);
		}
		
		return commands.toString();
	}

	/**
	 * Get the user
	 * 
	 * @return user
	 */
	public AliEnPrincipal getUser() {
		return user;
	}

	/**
	 * Get the site
	 * 
	 * @return site
	 */
	public String getSite() {
		return site;
	}

	/**
	 * get the user's name
	 * 
	 * @return user name
	 */
	public String getUsername() {
		return user.getName();
	}
	
	/**
	 * get the user's role
	 * 
	 * @return user role
	 */
	public String getRole() {
		return role;
	}
	

	/**
	 * get the current directory
	 * 
	 * @return LFN of the current directory
	 */
	public LFN getCurrentDir() {
//		if (curDir == null)
//			curDir = c_api.getLFN(myHome);
		return curDir;

	}

	/**
	 * get the current directory as string
	 * 
	 * @return String of the current directory
	 */
	public String getCurrentDirName() {
		if(getCurrentDir()!=null)
			return getCurrentDir().getCanonicalName();
		return "[none]";
	}

	/**
	 * get the current directory, replace home with ~
	 * 
	 * @return name of the current directory, ~ places home
	 */
	public String getCurrentDirTilded() {

		return getCurrentDir().getCanonicalName()
				.substring(0, getCurrentDir().getCanonicalName().length() - 1)
				.replace(myHome.substring(0, myHome.length() - 1), "~");
	}


	private void setShellPrintWriter(OutputStream os, String shelltype) {
		if (shelltype.equals("jaliensh"))
			out = new JShPrintWriter(os);
		else
			out = new RootPrintWriter(os);
	}

	
	private OutputStream os = null;
	
	private String[] arg = null;
	
	private JAliEnBaseCommand jcommand = null;


	@Override
	public void run() {
		while (true) {
			
			if(out==null)
				if ("setshell".equals(arg[0]))
				 setShellPrintWriter(os, arg[1]);
			  else
				  out = new RootPrintWriter(os);

			if(degraded){
				 if(triedConnects<maxTryConnect){			
					 if(!initializeJCentralConnection())
						flush();
					 if(triedConnects<maxTryConnect){		
						try {
								Thread.sleep(triedConnects*maxTryConnect*500);							
						} catch (InterruptedException e) {
							//	e.printStackTrace();
						}
					 }
				 }else{
					 System.out.println("Giving up...");
					 break;
				 }
			} else {
				execute();
				try {
					synchronized (this) {
						wait();
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * @param os
	 * @param arg
	 */
	public void setLine(OutputStream os, String[] arg) {
		this.os = os;
		this.arg = arg;
	}
	
	
	/**
	 * execute a command line
	 * 
	 */
	public void execute() {

		boolean help = false;

		boolean silent = false;

		String comm = arg[0];

		System.out.println("Received JSh call [" + comm + "].");
		
		if (logger.isLoggable(Level.INFO)) {
			logger.log(Level.INFO, "Received JSh call [" + comm + "].");
		}

		ArrayList<String> args = new ArrayList<String>(Arrays.asList(arg));
		args.remove(arg[0]);

		for (int i = 1; i < arg.length; i++)
			if (arg[i].startsWith("-pwd=")) {
				curDir = c_api.getLFN(arg[i].substring(arg[i]
						.indexOf('=') + 1));
				args.remove(arg[i]);
			} else if (arg[i].startsWith("-debug=")) {
				try {
					debug = Integer.parseInt(arg[i].substring(arg[i]
							.indexOf('=') + 1));
				} catch (NumberFormatException n) {
					// ignore
				}
				args.remove(arg[i]);
			} else if ("-silent".equals(arg[i])) {
				silent = true;
				args.remove(arg[i]);
			} else if (("-h".equals(arg[i])) || ("--h".equals(arg[i]))
					|| ("-help".equals(arg[i])) || ("--help".equals(arg[i]))) {
				help = true;
				args.remove(arg[i]);
			}

		if (!Arrays.asList(jAliEnCommandList).contains(comm)) {

			if (Arrays.asList(hiddenCommandList).contains(comm)) {
					if ("commandlist".equals(comm))
					out.printOutln(getCommandList());
				else if ("whoami".equals(comm))
					out.printOutln(getUsername());
				else if ("roleami".equals(comm))
					out.printOutln(getRole());
				else if ("blackwhite".equals(comm))
					out.blackwhitemode();
				else if ("color".equals(comm))
					out.colourmode();
				//else if ("shutdown".equals(comm))
				//	jbox.shutdown();
			//} else if (!"setshell".equals(comm)) {
			} else
			out.printErrln("Command [" + comm + "] not found!");
			//}
		} else {

			final Object[] param = { this, out, args };

			try {
				jcommand = getCommand(comm, param);
			} catch (Exception e) {

				if(e.getCause() instanceof OptionException){
					out.printErrln("Wrong arguments.");
					out.flush();
				} else {
					out.printErrln("Error executing command [" + comm
							+ "] (Potentially class implementation not found).");
					e.printStackTrace();
				}
				out.flush();
				return;
			}

			if (silent)
				jcommand.silent();

			try {

				if (args.size() != 0
						&& args.get(args.size() - 1).startsWith("&")) {
					int logno = 0;
					if (args.get(args.size() - 1).length() > 1) {
						try {
							logno = Integer.parseInt(args.get(args.size() - 1)
									.substring(1));
						} catch (NumberFormatException n) {
							// ignore
						}
					}
					JAliEnCommandscrlog.addScreenLogLine(logno,
							"we will screen to" + logno);
					args.remove(args.size() - 1);
				}

				if (!help
						&& (args.size() != 0 || jcommand
								.canRunWithoutArguments())) {
					jcommand.start();
					try {
						synchronized (jcommand) {
							jcommand.wait();
						}
					} catch (InterruptedException e) {
						// ignore
					}
				} else {
					jcommand.printHelp();
				}
			} catch (Exception e) {
				e.printStackTrace();
				out.printErrln("Error executing the command [" + comm + "].");
			}
		}
		flush();
	}

	
	
	private void flush(){
		if(degraded){
			out.degraded();
			out.setenv(UsersHelper.getHomeDir(user.getName()),getUsername(),getRole());
		} else 
			out.setenv(getCurrentDirName(),getUsername(),getRole());
		out.flush();
	}
	
	/**
	 * 
	 */
	public void killRunningCommand(){
		if(jcommand!=null){
			synchronized (jcommand) {
				jcommand.interrupt();
				jcommand = null;
			}
			out.flush();
		}
	}

	/**
	 * create and return a object of
	 * alien.shell.commands.JAliEnCommand.JAliEnCommand<classSuffix>
	 * 
	 * @param classSuffix
	 *            the name of the shell command, which will be taken as the
	 *            suffix for the classname
	 * @param objectParm
	 *            array of argument objects, need to fit to the class
	 * @return an instance of
	 *         alien.shell.commands.JAliEnCommand.JAliEnCommand<classSuffix>
	 * @throws Exception
	 */
	protected static JAliEnBaseCommand getCommand(String classSuffix,
			Object[] objectParm) throws Exception {

		@SuppressWarnings("rawtypes")
		Class cl = Class.forName("alien.shell.commands.JAliEnCommand"
				+ classSuffix);
		@SuppressWarnings({ "rawtypes", "unchecked" })
		java.lang.reflect.Constructor co = cl.getConstructor((new Class[] {
				JAliEnCOMMander.class, UIPrintWriter.class, ArrayList.class }));
		return (JAliEnBaseCommand) co.newInstance(objectParm);
	}
}
