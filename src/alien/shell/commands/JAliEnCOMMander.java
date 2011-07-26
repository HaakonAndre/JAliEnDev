package alien.shell.commands;

import java.io.File;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import alien.api.catalogue.CatalogueApiUtils;
import alien.catalogue.LFN;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.user.AliEnPrincipal;
import alien.user.UsersHelper;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCOMMander {
	
	

	/**
	 * The commands that are advertised on the shell, e.g. by tab+tab
	 */
	private static final String[] commandList = new String[] { "ls", "get",
			"cat","whoami", "whereis", "cp", "cd", "time", "mkdir", "find",
			"scrlog", "submit" };
	
	/**
	 * The commands that have a JAliEnCommand* implementation
	 */
	private static final String[] jAliEnCommandList = new String[] { "ls", "get",
		"cat",  "whereis",  "cp", "cd", "time", "mkdir", "find",
		"scrlog", "submit" };
	
	/**
	 * Commands to let UI talk internally with us here
	 */
	private static final String[] hiddenCommandList = new String[] { "whoami",
			"cdir", "commandlist", "gfilecomplete", "cdirtiled" };

	private UIPrintWriter out = null;

	/**
	 * 
	 */
	protected AliEnPrincipal user = AuthorizationFactory.getDefaultUser();

	/**
	 * 
	 */
	protected String site = ConfigUtils.getConfig().gets("alice_close_site")
			.trim();

	private String myHome = UsersHelper.getHomeDir(user.getName());
	
	private HashMap<String,File> localFileCash = new HashMap<String,File>();
	
	
	/**
	 * @param md5
	 * @param localFile
	 */
	protected void cashFile(String md5, File localFile){
		localFileCash.put(md5, localFile);
	}

	/**
	 * @param md5
	 * @return local file name
	 */
	protected File checkLocalFileCache(String md5){
		if(md5!=null && localFileCash.containsKey(md5))
			return localFileCash.get(md5);
		return null;
	}
	

	/**
	 * 
	 */
	protected LFN curDir = null;

	/**
	 * get list of commands
	 * 
	 * @return array of commands
	 */
	public static String getCommandList() {
		String commands = "";
		for(int i=0;i<commandList.length;i++)
			commands += commandList[i]+" ";
		return commands;
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
	 * get the current directory
	 * 
	 * @return LFN of the current directory
	 */
	public LFN getCurrentDir() {
		if (curDir == null)
			curDir = CatalogueApiUtils.getLFN(myHome);
		return curDir;

	}

	/**
	 * get the current directory as string
	 * 
	 * @return String of the current directory
	 */
	public String getCurrentDirName() {
		return getCurrentDir().getCanonicalName();
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

	private String gridFileCompletetion(ArrayList<String> args) {
		// TODO:
		return "";
	}

	private void setShellPrintWriter(OutputStream os, String shelltype) {
		if (shelltype.equals("jaliensh"))
			out = new JAliEnShPrintWriter(os);
		else
			out = new RootPrintWriter(os);
	}

	/**
	 * execute a command line
	 * @param os 
	 * @param arg 
	 */
	public void execute(OutputStream os, String[] arg) {

		String comm = arg[0];

		System.out.println("Command [" + comm + "] called!");
		
		//if ("setshell".equals(comm)) {
		//	setShellPrintWriter(os, arg[1]);
		//} else if (out == null) {
			out = new RootPrintWriter(os);
		//}

		ArrayList<String> args = new ArrayList<String>(Arrays.asList(arg));
		args.remove(arg[0]);

		if (!Arrays.asList(jAliEnCommandList).contains(comm)) {

			if (Arrays.asList(hiddenCommandList).contains(comm)) {
				if ("cdir".equals(comm))
					out.printOutln(getCurrentDirName());
				else if ("cdirtiled".equals(comm))
					out.printOutln(getCurrentDirTilded());
				else if ("commandlist".equals(comm))
					out.printOutln(getCommandList());
				else if ("gfilecomplete".equals(comm))
					out.printOutln(gridFileCompletetion(args));
				else if ("whoami".equals(comm))
					out.printOutln(getUsername());
			} else if (!"setshell".equals(comm)){
				out.printErrln("Command [" + comm + "] not found!");
			}
		} else {

			final Object[] param = { this, out, args };
			JAliEnBaseCommand jcommand = null;
			try {
				jcommand = getCommand(comm, param);
			} catch (Exception e) {
				e.printStackTrace();
				out.printErrln("Command [" + comm
						+ "] not found! (Class implementation not found.)");
				out.flush();
				return;
			}
			try {
				if (args.contains("-s")) {
					jcommand.silent();
					// TODO: we have to drop -s here
				}
				if (args.size() != 0
						&& args.get(args.size() - 1).startsWith("&")) {
					int logno = 0;
					if (args.get(args.size() - 1).length() > 1) {
						try {
							logno = Integer.parseInt(args.get(args.size() - 1)
									.substring(1));
						} catch (NumberFormatException n) {
							//ignore
						}
					}
					JAliEnCommandscrlog.addScreenLogLine(logno,
							"we will screen to" + logno);
					args.remove(args.size() - 1);
				}

				if (!args.contains("--help")
						&& !args.contains("-help")
						&& !args.contains("-h")
						&& (args.size() != 0 || jcommand
								.canRunWithoutArguments())) {
					jcommand.execute();
				} else {
					jcommand.printHelp();
				}
			} catch (Exception e) {
				e.printStackTrace();
				out.printErrln("Error executing the command [" + comm
						+ "].");
			}
		}
		out.setenv(getCurrentDirName());
		out.flush();
	}

	/**
	 * create and return a object of
	 * alien.shell.commands.JAliEnCommand.JAliEnCommand<classSuffix>
	 * @param classSuffix the
	 *            name of the shell command, which will be taken as the suffix
	 *            for the classname
	 * @param objectParm array
	 *            of argument objects, need to fit to the class
	 * @return an instance of
	 *         alien.shell.commands.JAliEnCommand.JAliEnCommand<classSuffix>
	 * @throws Exception 
	 */
	protected static JAliEnBaseCommand getCommand(String classSuffix,
			Object[] objectParm) throws Exception {

		@SuppressWarnings("rawtypes")
		Class cl = Class.forName("alien.shell.commands.JAliEnCommand"
		// Class cl =
		// Class.forName(JAliEnCOMMander.class.getPackage().toString() +
		// ".JAliEnCommand"
				+ classSuffix);
		@SuppressWarnings({ "rawtypes", "unchecked" })
		java.lang.reflect.Constructor co = cl.getConstructor((new Class[] {
				JAliEnCOMMander.class, UIPrintWriter.class, ArrayList.class }));
		return (JAliEnBaseCommand) co.newInstance(objectParm);
	}
}
