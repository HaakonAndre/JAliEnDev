package alien.shell.commands;

import java.util.ArrayList;
import java.util.Arrays;

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

	private static final String[] commandList = new String[] { "ls", "get",
			"cat", "whoami", "whereis", "cp", "cd", "time", "mkdir", "find",
			"scrlog","submit" };

	protected static AliEnPrincipal user = AuthorizationFactory
			.getDefaultUser();

	protected static String site = ConfigUtils.getConfig()
			.gets("alice_close_site").trim();

	private static String myHome = UsersHelper.getHomeDir(user.getName());

	protected static LFN curDir = null;

	/**
	 * get list of commands
	 * 
	 * @return array of commands
	 */
	public static String[] getCommandList() {
		return commandList;
	}

	/**
	 * Get the user
	 * 
	 * @return user
	 */
	public static AliEnPrincipal getUser() {
		return user;
	}

	/**
	 * Get the site
	 * 
	 * @return site
	 */
	public static String getSite() {
		return site;
	}

	/**
	 * get the user's name
	 * 
	 * @return username
	 */
	public static String getUsername() {
		return user.getName();
	}

	/**
	 * get the current directory
	 * 
	 * @return LFN of the current directory
	 */
	public static LFN getCurrentDir() {
		if (curDir == null)
			curDir = CatalogueApiUtils.getLFN(myHome);
		return curDir;

	}

	/**
	 * get the current directory, replace home with ~
	 * 
	 * @return name of the current directory, ~ places home
	 */
	public static String getCurrentDirTilded() {

		return getCurrentDir().getCanonicalName()
				.substring(0, getCurrentDir().getCanonicalName().length() - 1)
				.replace(myHome.substring(0, myHome.length() - 1), "~");
	}

	/**
	 * run a signature/line from the prompt
	 * 
	 * @param args
	 *            first element is command, which needs to be in command list,
	 *            following entries are arguments
	 */
	public static void run(String[] args) {
		ArrayList<String> argList = new ArrayList<String>(Arrays.asList(args));
		argList.remove(args[0]);
		try {
			execute(args[0], argList);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Command failed. See above.");
		}
	}

	/**
	 * execute a command line
	 * 
	 * @param the
	 *            command, which needs to be in command list and will search for
	 *            class as suffix, with JAliEnCommand<command>
	 * @param the
	 *            list of arguments
	 */
	private static void execute(String comm, ArrayList<String> args) {
		if (!Arrays.asList(commandList).contains(comm)) {

			System.err.println("Command [" + comm + "] not found!");
			return;
		}

		final Object[] param = { args };
		JAliEnBaseCommand jcommand = null;
		try {
			jcommand = getCommand(comm, param);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Command [" + comm
					+ "] not found! (Class implementation not found.)");
		}
		try {
			if (args.contains("-s")) {
				jcommand.silent();
				// TODO: we have to drop -s here
			}
			if (args.size() != 0 && args.get(args.size() - 1).startsWith("&")) {
				int logno = 0;
				if (args.get(args.size() - 1).length() > 1) {
					try {
						logno = Integer.parseInt(args.get(args.size() - 1)
								.substring(1));
					} catch (NumberFormatException n) {
					}
				}
				JAliEnCommandscrlog.addScreenLogLine(logno,
						"we will screen to" + logno);
				args.remove(args.size() - 1);
			}

			if (!args.contains("--help") && !args.contains("-help")
					&& !args.contains("-h")
					&& (args.size() != 0 || jcommand.canRunWithoutArguments())) {
				jcommand.execute();
			} else {
				jcommand.printHelp();
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error executing the command [" + comm + "].");
		}
	}

	/**
	 * create and return a object of
	 * alien.shell.commands.JAliEnCommand.JAliEnCommand<classSuffix>
	 * 
	 * @param the
	 *            name of the shell command, which will be taken as the suffix
	 *            for the classname
	 * @param array
	 *            of argument objects, need to fit to the class
	 * @return an instance of
	 *         alien.shell.commands.JAliEnCommand.JAliEnCommand<classSuffix>
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
		java.lang.reflect.Constructor co = cl
				.getConstructor((new Class[] { ArrayList.class }));
		return (JAliEnBaseCommand) co.newInstance(objectParm);
	}
}
