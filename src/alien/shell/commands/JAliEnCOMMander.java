package alien.shell.commands;

import java.util.ArrayList;
import java.util.Arrays;

import alien.catalogue.LFN;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.ui.api.CatalogueApiUtils;
import alien.user.AliEnPrincipal;
import alien.user.UsersHelper;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCOMMander {

	private static final String[] commandList = new String[] { "ls", "get",
			"cat", "whoami", "whereis", "cp", "cd" ,"time"};
	

	protected static AliEnPrincipal user = AuthorizationFactory
			.getDefaultUser();

	protected static String site = ConfigUtils.getConfig()
			.gets("alice_close_site").trim();

	private static String myHome = UsersHelper.getHomeDir(user.getName());

	protected static LFN curDir = CatalogueApiUtils.getLFN(myHome);

	/**
	 * get list of commands
	 * 
	 * @return array of commands
	 */
	public static String[] getCommandList() {
		return commandList;
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
		return curDir;

	}
	/**
	* get the current directory, replace home with ~
	* 
	* @return name of the current directory, ~ places home 
	*/
	public static String getCurrentDirTilded() {
		return curDir.getCanonicalName().replace(myHome, "~");
	}
	
	/**
	* run a signature/line from the prompt
	* 
	* @param args first element is command, which needs to be in command list, following entries are arguments
	*/
	public static void run(String[] args) {
		ArrayList<String> argList = new ArrayList<String>(Arrays.asList(args));
		argList.remove(args[0]);
		execute(args[0], argList);
	}
	
	/**
	* execute a command line
	* 
	* @param the command, which needs to be in command list and will search for class as suffix, with JAliEnCommand<command> 
	* @param the list of arguments
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
			if (args.contains("-s"))
				jcommand.silent();
			if (!args.contains("--help") && !args.contains("-help")
					&& !args.contains("-h")
					&& (args.size() != 0 || jcommand.canRunWithoutArguments())) {
				jcommand.execute();
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error executing the command [" + comm + "].");
		}
	}

	/**
	* create and return a object of alien.shell.commands.JAliEnCommand.JAliEnCommand<classSuffix>
	* 
	* @param the name of the shell command, which will be taken as the suffix for the classname
	* @param array of argument objects, need to fit to the class
	* @return an instance of alien.shell.commands.JAliEnCommand.JAliEnCommand<classSuffix>
	*/
	protected static JAliEnBaseCommand getCommand(String classSuffix,
			Object[] objectParm) throws Exception {

		@SuppressWarnings("rawtypes") 
		Class cl = Class.forName("alien.shell.commands.JAliEnCommand"
//				Class cl = Class.forName(JAliEnCOMMander.class.getPackage().toString() + ".JAliEnCommand"
				+ classSuffix);
		@SuppressWarnings({ "rawtypes", "unchecked" })
		java.lang.reflect.Constructor co = cl
				.getConstructor((new Class[] { ArrayList.class }));
		return (JAliEnBaseCommand) co.newInstance(objectParm);
	}
}
