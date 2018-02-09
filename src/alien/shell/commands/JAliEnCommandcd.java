package alien.shell.commands;

import java.util.ArrayList;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.user.UsersHelper;

/**
 * @author ron
 * @since June 4, 2011
 * @author sraje (Shikhar Raje, IIIT Hyderabad)
 * @since Modified 27th July, 2012
 */
public class JAliEnCommandcd extends JAliEnBaseCommand {

	@Override
	public void run() {

		LFN newDir = null;

		if (alArguments != null && alArguments.size() > 0)
			newDir = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), alArguments.get(0)));
		else
			newDir = commander.c_api.getLFN(UsersHelper.getHomeDir(commander.user.getName()));

		if (newDir != null) {
			if (newDir.isDirectory()) {
				commander.curDir = newDir;
				JAliEnCOMMander.setReturnArgs(deserializeForRoot(1));
			}
			else
				JAliEnCOMMander.setReturnCode(1, "Cannot open: " + alArguments.get(0) + " is file, not a directory");
		}
		else
			JAliEnCOMMander.setReturnCode(1, "No such file or directory");
	}

	/**
	 * printout the help info, none for this command
	 */
	@Override
	public void printHelp() {
		// ignore
	}

	/**
	 * cd can run without arguments
	 *
	 * @return <code>true</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

	/**
	 * Constructor needed for the command factory in commander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandcd(final JAliEnCOMMander commander, final ArrayList<String> alArguments) {
		super(commander, alArguments);
	}
}
