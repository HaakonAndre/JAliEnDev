package alien.shell.commands;

import java.util.ArrayList;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import joptsimple.OptionException;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandlfn2guid extends JAliEnBaseCommand {

	/**
	 * entry the call is executed on, either representing a LFN
	 */
	private String lfnName = null;

	/**
	 * execute the lfn2guid
	 */
	@Override
	public void run() {

		final LFN lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), lfnName));

		if (lfn == null)
			commander.printErrln("Could not get the LFN [" + lfnName + "].");
		else
			if (lfn.isDirectory())
				commander.printErrln("The LFN is a directory [" + lfn.getCanonicalName() + "].");
			else
				if (lfn.guid != null)
					commander.printOutln(padRight(lfn.getCanonicalName(), 80) + lfn.guid);
				else
					commander.printErrln("Could not get the GUID for [" + lfn.getCanonicalName() + "].");

		commander.outNextResult();
		if (lfn == null)
			commander.printOutln("Could not get the LFN [" + lfnName + "].");
		else
			if (lfn.isDirectory())
				commander.printOutln("The LFN is a directory [" + lfn.getCanonicalName() + "].");
			else
				if (lfn.guid != null)
					commander.printOutln(padRight(lfn.getCanonicalName(), 80) + lfn.guid);
				else
					commander.printOutln("Could not get the GUID for [" + lfn.getCanonicalName() + "].");
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("lfn2guid", "<filename>"));
		commander.printOutln();
	}

	/**
	 * lfn2guid cannot run without arguments
	 *
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * Constructor needed for the command factory in JAliEnCOMMander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandlfn2guid(final JAliEnCOMMander commander, final ArrayList<String> alArguments) throws OptionException {
		super(commander, alArguments);

		if (alArguments.size() != 1)
			throw new JAliEnCommandException();

		lfnName = alArguments.get(0);

	}

}
