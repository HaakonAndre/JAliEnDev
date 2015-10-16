package alien.shell.commands;

import java.util.ArrayList;

import joptsimple.OptionException;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;

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

		final LFN lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDir().getCanonicalName(), lfnName));

		if (lfn == null)
			out.printErrln("Could not get the LFN [" + lfnName + "].");
		else if (lfn.isDirectory())
			out.printErrln("The LFN is a directory [" + lfn.getCanonicalName() + "].");
		else if (lfn.guid != null)
			out.printOutln(padRight(lfn.getCanonicalName(), 80) + lfn.guid);
		else
			out.printErrln("Could not get the GUID for [" + lfn.getCanonicalName() + "].");

		if (out.isRootPrinter()) {
			out.nextResult();
			if (lfn == null)

				out.setField("message", "Could not get the LFN [" + lfnName + "].");

			else if (lfn.isDirectory())
				out.setField("message", "The LFN is a directory [" + lfn.getCanonicalName() + "].");
			else if (lfn.guid != null)
				out.setField("message", padRight(lfn.getCanonicalName(), 80) + lfn.guid);
			else
				out.setField("message", "Could not get the GUID for [" + lfn.getCanonicalName() + "].");
		}

	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {

		out.printOutln();
		out.printOutln(helpUsage("lfn2guid", "<filename>"));
		out.printOutln();
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
	 * @param out
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandlfn2guid(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);

		if (alArguments.size() != 1)
			throw new JAliEnCommandException();

		lfnName = alArguments.get(0);

	}

}
