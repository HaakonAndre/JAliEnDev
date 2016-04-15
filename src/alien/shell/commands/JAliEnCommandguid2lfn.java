package alien.shell.commands;

import java.util.ArrayList;

import alien.catalogue.GUID;
import joptsimple.OptionException;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandguid2lfn extends JAliEnBaseCommand {

	/**
	 * entry the call is executed on, either representing a LFN
	 */
	private String guidName = null;

	/**
	 * execute the lfn2guid
	 */
	@Override
	public void run() {
		final GUID guid = commander.c_api.getGUID(guidName, false, true);

		if (out.isRootPrinter()) {
			out.nextResult();
			if (guid == null)
				out.setField("message", "Could not get the GUID [" + guidName + "].");
			else if (guid.getLFNs() != null && guid.getLFNs().iterator().hasNext())
				out.setField("message", padRight(guid.guid + "", 40) + guid.getLFNs().iterator().next().getCanonicalName());
			else
				out.setField("message", "No LFNs are associated to this GUID [" + guid.guid + "].");
		} else if (guid == null)
			out.printErrln("Could not get the GUID [" + guidName + "].");
		else if (guid.getLFNs() != null && guid.getLFNs().iterator().hasNext())
			out.printOutln(padRight(guid.guid + "", 40) + guid.getLFNs().iterator().next().getCanonicalName());
		else
			out.printErrln("No LFNs are associated to this GUID [" + guid.guid + "].");
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {

		out.printOutln();
		out.printOutln(helpUsage("guid2lfn", "<GUID>"));
		out.printOutln();
	}

	/**
	 * guid2lfn cannot run without arguments
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
	public JAliEnCommandguid2lfn(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);

		if (alArguments.size() != 1)
			throw new JAliEnCommandException();

		guidName = alArguments.get(0);

	}

}
