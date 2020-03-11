package alien.shell.commands;

import java.util.Iterator;
import java.util.List;

import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.shell.ErrNo;
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

		final Iterator<LFN> it;

		if (guid == null)
			commander.setReturnCode(ErrNo.ENXIO, "Could not get the GUID [" + guidName + "].");
		else if (guid.getLFNs() != null && (it = guid.getLFNs().iterator()).hasNext()) {
			final LFN lfn = it.next();

			commander.printOutln(padRight(guid.guid + "", 40) + lfn.getCanonicalName());

			commander.printOut("guid", String.valueOf(guid.guid));
			commander.printOut("lfn", String.valueOf(lfn.getCanonicalName()));
		}
		else
			commander.setReturnCode(ErrNo.ENOENT, "No LFNs are associated to this GUID [" + guid.guid + "].");
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("guid2lfn", "<GUID>"));
		commander.printOutln();
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
	 *
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandguid2lfn(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		if (alArguments.size() != 1) {
			// help will be printed by the commander anyway since canRunWithoutArguments=false
			return;
		}

		guidName = alArguments.get(0);
	}

}
