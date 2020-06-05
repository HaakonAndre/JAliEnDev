package alien.shell.commands;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.StringTokenizer;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
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
	private Collection<String> guidNames = new LinkedHashSet<>();

	/**
	 * execute the lfn2guid
	 */
	@Override
	public void run() {
		for (final String guidName : guidNames) {
			final GUID guid = commander.c_api.getGUID(guidName, false, true);

			final Iterator<LFN> it;

			if (guid == null)
				commander.setReturnCode(ErrNo.ENXIO, "Could not get the GUID [" + guidName + "].");
			else if (guid.getLFNs() != null && (it = guid.getLFNs().iterator()).hasNext()) {
				final LFN lfn = it.next();

				commander.printOutln(padRight(guid.guid + "", 40) + lfn.getCanonicalName());

				commander.printOut("guid", String.valueOf(guid.guid));
				commander.printOut("lfn", String.valueOf(lfn.getCanonicalName()));
				commander.outNextResult();
			}
			else
				commander.setReturnCode(ErrNo.ENOENT, "No LFNs are associated to this GUID [" + guid.guid + "].");
		}
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

		if (alArguments.size() == 0) {
			// help will be printed by the commander anyway since canRunWithoutArguments=false
			return;
		}

		for (final String s : alArguments) {
			boolean ok = false;

			final StringTokenizer st = new StringTokenizer(s, " \r\n\t;#/\\?");

			while (st.hasMoreTokens()) {
				final String tok = st.nextToken();

				if (GUIDUtils.isValidGUID(tok)) {
					guidNames.add(tok);
					ok = true;
					continue;
				}
			}

			if (!ok) {
				commander.setReturnCode(ErrNo.EINVAL, "No GUID in this string: " + s);
				setArgumentsOk(false);
				return;
			}
		}
	}
}
