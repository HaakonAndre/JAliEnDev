package alien.shell.commands;

import java.util.ArrayList;
import java.util.Set;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.LFNListCollectionFromString;
import alien.catalogue.LFN;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author ron
 * @since Nov 24, 2011
 */
public class JAliEnCommandlistFilesFromCollection extends JAliEnBaseCommand {

	private String sPath = null;

	/**
	 * the LFN for path
	 */
	private Set<LFN> lfns = null;

	private String errorMessage = null;

	/*
	 * The command received -z argument
	 */
	private boolean bZ = false;

	/*
	 * The command received -x argument
	 */
	private boolean bX = false;

	/**
	 * execute the type
	 */
	@Override
	public void run() {

		// A9D461B2-1386-11E1-9717-7623A10ABEEF (from the file /alice/data/2011/LHC11h/000168512/raw/11000168512082.99.root)
		// -v A9D461B2-1386-11E1-9717-7623A10ABEEF (from the file /alice/data/2011/LHC11h/000168512/raw/11000168512082.99.root)( size = 1868499542)( md5 = d1f1157f09b76ed5a1cd095b009d9348)

		String collectionPath;

		if (sPath.startsWith("/"))
			collectionPath = sPath;
		else {
			collectionPath = commander.getCurrentDirName() + sPath;
		}

		try {
			final LFNListCollectionFromString ret = Dispatcher.execute(new LFNListCollectionFromString(commander.getUser(), collectionPath, bX));

			lfns = ret.getLFNs();
		} catch (final ServerException e) {
			final Throwable cause = e.getCause();

			errorMessage = cause.getMessage();
		}

		if (errorMessage != null) {
			commander.printErrln(errorMessage);

			return;
		}


		final StringBuilder sb = new StringBuilder();
		for (final LFN lfn : lfns) {
			commander.outNextResult();
			commander.printOut("guid", lfn.guid.toString());
			commander.printOut("lfn", lfn.getCanonicalName());
			sb.append(lfn.guid);
			sb.append(" (from the file ");
			sb.append(lfn.getCanonicalName());
			sb.append(")");
			if (bZ) {
				commander.printOut("size", " " + lfn.size);
				commander.printOut("md5", " " + lfn.md5);
				sb.append(" (size = ");
				sb.append(lfn.size);
				sb.append(") ");
				sb.append("(md5 = ");
				sb.append(lfn.md5);
				sb.append(")");
			}
			sb.append("\n");
		}

		commander.printOutln(sb.toString());
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		// ignore
	}

	/**
	 * ls can run without arguments
	 *
	 * @return <code>true</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * serialize return values for gapi/root from root it is allways called with -v -z
	 *
	 * @return serialized return
	 */

	@Override
	public String deserializeForRoot() {
		if (lfns == null || lfns.size() == 0)
			return "";

		final StringBuilder ret = new StringBuilder();

		for (final LFN c : lfns) {
			ret.append(RootPrintWriter.columnseparator).append(RootPrintWriter.fielddescriptor).append("origLFN").append(RootPrintWriter.fieldseparator);

			ret.append(c.lfn);

			ret.append(RootPrintWriter.columnseparator).append(RootPrintWriter.fielddescriptor).append("localName").append(RootPrintWriter.fieldseparator);
			// skipped

			ret.append(RootPrintWriter.columnseparator).append(RootPrintWriter.fielddescriptor).append("data").append(RootPrintWriter.fieldseparator);
			// skipped

			ret.append(RootPrintWriter.columnseparator).append(RootPrintWriter.fielddescriptor).append("guid").append(RootPrintWriter.fieldseparator);
			ret.append(c.guid);

		}

		return ret.toString();
	}

	/**
	 * Constructor needed for the command factory in commander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandlistFilesFromCollection(final JAliEnCOMMander commander, final ArrayList<String> alArguments) throws OptionException {
		super(commander, alArguments);

		final OptionParser parser = new OptionParser();

		parser.accepts("z");
		parser.accepts("s");
		parser.accepts("x");

		final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

		if (options.has("s"))
			silent();

		bZ = options.has("z");
		bX = options.has("x");

		if (options.nonOptionArguments().size() != 1)
			throw new JAliEnCommandException();

		sPath = options.nonOptionArguments().get(0).toString();
	}
}
