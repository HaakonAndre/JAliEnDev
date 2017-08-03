package alien.shell.commands;

import java.util.ArrayList;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author ron
 * @since Nov 24, 2011
 */
public class JAliEnCommandtype extends JAliEnBaseCommand {

	private String sPath = null;

	/**
	 * the LFN for path
	 */
	private LFN lfn = null;

	/**
	 * execute the type
	 */
	@Override
	public void run() {

		if (sPath != null)

			lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDir().getCanonicalName(), sPath));

		if (lfn == null) {
			out.printOutln("No such file or directory: [" + sPath + "]");
			return;
		}

		if (out.isRootPrinter()) {
			out.nextResult();
			if (lfn.isFile())
				out.setField("type", "file");
			else
				if (lfn.isDirectory())
					out.setField("type", "directory");
				else
					if (lfn.isCollection())
						out.setField("type", "collection");
		}
		else {
			String ret = "";
			if (lfn.isFile())
				ret += "file";
			else
				if (lfn.isDirectory())
					ret += "directory";
				else
					if (lfn.isCollection())
						ret += "collection";
			logger.info("Type line : " + ret);
			if (!isSilent())
				out.printOutln(ret);
		}
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
	 * serialize return values for gapi/root
	 *
	 * @return serialized return
	 */
	@Override
	public String deserializeForRoot() {

		if (lfn == null)
			return super.deserializeForRoot(0);

		String ret = RootPrintWriter.columnseparator + RootPrintWriter.fielddescriptor + "type" + RootPrintWriter.fieldseparator;

		if (lfn.isFile())
			ret += "file";
		else
			if (lfn.isDirectory())
				ret += "directory";
			else
				if (lfn.isCollection())
					ret += "collection";
				else
					return super.deserializeForRoot(0);

		return ret;

	}

	/**
	 * Constructor needed for the command factory in commander
	 *
	 * @param commander
	 * @param out
	 *
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandtype(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);

		final OptionParser parser = new OptionParser();

		parser.accepts("z");
		parser.accepts("s");

		final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

		if (options.has("s"))
			silent();

		if (options.nonOptionArguments().size() != 1)
			throw new JAliEnCommandException();

		sPath = options.nonOptionArguments().get(0).toString();

	}

}
