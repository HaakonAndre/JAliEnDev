package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

import alien.catalogue.Package;

/**
 * @author ron
 * @since Nov 23, 2011
 */
public class JAliEnCommandpackages extends JAliEnBaseCommand {

	private List<Package> packs = null;

	@Override
	public void run() {

		packs = commander.c_api.getPackages(getPackagePlatformName());

		if (packs != null) {
			for (final Package p : packs)
				if (out.isRootPrinter()) {
					out.nextResult();
					out.setField("packages", p.getFullName());

				}

				else {
					String ret = "";

					ret += "result" + padSpace(1) + p.getFullName();

					if (!isSilent())
						out.printOutln(ret);
				}
		}
		else {
			out.printErrln("Couldn't find any packages.");
			out.setReturnCode(1, "Couldn't find any packages.");
			out.setReturnArgs(deserializeForRoot(0));
		}

	}

	private static String getPackagePlatformName() {

		String ret = System.getProperty("os.name");

		if (System.getProperty("os.arch").contains("amd64"))
			ret += "-x86_64";

		else
			if (ret.toLowerCase().contains("mac") && System.getProperty("os.arch").contains("ppc"))
				ret = "Darwin-PowerMacintosh";

		return ret;
	}

	/**
	 * serialize return values for gapi/root
	 *
	 * @return serialized return
	 */
	@Override
	public String deserializeForRoot() {
		if (packs != null) {

			final StringBuilder sb = new StringBuilder();

			sb.append(RootPrintWriter.columnseparator).append(RootPrintWriter.fielddescriptor).append("__result__").append(RootPrintWriter.fieldseparator).append("1\n");

			for (final Package p : packs)
				sb.append(RootPrintWriter.columnseparator).append(RootPrintWriter.fielddescriptor).append("__result__").append(RootPrintWriter.fieldseparator).append(p.getFullName()).append('\n');

			return sb.toString();
		}

		return super.deserializeForRoot(0);
	}

	/**
	 * printout the help info, none for this command
	 */
	@Override
	public void printHelp() {

		out.printOutln();
		out.printOutln(helpUsage("packages", "  list available packages"));
		out.printOutln();
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
	 * @param out
	 *
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandpackages(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) {
		super(commander, out, alArguments);
	}
}
