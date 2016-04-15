package alien.shell.commands;

import java.util.ArrayList;

/**
 * @author ron
 * @since August 26, 2011
 */
public class JAliEnCommandpwd extends JAliEnBaseCommand {

	/**
	 * execute the pwd
	 */
	@Override
	public void run() {

		if (out.isRootPrinter()) {
			out.nextResult();
			out.setField("pwd", commander.curDir.getCanonicalName());
		} else {
			String ret = "";
			ret += commander.curDir.getCanonicalName();
			logger.info("PWD line : " + ret);
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
	 * get cannot run without arguments
	 *
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

	/**
	 * serialize return values for gapi/root
	 *
	 * @return serialized return
	 */
	@Override
	public String deserializeForRoot() {
		return RootPrintWriter.columnseparator + RootPrintWriter.fielddescriptor + "__result__" + RootPrintWriter.fieldseparator + "1";
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
	public JAliEnCommandpwd(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) {
		super(commander, out, alArguments);

	}
}
