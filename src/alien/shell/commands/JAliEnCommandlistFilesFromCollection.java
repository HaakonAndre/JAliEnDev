package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;

/**
 * @author ron
 * @since Nov 24, 2011
 */
public class JAliEnCommandlistFilesFromCollection extends JAliEnBaseCommand {

	private String sPath = null;

	/**
	 * the LFN for path
	 */
	private List<LFN> lfns = new ArrayList<LFN>();

	/**
	 * execute the type
	 */
	@Override
	public void run() {
		
		LFN coll = null;

		if (sPath != null)
			coll = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(
					commander.user.getName(), commander.getCurrentDir()
							.getCanonicalName(), sPath));


		if (coll == null){
			if(!isSilent())
				out.printErrln("No such file or directory: [" + sPath + "]");
			return;
		}
		
		if(!coll.isCollection()){
			if(!isSilent())
				out.printOutln("Not a collection: [" + sPath + "]");
			return;
		}
		

		for(String c: coll.listCollection())
			lfns.add(commander.c_api.getLFN(c));
				
		//if (out.isRootPrinter())
			out.setReturnArgs(deserializeForRoot());
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
		if (lfns == null || lfns.size()==0)
			return "";
		
		final StringBuilder ret = new StringBuilder();

		for(final LFN c: lfns){
			 ret.append(RootPrintWriter.columnseparator).append(RootPrintWriter.fielddescriptor).append("origLFN").append(RootPrintWriter.fieldseparator);
			 
			 ret.append(c.lfn);
			 
			 ret.append(RootPrintWriter.columnseparator).append(RootPrintWriter.fielddescriptor).append("localName").append(RootPrintWriter.fieldseparator);
			 //skipped
			 
			 ret.append(RootPrintWriter.columnseparator).append(RootPrintWriter.fielddescriptor).append("data").append(RootPrintWriter.fieldseparator);
			 //skipped
			 
			 ret.append(RootPrintWriter.columnseparator).append(RootPrintWriter.fielddescriptor).append("guid").append(RootPrintWriter.fieldseparator);
			 ret.append(c.guid);
				
		}

		return ret.toString();
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
	public JAliEnCommandlistFilesFromCollection(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);

		final OptionParser parser = new OptionParser();

		parser.accepts("z");
		parser.accepts("s");

		final OptionSet options = parser.parse(alArguments
				.toArray(new String[] {}));

		if (options.has("s"))
			silent();

		if (options.nonOptionArguments().size() != 1)
			throw new JAliEnCommandException();

		sPath = options.nonOptionArguments().get(0);

	}

}
