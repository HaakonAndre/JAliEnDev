package alien.shell.commands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import alien.catalogue.PFN;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandcommit extends JAliEnBaseCommand {

	/**
	 * commit request raw envelope
	 */
	private String rawenvelope = "";

	/**
	 * commit request lfn
	 */
	private String lfn = "";
	
	/**
	 * commit request size
	 */
	private int size = 0;

		/**
		 * commit request permissions
		 */
		private String perm = "";

	/**
	 * commit request expiration
	 */
	private String expire = "";

	/**
	 * commit request PFN
	 */
	private String pfn = "";
	
	/**
	 * commit request SE
	 */
	private String se = "";
	
	/**
	 * commit request GUID
	 */
	private String guid = "";

	
	/**
	 * commit request MD5
	 */
	private String md5 = "";

	
	/**
	 * execute the commit
	 */
	@Override
	public void run() {
		
		List<PFN> pfns = null;
		if(rawenvelope.contains("signature=")){
			pfns = commander.c_api.registerEnvelopes(new ArrayList<String>(Arrays.asList(rawenvelope)));
			
		}
		else{
			pfns = commander.c_api.registerEncryptedEnvelope(rawenvelope,size,md5,lfn,perm,expire,pfn,se,guid);
		}
			

		if (out.isRootPrinter())
				out.setReturnArgs(deserializeForRoot((pfns!=null && pfns.size()>0)));
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
		return false;
	}

	/**
	 * serialize return values for gapi/root
	 * @param status 
	 * 
	 * @return serialized return
	 */
	public String deserializeForRoot(final boolean status) {
		
		if(status)
			return RootPrintWriter.columnseparator 
				+ RootPrintWriter.fielddescriptor + lfn + RootPrintWriter.fieldseparator + "0";
		
		return RootPrintWriter.columnseparator 
				+ RootPrintWriter.fielddescriptor + "lfn" + RootPrintWriter.fieldseparator + "1";
		
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
	public JAliEnCommandcommit(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) {
		super(commander, out, alArguments);
		  
		java.util.ListIterator<String> arg = alArguments.listIterator();

		if (arg.hasNext()) {
			rawenvelope = arg.next();
			if (arg.hasNext()){
				try {
					size = Integer.parseInt(arg.next());
				} catch (NumberFormatException e) {
					// ignore
				}
			}
			if (arg.hasNext())
				lfn = arg.next();
			if (arg.hasNext())
				perm = arg.next();
			if (arg.hasNext())
				expire = arg.next();
			if (arg.hasNext())
				pfn = arg.next();
			if (arg.hasNext())
				se = arg.next();
			if (arg.hasNext())
				guid = arg.next();
			if (arg.hasNext())
				md5 = arg.next();


		} else
			out.printErrln("No envelope to register passed.");

	}
}
