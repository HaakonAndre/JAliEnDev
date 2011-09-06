package alien.shell.commands;

import java.util.ArrayList;
import java.util.Arrays;

import alien.api.catalogue.CatalogueApiUtils;

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
	public void execute() {

		if(rawenvelope.contains("signature="))
			CatalogueApiUtils.registerEnvelopes(commander.user,new ArrayList<String>(Arrays.asList(rawenvelope)));
		else
			CatalogueApiUtils.registerEncryptedEnvelope(commander.user,rawenvelope,size,lfn,perm,expire,pfn,se,guid,md5);
			

		if (out.isRootPrinter())
			out.setReturnArgs(deserializeForRoot());
	}

	/**
	 * printout the help info
	 */
	public void printHelp() {
		// ignore
	}

	/**
	 * get cannot run without arguments
	 * 
	 * @return <code>false</code>
	 */
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * set command's silence trigger
	 */
	public void silent() {
		// ignore
	}

	/**
	 * serialize return values for gapi/root
	 * 
	 * @return serialized return
	 */
	public String deserializeForRoot() {

		return super.deserializeForRoot();
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
