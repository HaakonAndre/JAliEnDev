package alien.shell.commands;

import java.util.ArrayList;
import java.util.Set;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.LFNListCollectionFromString;
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
	private Set<LFN> lfns = null;

	private String errorMessage = null;
	
	/*
	 * The command received -v verbose argument
	 * */
	private boolean bZ = false;
	
	/**
	 * execute the type
	 */
	@Override
	public void run() 
	{
		
		//A9D461B2-1386-11E1-9717-7623A10ABEEF  (from the file /alice/data/2011/LHC11h/000168512/raw/11000168512082.99.root)
		// -v  A9D461B2-1386-11E1-9717-7623A10ABEEF  (from the file /alice/data/2011/LHC11h/000168512/raw/11000168512082.99.root)( size = 1868499542)( md5 = d1f1157f09b76ed5a1cd095b009d9348)
		
		String collectionPath;
		
		if(sPath.startsWith("/"))
			collectionPath = sPath;
		else
		{
			collectionPath = commander.getCurrentDir().getCanonicalName()+sPath;	
		}
		
		try
		{
			final LFNListCollectionFromString ret = Dispatcher.execute(new LFNListCollectionFromString(commander.getUser(), commander.getRole(), collectionPath));
			
			lfns = ret.getLFNs();
		}
		catch (ServerException e)
		{
			Throwable cause = e.getCause();
			
			errorMessage = cause.getMessage();
		}
		
		if (errorMessage != null){
			if(!isSilent())
				out.printErrln(errorMessage);
			
			return;
		}
		
		if (out.isRootPrinter())
		{
			out.nextResult();
			for(final LFN c: lfns)	
			{
				out.setField("guid",c.guid.toString());
				out.setField("lfn", c.getCanonicalName());
				if(bZ)
				{
					out.setField("size"," "+c.size);
					out.setField("md5"," "+c.md5);
				}
			}
			if(!isSilent())
				out.setReturnCode(1,"Not a collection");
		}
		else
		{
			StringBuilder sb = new StringBuilder();
		
			for(LFN lfn: lfns){
			sb.append(lfn.guid);
			sb.append(" (from the file ");
			sb.append(lfn.getCanonicalName());
			sb.append(")");
		
			//( size = 1868499542)( md5 = d1f1157f09b76ed5a1cd095b009d9348)
			if(bZ){
				sb.append(" (size = ");
				sb.append(lfn.size);
				sb.append(") ");
				sb.append("(md5 = ");
				sb.append(lfn.md5);
				sb.append(")");
			}
			
			sb.append("\n");
		}
		
		out.printOutln(sb.toString());
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
	 * from root it is allways called with -v -z
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

		bZ = options.has("z");
		
		if (options.nonOptionArguments().size() != 1)
			throw new JAliEnCommandException();

		sPath = options.nonOptionArguments().get(0);

	}

}
