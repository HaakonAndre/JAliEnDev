package alien.shell.commands;

import java.util.ArrayList;
import java.util.logging.Level;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.RemoveLFNfromString;

/**
 * @author ron
 * @since Oct 27, 2011
 * @author sraje (Shikhar Raje, IIIT Hyderabad)
 * @since June 25, 2012
 */
public class JAliEnCommandrm extends JAliEnBaseCommand
{
	/**
	 * Variable for -f "Force" flag and -i "Interactive" flag.
	 * These 2 flags contradict each other, and hence only 1 variable for them.
	 * (Source: GNU Man pages for rm).
	 * @val True if interactive; False if forced. 
	 */
	boolean bIF = false;
	
	/**
	 * Variable for -r "Recursive" flag
	 */
	boolean bR = false;
	
	/**
	 * Variable for -v "Verbose" flag
	 */
	boolean bV = false;
	
	@Override
	public void run()
	{
		for (String path : alArguments)
		{
			//My added code... From the Dispatcher, direct, instead of from the wrapper for in the COMMander class, like above...
			
			RemoveLFNfromString rlfn = new RemoveLFNfromString(commander.getUser(), commander.getRole(), path);
			if(out.isRootPrinter())
			{
				try
				{
					RemoveLFNfromString a =  Dispatcher.execute(rlfn);//Remember, all checking is being done server side now.
					
					if (!a.wasRemoved())
					{
						out.setReturnCode(1, "Failed to remove [" + path + "]" );
					}
				}
				catch (ServerException e)
				{
					e.getCause().printStackTrace();
					out.setReturnCode(1, "Failed to remove [" + path + "]" );
				}		
			}
			
			else
			{
			try
			{
				RemoveLFNfromString a =  Dispatcher.execute(rlfn);//Remember, all checking is being done server side now.
				
				if (!a.wasRemoved() && bV)
				{
					out.printErrln("Failed to remove [" + path + "]");
				}
			}
			catch (ServerException e)
			{
				logger.log(Level.WARNING,"Could not get LFN: " + path);
				e.getCause().printStackTrace();
				
				if(bV)
					out.printErrln("Error removing [" + path + "]");
			}		
			}
		}
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp()
	{
		out.printOutln();
		out.printOutln(helpUsage("rm",
				" <LFN> [<LFN>[,<LFN>]]"));
		out.printOutln(helpStartOptions());
		out.printOutln(helpOption("-silent","execute command silently"));
		out.printOutln();
	}

	/**
	 * rm cannot run without arguments
	 * 
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments()
	{
		return false;
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
	public JAliEnCommandrm(JAliEnCOMMander commander, UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException
	{
		super(commander, out, alArguments);
		try
		{
			final OptionParser parser = new OptionParser();
			parser.accepts("i");
			parser.accepts("f");
			parser.accepts("r");
			parser.accepts("v");
			
			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));
			
			bIF = options.has("i");
			bIF = !options.has("f");
			bR = options.has("r");
			bV = options.has("v");
		}
		catch (OptionException e)
		{
			printHelp();
			//throw e;
		}
	}
}
