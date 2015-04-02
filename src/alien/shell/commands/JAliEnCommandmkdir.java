package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import alien.catalogue.FileSystemUtils;

/**
 * @author ron
 * @since June 6, 2011 <br>
 * usage: mkdir   [-options] directory [directory[,directory]] <br>
 * options: <br>
 *                   -p                     :  create parents as needed <br>
 *                   -silent                :  execute command silently <br>
 */
public class JAliEnCommandmkdir extends JAliEnBaseCommand {

	/**
	 * marker for -p argument : create parents as needed
	 */
	private boolean bP = false;
	
	/**
	 * the list of directories that will be created
	 * */
	private List<String> alPaths = null;
	
	private boolean success = true;
	
	@Override
	public void run() 
	{
		
		for (String path: alPaths)
		{

			if(out.isRootPrinter())
			{
				out.nextResult();
				if(bP)
				{
					if(commander.c_api.createCatalogueDirectory(FileSystemUtils.getAbsolutePath(
							commander.user.getName(),
							commander.getCurrentDir().getCanonicalName(),path),true)==null)
					{
						if(!isSilent())
						out.setReturnCode(1,"Could not create directory (or non-existing parents): " + path);
					}
				}
				else 
				{
					if(commander.c_api.createCatalogueDirectory(FileSystemUtils.getAbsolutePath(
							commander.user.getName(),
							commander.getCurrentDir().getCanonicalName(),path))==null)
					{
						if(!isSilent())
						out.setReturnCode(2,"Could not create directory: " + path);
					}
				}
			}
			else
			{
			if(bP)
			{
					if(commander.c_api.createCatalogueDirectory(FileSystemUtils.getAbsolutePath(
						commander.user.getName(),
						commander.getCurrentDir().getCanonicalName(),path),true)==null)
				{
					if(!isSilent())
						out.printErrln("Could not create directory (or non-existing parents): " + path);
					logger.log(Level.WARNING,"Could not create directory (or non-existing parents): " + path);
					success = false;
				}
			}
			else 
			{
					if(commander.c_api.createCatalogueDirectory(FileSystemUtils.getAbsolutePath(
						commander.user.getName(),
						commander.getCurrentDir().getCanonicalName(),path))==null)
				{
					if(!isSilent())
						out.printErrln("Could not create directory: " + path);
					logger.log(Level.WARNING,"Could not create directory: " + path);
					success = false;
				}
			}
			}
		}		
	}
	

	/**
	 * serialize return values for gapi/root
	 * 
	 * @return serialized return
	 */
	@Override
	public String deserializeForRoot() {

		String ret = RootPrintWriter.columnseparator + RootPrintWriter.fielddescriptor + "__result__"
				 + RootPrintWriter.fieldseparator;
		if(success)
			ret += "1";
		else
			ret += "0";
		
		return ret;
	}
	
	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {

		out.printOutln();
		out.printOutln(helpUsage("mkdir","[-options] <directory> [<directory>[,<directory>]]"));
		out.printOutln(helpStartOptions());
		out.printOutln(helpOption("-p","create parents as needed"));
		out.printOutln(helpOption("-silent","execute command silently"));
		out.printOutln();
	}

	/**
	 * mkdir cannot run without arguments
	 * 
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * Constructor needed for the command factory in commander
	 * @param commander 
	 * @param out 
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException 
	 */
	public JAliEnCommandmkdir(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);

		try {
			final OptionParser parser = new OptionParser();

			parser.accepts("p");
			parser.accepts("s");

			final OptionSet options = parser.parse(alArguments
					.toArray(new String[] {}));

			alPaths = optionToString(options.nonOptionArguments());

			if(options.has("s"))
				silent();
			bP = options.has("p");
		} catch (OptionException e) {
			printHelp();
			throw e;
		}
	}
}
