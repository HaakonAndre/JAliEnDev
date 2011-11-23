package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

import alien.catalogue.Package;
import alien.user.UsersHelper;

/**
 * @author ron
 * @since Nov 23, 2011
 */
public class JAliEnCommandpackages extends JAliEnBaseCommand {
	
	
	private List<Package> packs = null;
	
	@Override
	public void run() {
		
		packs = commander.c_api.getPackages(getPackagePlatformName());
				
		
		if (packs != null){
			for (Package p: packs){
				
				if(out.isRootPrinter())
					out.setReturnArgs(deserializeForRoot());
				else
					if(!isSilent())
						out.printOutln("	" + p.getFullName());
			}
		}
		else{
			out.printErrln("Couldn't find any packages.");
			out.setReturnArgs(deserializeForRoot(0));
		}

	}

	private String getPackagePlatformName(){

		String ret =  System.getProperty("os.name");
		
		if(System.getProperty("os.arch").contains("amd64"))
			ret += "-x86_64";
		else if(ret.toLowerCase().contains("mac") && System.getProperty("os.arch").contains("ppc"))
			ret  = "Darwin-PowerMacintosh";
	
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
			
			String ret =  RootPrintWriter.columnseparator + RootPrintWriter.fielddescriptor + "__result__"
					 + RootPrintWriter.fieldseparator + "1\n";
			for(Package p : packs)
				ret += RootPrintWriter.columnseparator + RootPrintWriter.fielddescriptor + "__result__"
					 + RootPrintWriter.fieldseparator + p.getFullName() + "\n";
					
			return ret;
		}
		return super.deserializeForRoot(0);
	}
	
	
	/**
	 * printout the help info, none for this command
	 */
	@Override
	public void printHelp() {
		
		out.printOutln();
		out.printOutln(helpUsage("packages","  list available packages"));
		out.printOutln();
	}

	/**
	 * cd can run without arguments 
	 * @return <code>true</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

	
	/**
	 * Constructor needed for the command factory in commander
	 * @param commander 
	 * @param out 
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandpackages(JAliEnCOMMander commander, UIPrintWriter out, final ArrayList<String> alArguments){
		super(commander, out,alArguments);
	}
}
