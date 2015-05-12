package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class JAliEnCommanddeleteMirror extends JAliEnBaseCommand {
	private boolean useLFNasGuid;
	private String lfn;
	private String se;
	
	@Override
	public void run() {
		if(this.lfn==null || this.lfn.length()==0 ||
				this.se==null || this.se.length()==0){
			this.printHelp();
			return;
		}
		if( useLFNasGuid ){
			if( !GUIDUtils.isValidGUID( this.lfn ) ){
				out.printErrln("This is not a valid GUID");
				return;
			}
			GUID guid = commander.c_api.getGUID(this.lfn);
			if( guid==null ){
				out.printErrln("No such GUID");
				return;
			}
		}
		else
			lfn = FileSystemUtils.getAbsolutePath(
					commander.user.getName(),
					commander.getCurrentDir().getCanonicalName(), lfn);

			int result = commander.c_api.deleteMirror( 
							lfn, 
							this.useLFNasGuid, se );
			if( result == 0)
				out.printOutln("Mirror scheduled to be deleted from " + this.se);
			else{
				String errline=null;
				switch( result ){
					case -1: errline="invalid GUID"; break;
					case -2: errline="failed to get SE"; break;
					case -3: errline="user not authorized"; break;
					case -4: errline="unknown error"; break;
				}
				out.printErrln("Error deleting mirror: " + errline);
			}		
		// check is PFN		
	}

	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln("Removes a replica of a file from the catalogue");
		out.printOutln("Usage:");
		out.printOutln("        deleteMirror [-g] <lfn> <se> [<pfn>]");
		out.printOutln();
		out.printOutln("Options:");
		out.printOutln("   -g: the lfn is a guid");
		out.printOutln();		
	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	public JAliEnCommanddeleteMirror(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
		try{
			final OptionParser parser = new OptionParser();		
			parser.accepts("g");		
	
			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));
	
			List<String> lfns = optionToString(options.nonOptionArguments());
			if( lfns==null ){
				System.out.println( lfns );
				return;
			}
			int argLen = lfns.size();
			if(argLen!=2){
				this.printHelp();
				return;
			}
					
			this.lfn = lfns.get(0);
			this.se = lfns.get(1);
			
			useLFNasGuid = options.has("g");						
		}catch(OptionException e) {
			printHelp();
			throw e;
		}

	}
}
