package alien.shell.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import alien.catalogue.FileSystemUtils;
import alien.se.SE;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class JAliEnCommandlistSEDistance extends JAliEnBaseCommand {
	private boolean useWriteMetrics;
	private String site;
	private String lfn_name;
	
	@Override
	public void run() {
		System.out.println( "Site: " + this.site + 
							"\nwrite: " + this.useWriteMetrics + "\nlfn: " + this.lfn_name );
		
		if( !this.useWriteMetrics && 
				(this.lfn_name==null || this.lfn_name.length()==0) ){
			out.printErrln("No LFN specified for read metrics");
			return;
		}
		
		if( lfn_name!=null && lfn_name.length()!=0 ){
			this.lfn_name = FileSystemUtils.getAbsolutePath(
					commander.user.getName(),
					commander.getCurrentDir().getCanonicalName(),
					this.lfn_name );
		}
		List<HashMap<SE,Double>> results = commander.c_api.listSEDistance(site, 
											this.useWriteMetrics, 
											this.lfn_name);
		for( HashMap<SE,Double> smap: results ){
				Set<SE> selist = smap.keySet();
				for( SE s: selist )
					out.printOutln( String.format("%1$"+ 40 + "s", s.seName)
							+ "\t(read: " + 
							String.format( "%.9f", s.demoteRead ) + 
							",  write: " + 
							String.format( "%.9f", s.demoteWrite ) +
							",  distance: " + String.format( "%.9f", smap.get(s) ) +  
							")");
		}
		out.printOutln();
	}

	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln("listSEDistance: Returns the closest working SE for a particular site. Usage");
		out.printOutln();
		out.printOutln(" listSEDistance [<site>] [read [<lfn>]|write]");
		out.printOutln();
		out.printOutln();
		out.printOutln(" Options:");
		out.printOutln("   <site>: site name. Default: current site");
		out.printOutln("   [read|write]: action. Default write. In the case of read, if an lfn is specified, use only SE that contain that file");
		out.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {		
		return true;
	}

	public JAliEnCommandlistSEDistance(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
		
		this.useWriteMetrics = true;
		try{
			int argLen = alArguments.size();
			if( argLen==0 )
				return;
			String arg = alArguments.get(0);
			if( !arg.equals("read") && !arg.equals("write") )
				this.site = arg;
			else
				this.useWriteMetrics = (arg.equals("write"));
			arg = alArguments.get(1);
			if( !arg.equals("read") && !arg.equals("write") )
				this.lfn_name = arg;
			else
				this.useWriteMetrics = (arg.equals("write"));
			arg = alArguments.get(2);
			if( !this.useWriteMetrics && this.lfn_name==null )
				this.lfn_name = arg;
		}catch( IndexOutOfBoundsException e ){ ; }
		
		System.out.println( this.site + " " + this.useWriteMetrics + " " + this.lfn_name );
	}
}
