package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class JAliEnCommandlistSEDistance extends JAliEnBaseCommand {
	private boolean useWriteMetrics;
	private String site;
	private String lfn_name;
	
	@Override
	public void run() {
		// TODO Auto-generated method stub

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
		int argLen = alArguments.size();
		if( argLen==0 )
			return;
		String arg = alArguments.get(0);
		if( !arg.equals("read") && !arg.equals("write") )
			this.site = arg;
		else
			this.useWriteMetrics = (arg.equals("write"));
		if( argLen==1 )
			return;
		arg = alArguments.get(1);
		if( !arg.equals("read") && !arg.equals("write") )
			this.site = this.lfn_name;
		else
			this.useWriteMetrics = (arg.equals("write"));
		if( argLen==2 )
			return;
		if( !this.useWriteMetrics && this.lfn_name==null && argLen==3 )
			this.lfn_name = arg;
	}
}
