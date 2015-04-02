package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

public class JAliEnCommandtouch extends JAliEnBaseCommand {
	private final List<String> filelist;
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void printHelp() {
		// TODO Auto-generated method stub
		out.printOutln();
		out.printOutln(helpUsage("touch",
				" <LFN> [<LFN>[,<LFN>]]"));		
		out.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		// TODO Auto-generated method stub
		return false;
	}
	
	/**
	 * Constructor needed for the command factory in commander
	 * @param commander 
	 * @param out 
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandtouch(JAliEnCOMMander commander, UIPrintWriter out,
								final ArrayList<String> alArguments){
		super(commander, out,alArguments);
		
		filelist = new ArrayList<>(alArguments.size());
		
		for(String file: alArguments){
			try{
				filelist.add(file);
			}
			catch(NumberFormatException e ){
				throw new JAliEnCommandException();
			}
		}
	}
}
