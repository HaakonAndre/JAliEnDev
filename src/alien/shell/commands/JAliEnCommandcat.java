package alien.shell.commands;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.logging.Level;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.Format;
import lazyj.Utils;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandcat extends JAliEnBaseCommand {

	private boolean bN = false;
	private boolean bG = false;
	private boolean bE = false;
	private boolean bB = false;
	private boolean bT = false;
	private boolean bO = false;
	private String outputFileName = null;
	private ArrayList<String> alPaths = null;
	
	@Override
	public void run() {
		for (final String eachFileName : alPaths) {
			File fout = catFile(eachFileName);
			int count = 0;
			if (fout != null && fout.exists() && fout.isFile()
					&& fout.canRead()) {
				final String content = Utils.readFile(fout.getAbsolutePath());
				if (content != null) {
					BufferedReader br = new BufferedReader(new StringReader(content));
					
					String line;
					
					try{
						while ( (line=br.readLine())!=null ){
							if(bO){
								
								
								FileWriter fstream = new FileWriter(eachFileName);
								  BufferedWriter o = new BufferedWriter(fstream);
								  o.write(content);
								  fstream.close();
								  o.close();
							
							
							}
							if (bN){
								out.printOut(++count+"  ");
							}
							else
							if (bB){
								if (line.trim().length()>0){
									out.printOut(++count+"  ");
								}
							}
							
							if (bT)
								line = Format.replace(line, "\t", "^I");
							
							out.printOut(line);
							
							if (bE)
								out.printOut("$");
							
							out.printOutln();
						}
					}
					catch (IOException ioe){
						// ignore, cannot happen
					}
					
				
				}
				
					else if (!isSilent())
					out.printErrln("Could not read the contents of "
							+ fout.getAbsolutePath());
			} else if (!isSilent())
				out.printErrln("Not able to get the file " + alArguments.get(0));
		}
	}

	/**
	 * @param fileName catalogue file name to cat
	 * @return file handle for downloaded file
	 */
	public File catFile(final String fileName) {
		ArrayList<String> args = new ArrayList<String>(2);
		args.add("-t");
		args.add(fileName);

		JAliEnCommandcp cp;
		try {
			cp = (JAliEnCommandcp) JAliEnCOMMander.getCommand("cp",
					new Object[] { commander, out, args });
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		cp.silent();
		
		try {
			
			cp.start();
			while (cp.isAlive()) {
				Thread.sleep(500);
				if (!isSilent())
					out.pending();
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return cp.getOutputFile();
	}

	@Override
	public String deserializeForRoot() {
		logger.log(Level.INFO, toString());

		final StringBuilder ret = new StringBuilder();

		

			return ret.toString();
		
	
	//	return super.deserializeForRoot();

	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln(helpUsage("cat","[-options] [<filename>]"));
		out.printOutln(helpStartOptions());
		out.printOutln(helpOption("-o","outputfilename"));
		out.printOutln(helpOption("-n","number all output lines"));
		out.printOutln(helpOption("-b","number nonblank output lines"));
		out.printOutln(helpOption("-E","shows ends - display $ at end of each line number"));
		out.printOutln(helpOption("-T","show tabs -display TAB characters as ^I"));
		out.printOutln();
	}

	/**
	 * cat cannot run without arguments
	 * 
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * Constructor needed for the command factory in JAliEnCOMMander
	 * @param commander 
	 * @param out 
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandcat(JAliEnCOMMander commander, UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
		
			try {

				final OptionParser parser = new OptionParser();
			
				
				parser.accepts("o").withRequiredArg();
				parser.accepts("n");
				parser.accepts("b");
				parser.accepts("E");
				parser.accepts("T");
				
				final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));
				if (options.has("o") && options.hasArgument("o")) {
					bO = true;
					outputFileName = (String) options.valueOf("o");
					
				}
				
				alPaths = new ArrayList<String>(options.nonOptionArguments().size());
				alPaths.addAll(options.nonOptionArguments());

				
				bO = options.has("o");
				bN = options.has("n");
				bB = options.has("b");
				bE = options.has("E");
				bT = options.has("T");
				
			}
			catch (OptionException e) {
				printHelp();
				throw e;
			}
		}
	

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("\n { JAliEnCommandcatreceived\n");
		sb.append("Arguments: ");

		if(bG) sb.append(" -g ");
		if(bO) sb.append(" -o ");
		if(bN) sb.append(" -n ");
		if(bT) sb.append(" -T ");
		if(bB) sb.append(" -b ");
		if(bE) sb.append(" -E ");
		

		sb.append("}");

		return sb.toString();
	}
}
	

