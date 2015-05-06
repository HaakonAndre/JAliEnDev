package alien.shell.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.util.StringTokenizer;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUIDUtils;

/**
 * @author psvirin
 *
 */
public class JAliEnCommandmirror extends JAliEnBaseCommand {
	private boolean keepSamePath;
	private boolean useLFNasGuid;
	private boolean checkFileIsPresentOnDest;
	private boolean transferWholeArchive;	
	private String collection;
	private Integer masterTransferId;
	private Integer attempts;
	private String lfn;
	private String dstSE;
	
	private int referenceCount = 0;
	private final List<String> ses = new ArrayList<>();
	private final List<String> exses = new ArrayList<>();
	private final HashMap<String, Integer> qos = new HashMap<>();
		
	
	/**
	 * @param commander
	 * @param out
	 * @param alArguments
	 */
	public JAliEnCommandmirror(JAliEnCOMMander commander, UIPrintWriter out,
			ArrayList<String> alArguments) {
		super(commander, out, alArguments);
		
		try {
			final OptionParser parser = new OptionParser();

			//parser.accepts("f");
			parser.accepts("try").withRequiredArg();
			parser.accepts("S").withRequiredArg();
			parser.accepts("g");
			//parser.accepts("u");
			//parser.accepts("m").withRequiredArg();

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			List<String> lfns = optionToString(options.nonOptionArguments());
			if( lfns==null || lfns.size()==0 ){
				return;
			}				
			this.lfn = lfns.get(0);			

			//keepSamePath = options.has("f");
			useLFNasGuid = options.has("g");
			checkFileIsPresentOnDest = options.has("u");
			transferWholeArchive = options.has("r");
						
			//if( options.has("m") )
			//	masterTransferId = Integer.parseInt( (String) options.valueOf("m") );
			if( options.has("try") )
				attempts = Integer.parseInt( (String) options.valueOf("try") );
			
			if (options.has("S") && options.hasArgument("S")) {
				if ((String) options.valueOf("S") != null) {
					final StringTokenizer st = new StringTokenizer((String) options.valueOf("S"), ",");
					while (st.hasMoreElements()) {
						final String spec = st.nextToken();
						if (spec.contains("::")) {
							if (spec.indexOf("::") != spec.lastIndexOf("::")) { // any
																				// SE
																				// spec
								if (spec.startsWith("!")){ // an exSE spec									
									exses.add(spec.substring(1).toUpperCase());
								}
								else {// an SE spec
									ses.add(spec.toUpperCase());
									referenceCount++;
								}
							}
						}
						else
							if (spec.contains(":")) {// a qosTag:count spec
								try {
									final int c = Integer.parseInt(spec.substring(spec.indexOf(':') + 1));
									if (c > 0) {
										qos.put(spec.substring(0, spec.indexOf(':')), Integer.valueOf(c));
										referenceCount = referenceCount + c;
									}
									else
										throw new JAliEnCommandException();

								}
								catch (final Exception e) {
									throw new JAliEnCommandException();
								}
							}
							else if (!spec.equals(""))
									throw new JAliEnCommandException();
					}
				}
			}
			else{
				if( lfns.size() != 2 )
					throw new JAliEnCommandException();
				this.dstSE = lfns.get(1);
			}
			System.out.println(this.exses);
		} catch (OptionException e) {
			printHelp();
			throw e;
		}		
	}

	@Override
	public void run(){
		if( this.dstSE==null || this.dstSE.length()==0 ){ 
			if( this.ses.size()==0 ){
				out.printErrln("No destination SEs specification found, please consult help for mirror command");
				return;
			}
			
			
		}
		
		if( this.useLFNasGuid && !GUIDUtils.isValidGUID( this.lfn ) ){
			out.printErrln("Invalid GUID was specified");
			return;
		}
		
		commander.c_api.mirrorLFN(FileSystemUtils.getAbsolutePath(
				commander.user.getName(),
				commander.getCurrentDir().getCanonicalName(),
				lfn),
				this.dstSE,
				//this.keepSamePath,
				this.useLFNasGuid,
				//this.checkFileIsPresentOnDest,
				//this.transferWholeArchive,												
				//this.masterTransferId,
				this.attempts
				);
	}

	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln("mirror Copies a file into another SE");
		out.printOutln(" Usage:");
		out.printOutln("	mirror [-g] [-try <number>] [-S [se[,se2[,!se3[,qos:count]]]]] <lfn> [<SE>]");
		//out.printOutln(" Options:        -f       keep the same relative path");
		out.printOutln("                 -g:      Use the lfn as a guid");
		out.printOutln("                 -S:     specifies the destination SEs to be used");
		//out.printOutln("                 -m <id>  Put the transfer under the masterTransfer of <id>");
		//out.printOutln("                 -u       Don't issue the transfer if the file is already in that SE");
		//out.printOutln("                 -r       If the file is in a zip archive, transfer the whole archive");
		out.printOutln("                 -try <NumOfAttempts>     Specifies the number of attempts to try and mirror the file");
		out.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}	
}