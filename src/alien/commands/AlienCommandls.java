package alien.commands;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import lazyj.Log;

import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;

public class AlienCommandls extends AlienCommand {
	private static ArrayList<String> lsArguments = new ArrayList<String>();

	static{
		lsArguments.add("help");
		lsArguments.add("l");
		lsArguments.add("a");
	}

	private boolean bHelp =  false;
	private boolean bL = false;
	private boolean bA = false;

	public AlienCommandls(final Principal p, final ArrayList<Object> al) throws Exception {
		super(p, al);
	}

	public AlienCommandls (final Principal p, final String sUsername, final String sCurrentDirectory, final String sCommand, final List alArguments) throws Exception {
		super(p, sUsername, sCurrentDirectory, sCommand, alArguments);
	}

	/*
	 * ls returns a map of <String, List<String>> with only 2 keys <br />
	 * 	- rcvalues - the list of files
	 * 	- rcmessages - the list of files with an extra \n at the end of the file name
	 */
	@Override
	public HashMap<String, ArrayList<String>> executeCommand() {
		HashMap<String, ArrayList<String>> hmReturn = new HashMap<String, ArrayList<String>>();

		ArrayList<String> alrcValues = new ArrayList<String>();
		ArrayList<String> alrcMessages = new ArrayList<String>();

		ArrayList<String> alPaths = new ArrayList<String>();

		//we got arguments for ls
		if(this.alArguments != null && this.alArguments.size() > 0){

			for(Object oArg: this.alArguments){
				String sArg = (String) oArg;

				//we got an argument
				if(sArg.startsWith("-")){
					if(sArg.length() == 1){
						alrcMessages.add("Expected argument after -. \n ls -help for more help");
					}
					else{
						String sLocalArg = sArg.substring(1);

						if(!lsArguments.contains(sLocalArg)){
							alrcMessages.add("Unknown argument "+sArg+"! \n ls -help for more help");
						}
						else{
							if("l".equals(sLocalArg))
								bL = true;

							if("a".equals(sLocalArg))
								bA = true;
						}
					}
				}
				else{
					//we got paths
					alPaths.add(sArg);
				}
			}
		}
		else{
			alPaths.add(this.sCurrentDirectory);
		}

		int iDirs = alPaths.size();
		
		for(String sPath: alPaths){
			//listing current directory	
			final LFN entry = LFNUtils.getLFN(sPath);

			//what message in case of error?
			if (entry != null){

				List<LFN> lLFN;

				if (entry.type=='d'){
					lLFN = entry.list();
				}
				else
					lLFN = Arrays.asList(entry);

				if(iDirs != 1){
					alrcMessages.add(sPath+"\n");
				}
				
				for(LFN localLFN : lLFN){
					alrcValues.add(bL ? localLFN.getName() : localLFN.getFileName());
					alrcMessages.add( bL ? localLFN.getName()+"\n" : localLFN.getFileName()+"\n");
				}
			}
			else{
				alrcMessages.add("No such file or directory");
			}
		}

		hmReturn.put("rcvalues", alrcValues);
		hmReturn.put("rcmessages", alrcMessages);

		return hmReturn;
	}

}
