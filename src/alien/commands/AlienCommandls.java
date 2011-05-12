package alien.commands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;

public class AlienCommandls extends AlienCommand {

	public AlienCommandls(ArrayList<Object> al) throws Exception {
		super(al);
	}

	public AlienCommandls (final String sUsername, final String sCurrentDirectory, final String sCommand, final ArrayList<Object> alArguments) throws Exception {
		super(sUsername, sCurrentDirectory, sCommand, alArguments);
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

		//we got arguments for ls
		if(this.alArguments != null && this.alArguments.size() > 0){
			return null;
		}
		else{
			//listing current directory	
			final LFN entry = LFNUtils.getLFN(this.sCurrentDirectory);

			//what message in case of error?
			if (entry != null){

				List<LFN> lLFN;
				
				if (entry.type=='d'){
					lLFN = entry.list();
				}
				else
					lLFN = Arrays.asList(entry);
			
				for(LFN localLFN : lLFN){
					alrcValues.add(localLFN.getName());
					alrcMessages.add(localLFN.getName()+"\n");
				}
			}
		}

		hmReturn.put("rcvalues", alrcValues);
		hmReturn.put("rcmessages", alrcMessages);

		return hmReturn;
	}

}
