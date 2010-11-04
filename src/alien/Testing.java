package alien;

import java.util.Set;
import java.util.UUID;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;

/**
 * Testing stuff
 * 
 * @author costing
 *
 */
public class Testing {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		UUID startingGUID = UUID.fromString("00270ff2-3bd3-11df-9bee-001cc45cb5dc");
		
		GUID guid = GUIDUtils.getGUID(startingGUID);
				
		System.err.println(guid);
		
		Set<LFN> lfns = GUIDUtils.getLFNsForGUID(guid);
		
		for (LFN lfn: lfns){
			System.err.println(lfn);
			System.err.println(lfn.getCanonicalName());
		}
		
		LFN lfn = LFNUtils.getLFN("/alice/sim/LHC10a18/140014/128/QA.root");
		
		LFN parent = lfn;
		
		while ( (parent = parent.getParentDir())!=null ){
			System.err.println(parent);
		}
	}
	
}
