package alien;

import java.util.Set;
import java.util.UUID;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;

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
	}
	
}
