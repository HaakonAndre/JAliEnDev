package alien;

import java.util.Set;

import lazyj.DBFunctions;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.config.ConfigUtils;

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
		DBFunctions db = ConfigUtils.getDB("alice_users");
		
		db.query("SELECT * FROM G94L WHERE guidId=67271284");
		
		GUID guid = new GUID(db);
		
		System.err.println(guid);
		
		Set<LFN> lfns = GUIDUtils.getLFNsForGUID(guid);
		
		for (LFN lfn: lfns){
			System.err.println(lfn);
			System.err.println(lfn.getCanonicalName());
		}
	}
	
}
