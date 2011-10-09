package alien.test.chapters;

import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.test.TestConfig;


/**
 * @author ron
 * @since October 09, 2011
 */
public class TestCentralUtils {
	
	
	/**
	 * @return status
	 */
	public static boolean testChaper(){
		System.out.println("--------------------------------------");	
		System.out.println("----- TEST1 [INIT]: ooooooo -----");
	
		
		System.out.println("aliConfig is: "+System.getProperty("AliEnConfig"));
		
		String getIt = TestConfig.base_home_dir;
		getIt = "/localdomain/";
		System.out.println("getIt: " + getIt);
		
		LFN l1 = LFNUtils.getLFN(getIt);

		System.out.println("---");
		if(l1!=null){
			System.out.println("LFN: " + l1.getCanonicalName());
			System.out.println("LFN: " + l1.list());
		}
		else
			System.out.println("lfn null.");
	
		System.out.println("----- TEST2 [DONE]: ooooooo -----");

	
	
		System.out.println("--------------------------------------");	
	
		return true;
	
	}
}


