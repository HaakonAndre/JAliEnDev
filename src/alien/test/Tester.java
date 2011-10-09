package alien.test;

import alien.config.ConfigUtils;
import alien.test.chapters.TestCentralUtils;
import alien.test.utils.TestException;



/**
 * @author ron
 * @since October 09, 2011
 */
public class Tester {

	
	static{
		ConfigUtils.getVersion();
	}
	
	

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		
		System.out.println();
		System.out.println();
		System.out.println();
		System.out.println("---- jAliEn Test System ----");
		System.out.println("Starting ...");
		
		

		if (!TestBrain.findCommands()) {
			interrupt("Necessary programs missing.");
			return;
		}

		try {
			if (SetupTestVO.setupVO()) 
				System.out.println("VO Setup successful.");
			else
				System.out.println("VO Setup failed. No exceptions.");
			
			
			if (TestCentralUtils.testChaper()) 
				System.out.println("TestCentralUtils successful.");
			else
				System.out.println("TestCentralUtils failed. No exceptions.");
			
			
		} catch (TestException e) {
			interrupt(e.getMessage());
			return;
		}

	}
	
	

	private static void interrupt(String message) {
		System.err.println("We have a fatal problem: " + message);
		System.err.println("Shutting down...");
		System.err.println();
		System.err.println("May the force be with u next time!");
	}
	
	

}


