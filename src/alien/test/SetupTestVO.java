package alien.test;

import java.io.File;

import alien.config.ConfigUtils;
import alien.test.setup.CreateCertificates;
import alien.test.setup.CreateLDAP;
import alien.test.utils.TestCommand;
import alien.test.utils.TestException;

/**
 * @author ron
 * @since Sep 09, 2011
 */
public class SetupTestVO {

	
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
			if (setupVO()) 
				System.out.println("VO Setup successful.");
			else
				System.out.println("VO Setup failed. No exceptions.");

		} catch (TestException e) {
			interrupt(e.getMessage());
			return;
		}

	}

	private static boolean setupVO() throws Exception {

		// step 0
		//--------------------------------------------------------------
		File oldLink = new File(TestConfig.tvo_home);
		if (oldLink.exists()){
			TestCommand link = new TestCommand(new String[] { "rm", oldLink.getAbsolutePath() });
			if(!link.exec())
				oldLink.renameTo(new File(TestConfig.tvo_home+"_movedBy"+TestConfig.now));
			if (oldLink.exists())
				throw new TestException ("Could not handle the old testVO entry: " + oldLink.getAbsolutePath());
		}
		
		TestConfig.initialize();
		
		System.out.println();
		System.out.println();
		System.out.println("Now let's start the work...");
		System.out.println("Creating TestVO in: " + TestConfig.tvo_real_home);

		if (!(new File(TestConfig.tvo_real_home)).mkdirs()) {
			throw new TestException("Could not create test VO directory.");
		}
		TestCommand link = new TestCommand(new String[] { "ln", "-s",
				TestConfig.tvo_real_home,TestConfig.tvo_home });
		if (!link.exec())
			throw new TestException(
					"VO Setup ok, but final link setting failed.");
		//--------------------------------------------------------------
		//
		
		boolean verbose = false;

		
		// step 1
		System.out.println("----- STEP1 [start]: Certificates -----");
		if (!CreateCertificates.doit(verbose))
			throw new TestException("Creating Certificates failed.");
		System.out.println("----- STEP1 [done]: Certificates -----");
		
		
		// step 2
		System.out.println("----- STEP2 [start]: Config -----");
		TestConfig.createConfig();
		System.out.println("----- STEP2 [done]: Config -----");
		
		// step 3
		System.out.println("----- STEP3 [start]: LDAP -----");
		CreateLDAP.rampUpLDAP();
		System.out.println("----- STEP3 [done]: LDAP -----");
		
		return true;
	}

	private static void interrupt(String message) {
//		System.err.println("We have a fatal problem: " + message);
		System.err.println("Shutting down...");
		System.err.println();
		System.err.println("May the force be with u next time!");
	}
}
