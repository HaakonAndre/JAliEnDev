package alien.test;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import alien.test.setup.CreateCertificates;
import alien.test.setup.CreateLDAP;
import alien.test.utils.TestCommand;
import alien.test.utils.TestException;

/**
 * @author ron
 * @since Sep 09, 2011
 */
public class SetupTestVO {

	
	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

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
		if (!CreateCertificates.doit(verbose))
			throw new TestException("Creating Certificates failed.");
		// step 2
		TestConfig.createConfig();
		
		// step 3
		CreateLDAP.rampUpLDAP();

		
		return true;
	}

	private static void interrupt(String message) {
		System.err.println("We have a fatal problem: " + message);
		System.err.println("Shutting down...");
		System.err.println();
		System.err.println("May the force be with u next time!");
	}
}
