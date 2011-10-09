package alien.test;

import java.io.File;

import alien.test.setup.CreateCertificates;
import alien.test.setup.CreateDB;
import alien.test.setup.CreateLDAP;
import alien.test.setup.ManageSysEntities;
import alien.test.utils.TestCommand;
import alien.test.utils.TestException;

/**
 * @author ron
 * @since Sep 09, 2011
 */
public class SetupTestVO {
	
	
	protected static boolean setupVO() throws Exception {

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
		System.out.println("----- STEP1 [INIT]: Certificates -----");
		if (!CreateCertificates.doit(verbose))
			throw new TestException("Creating Certificates failed.");
		System.out.println("----- STEP1 [DONE]: Certificates -----");
		
		
		// step 2
		System.out.println("----- STEP2 [INIT]: Config -----");
		TestConfig.createConfig();
		System.out.println("----- STEP2 [DONE]: Config -----");
		
		// step 3
		System.out.println("----- STEP3 [INIT]: LDAP -----");
		if (!CreateLDAP.rampUpLDAP())
			throw new TestException("Initializing LDAP failed.");
		System.out.println("----- STEP3 [DONE]: LDAP -----");

		// step 4
		System.out.println("----- STEP4 [INIT]: DB -----");
		if (!CreateDB.rampUpDB())
			throw new TestException("Initializing DB failed.");
		System.out.println("----- STEP4 [DONE]: DB -----");

		// step 4
		System.out.println("----- STEP5 [INIT]: init VO -----");
		if (!rampUpVO())
			throw new TestException("Initializing VO failed.");
		System.out.println("----- STEP5 [DONE]: init VO -----");
				
		return true;
	}
	
	
	private static boolean rampUpVO() throws Exception{
	
		boolean ret = true;
		
		if(!ManageSysEntities.addUser("jalien","1","admin"))
			ret = false;
		if(!ManageSysEntities.addSite("JTestSite", TestConfig.domain, "/tmp", "/tmp", "/tmp"))
			ret = false;
		if(!ManageSysEntities.addSE("firstse","1","JTestSite", TestConfig.full_host_name+":8092", "disk"))
			ret = false;
		
		return ret;
	
	}
	
}
