package alien.test.setup;

import java.util.ArrayList;

import alien.test.TestBrain;
import alien.test.TestConfig;
import alien.test.utils.Functions;
import alien.test.utils.TestCommand;

/**
 * @author ron
 * @since Sep 09, 2011
 */
public final class CreateCertificates {

	/**
	 * @param verbose 
	 * @return true if all tasks were successful
	 * @throws Exception 
	 */
	public static boolean doit(boolean verbose) throws Exception{


		if (!Functions.makeDirs(TestConfig.tvo_certs))
			return false;
		if (!Functions.makeDirs(TestConfig.tvo_trusts))
			return false;
		
		String usereq = TestConfig.tvo_certs + "/userreq.pem";

		ArrayList<TestCommand> commands = new ArrayList<TestCommand>();

		
		commands.add(new TestCommand(new String[] {TestBrain.cOpenssl, "genrsa", "-out", TestConfig.ca_key, "1024"}));
		commands.add(new TestCommand(new String[] {TestBrain.cChmod, "400", TestConfig.ca_key}));
		commands.add(new TestCommand(new String[] {TestBrain.cOpenssl, "req", "-new", "-batch", "-key", TestConfig.ca_key
				,"-x509", "-days", "365", "-out", TestConfig.ca_cert
				,"-subj", TestConfig.certSubjectCA}));
		commands.add(new TestCommand(new String[] {TestBrain.cChmod, "640", TestConfig.ca_cert}));

		Functions.execShell(commands,verbose);
		commands.clear();

		String hash = Functions.callGetStdOut(new String[] {TestBrain.cOpenssl,"x509", "-hash", "-noout", "-in", TestConfig.ca_cert});
		
		commands.add(new TestCommand(new String[] {TestBrain.cCp, TestConfig.ca_cert, TestConfig.tvo_trusts+"/"
				+ hash + ".0"}));
		// COMMANDS[6]="rm -rf $HOME/.globus;ln -s  $USERDIR $HOME/.globus"
		commands.add(new TestCommand(new String[] {TestBrain.cOpenssl, "req", "-nodes", "-newkey", "rsa:1024", "-out ", usereq
				,"-keyout", TestConfig.user_key
				,"-subj", TestConfig.certSubjectuser}));
		commands.add(new TestCommand(new String[] {TestBrain.cOpenssl, "x509", "-req", "-in"
				,usereq
				,"-CA", TestConfig.ca_cert, "-CAkey", TestConfig.ca_key, "-CAcreateserial", "-out", TestConfig.user_cert}));
		commands.add(new TestCommand(new String[] {TestBrain.cChmod, "640", TestConfig.user_cert}));
		commands.add(new TestCommand(new String[] {TestBrain.cChmod, "400", TestConfig.user_key}));
				
		
		return Functions.execShell(commands,verbose);

	}
}
