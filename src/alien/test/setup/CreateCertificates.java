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
		
		String userreq = TestConfig.tvo_certs + "/userreq.pem";
		String hostreq = TestConfig.tvo_certs + "/hostreq.pem";

		ArrayList<TestCommand> commands = new ArrayList<TestCommand>();

		// CA CERT/KEY:
		commands.add(new TestCommand(new String[] {TestBrain.cOpenssl, "genrsa", "-out", TestConfig.ca_key, "1024"}));
		commands.add(new TestCommand(new String[] {TestBrain.cChmod, "400", TestConfig.ca_key}));
		commands.add(new TestCommand(new String[] {TestBrain.cOpenssl, "req", "-new", "-batch", "-key", TestConfig.ca_key
				,"-x509", "-days", "365", "-out", TestConfig.ca_cert
				,"-subj", TestConfig.certSubjectCA}));
		commands.add(new TestCommand(new String[] {TestBrain.cChmod, "640", TestConfig.ca_cert}));

		Functions.execShell(commands,verbose);
		commands.clear();

		String hash = Functions.callGetStdOut(new String[] {TestBrain.cOpenssl,"x509", "-hash", "-noout", "-in", TestConfig.ca_cert});
		
		commands.add(new TestCommand(new String[] {TestBrain.cOpenssl,"x509", "-inform","PEM","-in", TestConfig.ca_cert
				,"-outform","DER","-out",TestConfig.tvo_trusts+"/" + hash + ".der"}));

		
		
		// USER CERT/KEY:
		commands.add(new TestCommand(new String[] {TestBrain.cOpenssl, "req", "-nodes", "-newkey", "rsa:1024", "-out ", userreq
				,"-keyout", TestConfig.user_key
				,"-subj", TestConfig.certSubjectuser}));
		commands.add(new TestCommand(new String[] {TestBrain.cOpenssl, "x509", "-req", "-in"
				,userreq
				,"-CA", TestConfig.ca_cert, "-CAkey", TestConfig.ca_key, "-CAcreateserial", "-out", TestConfig.user_cert}));
		commands.add(new TestCommand(new String[] {TestBrain.cChmod, "640", TestConfig.user_cert}));
		commands.add(new TestCommand(new String[] {TestBrain.cChmod, "400", TestConfig.user_key}));
		
		Functions.execShell(commands,verbose);
		commands.clear();
		
		hash = Functions.callGetStdOut(new String[] {TestBrain.cOpenssl,"x509", "-hash", "-noout", "-in", TestConfig.user_cert});

		commands.add(new TestCommand(new String[] {TestBrain.cOpenssl,"x509", "-inform","PEM","-in", TestConfig.user_cert
				,"-outform","DER","-out",TestConfig.tvo_trusts+"/" + hash + ".der"}));
		
		
		// HOST CERT/KEY:
		commands.add(new TestCommand(new String[] {TestBrain.cOpenssl, "req", "-nodes", "-newkey", "rsa:1024", "-out ", hostreq
				,"-keyout", TestConfig.host_key
				,"-subj", TestConfig.certSubjecthost}));
		commands.add(new TestCommand(new String[] {TestBrain.cOpenssl, "x509", "-req", "-in"
				,hostreq
				,"-CA", TestConfig.ca_cert, "-CAkey", TestConfig.ca_key, "-CAcreateserial", "-out", TestConfig.host_cert}));
		commands.add(new TestCommand(new String[] {TestBrain.cChmod, "640", TestConfig.host_cert}));
		commands.add(new TestCommand(new String[] {TestBrain.cChmod, "400", TestConfig.host_key}));
		
		Functions.execShell(commands,verbose);
		commands.clear();

		hash = Functions.callGetStdOut(new String[] {TestBrain.cOpenssl,"x509", "-hash", "-noout", "-in", TestConfig.host_cert});
		
		commands.add(new TestCommand(new String[] {TestBrain.cOpenssl,"x509", "-inform","PEM","-in", TestConfig.host_cert
				,"-outform","DER","-out",TestConfig.tvo_trusts+"/" + hash + ".der"}));
		
		
		createAuthenAndSEKey();
		
		return Functions.execShell(commands,verbose);

	}
	
	/**
	 * 
	 */
	public static void createAuthenAndSEKey(){
		
		Functions.callGetStdOut(new String[] {TestBrain.cOpenssl, "req", "-x509", "-nodes", "-days", "365"
				, "-newkey", "rsa:1024", "-keyout", TestConfig.jAuthZ_priv, "-out", TestConfig.jAuthZ_pub, "-subj", TestConfig.certSubjectjAuthZ});
		
		
		Functions.callGetStdOut(new String[] {TestBrain.cOpenssl, "req", "-x509", "-nodes", "-days", "365"
				, "-newkey", "rsa:1024", "-keyout", TestConfig.SE_priv, "-out", TestConfig.SE_pub, "-subj", TestConfig.certSubjectSE});
		
	}
	
	
	
}
