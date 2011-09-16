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

		// export PATH=$ALIEN_ROOT/bin:$PATH
		// export LD_LIBRARY_PATH=$ALIEN_ROOT/lib:$LD_LIBRARY_PATH
		// export DYLD_LIBRARY_PATH=$ALIEN_ROOT/lib:$DYLD_LIBRARY_PATH
		//
		// USERDIR=$HOME/.alien/globus
		// DIR=$HOME/.alien/etc/aliend/ldap/certs/
		// CA_KEY=$HOME/.alien/globus/ca_key.pem
		// CA_CERT=$HOME/.alien/globus/ca_cert.pem
		//
		// HOST_KEY=$DIR/host.key.pem
		// HOST_CERT=$DIR/host.cert.pem


		commands.add(new TestCommand(new String[] {TestBrain.cOpenssl, "genrsa", "-out", TestConfig.ca_key, "1024"}));
		commands.add(new TestCommand(new String[] {TestBrain.cChmod, "400", TestConfig.ca_key}));
		commands.add(new TestCommand(new String[] {TestBrain.cOpenssl, "req", "-new", "-batch", "-key", TestConfig.ca_key
				,"-x509", "-days", "365", "-out", TestConfig.ca_cert
				,"-subj", "/C=CH/O=AliEn/CN=AlienCA"}));
		commands.add(new TestCommand(new String[] {TestBrain.cChmod, "640", TestConfig.ca_cert}));

		Functions.execShell(commands,verbose);
		commands.clear();

		String hash = Functions.callGetStdOut(new String[] {TestBrain.cOpenssl,"x509", "-hash", "-noout", "-in", TestConfig.ca_cert});
		
		commands.add(new TestCommand(new String[] {TestBrain.cCp, TestConfig.ca_cert, TestConfig.tvo_trusts+"/"
				+ hash + ".0"}));
		// COMMANDS[6]="rm -rf $HOME/.globus;ln -s  $USERDIR $HOME/.globus"
		commands.add(new TestCommand(new String[] {TestBrain.cOpenssl, "req", "-nodes", "-newkey", "rsa:1024", "-out ", usereq
				,"-keyout", TestConfig.user_key
				,"-subj", "/C=CH/O=AliEn/CN=test-user-cert"}));
		commands.add(new TestCommand(new String[] {TestBrain.cOpenssl, "x509", "-req", "-in"
				,usereq
				,"-CA", TestConfig.ca_cert, "-CAkey", TestConfig.ca_key, "-CAcreateserial", "-out", TestConfig.user_cert}));
		commands.add(new TestCommand(new String[] {TestBrain.cChmod, "640", TestConfig.user_cert}));
		commands.add(new TestCommand(new String[] {TestBrain.cChmod, "400", TestConfig.user_key}));
		
		
		
		
//		commands.add(new TestCommand(new String[] {"cp -f " + user_cert + "$HOST_CERT"}));
//		commands.add(new TestCommand(new String[] {"cp -f " + user_cert + "$HOST_KEY"}));
		// COMMANDS[12]="$ALIEN_ROOT/bin/alien config "
		//
		// which openssl
		// EXECUTE_SHELL "CREATE_CERT"
		//
		// CAFILE=$ALIEN_ROOT/globus/share/certificates/`openssl x509 -hash
		// -noout < $CA_CERT`.0
		// SIGNING_POLICY_FILE=$ALIEN_ROOT/globus/share/certificates/`openssl
		// x509 -in $CA_CERT -noout -hash`.signing_policy
		// SUBJECT=`openssl x509 -subject -noout < $CA_CERT|awk -F' ' '{print
		// $2}'`
		//
		// echo "Let's create siging policy file"
		// echo "access_id_CA X509 '$SUBJECT'" > $SIGNING_POLICY_FILE
		// echo "pos_rights globus CA:sign" >> $SIGNING_POLICY_FILE
		// echo "cond_subjects globus '*'" >> $SIGNING_POLICY_FILE
		//
		//
		// echo "Let's check if the file $CAFILE exists"
		// [ -f "$CAFILE" ] || echo -n "WARNING THE CA IS NOT IN X509_CERT_DIR"
		// echo "Checking if the certificate is ok"
		// openssl verify -CApath $ALIEN_ROOT/globus/share/certificates -purpose
		// sslclient $USERDIR/user_cert.pem
		// exit;
		//
		
		
		return Functions.execShell(commands,verbose);

	}
}
