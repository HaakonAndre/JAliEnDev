package alien.test.setup;

import java.util.UUID;

import alien.test.TestBrain;
import alien.test.TestConfig;
import alien.test.utils.Functions;
import alien.test.utils.TestCommand;
import alien.test.utils.TestException;

/**
 * @author ron
 * @since Sep 16, 2011
 */
public class CreateLDAP {


	/**
	 * the LDAP location
	 */
	public static final String ldap_home = TestConfig.jalien_home + "/slapd";
	
	/**
	 * the LDAP binary
	 */
	public static final String ldap_bin = ldap_home + "/slapd";

	/**
	 * the LDAP args file
	 */
	public static String ldap_args_file = "/tmp/jalien-slapd.args";
	
	/**
	 * the LDAP pid file
	 */
	public static String ldap_pid_file = "/tmp/jalien-slapd.pid";
	
	/**
	 * the LDAP pid
	 */
	public static String ldap_pid = null;
	
	
	/**
	 * @return state of rampUp
	 * @throws Exception
	 */
	public static boolean rampUpLDAP() throws Exception{
		createConfig();
		startLDAP();
		return true;
	}
	
	
	/**
	 */
	public static void startLDAP() throws Exception{
		TestCommand slapd = new TestCommand(new String[] {
				TestBrain.cBash,"-c",ldap_bin+ " -d 1 -s 0 -h ldap://127.0.0.1:"+ TestConfig.ldap_port + " -f " +
						TestConfig.ldap_conf_file + " > "+TestConfig.ldap_log+" 2>&1 &"});
		//slapd.verbose();
		slapd.exec();		
	}

	/**
	 * @throws Exception 
	 */
	public static void stopLDAP() throws Exception{
		
		ldap_pid = Functions.getFileContent(ldap_pid_file);
		
	
		TestCommand slapd = new TestCommand(new String[] {TestBrain.cKill,"-9",ldap_pid});
		slapd.verbose();
		if(!slapd.exec()){
			throw new TestException("Could not stop LDAP: \n STDOUT: " + slapd.getStdOut()+"\n STDERR: " + slapd.getStdErr());
		}
	}
	
	
	private static String hashPassword(String pass){
		TestCommand link = new TestCommand(new String[] { ldap_home+"/slappasswd", "-s","'"+pass+"'" });
		if(link.exec())
			return link.getStdOut();
		return null;
	}
	
	
	/**
	 * @throws Exception
	 */
	public static void createConfig() throws Exception {

		TestConfig.ldap_pass =  UUID.randomUUID().toString();
		
			
		
			Functions.writeOutFile(TestConfig.ldap_conf_file,
					slapd_config_content(hashPassword(TestConfig.ldap_pass)));
			
			Functions.writeOutFile(TestConfig.ldap_config,
					"password="+ TestConfig.ldap_pass +"\n");

	}
	
	private static String slapd_config_content(String pass_hash){
		
	return 
	"\n"+
			"#\n"+
			"# See slapd.conf(5) for details on configuration options.\n"+
			"# This file should NOT be world readable.\n"+
			"#\n"+
			"\n"+
			"include         "+ ldap_home+"/schema/core.schema\n"+
			"include         "+ ldap_home+"/schema/cosine.schema\n"+
			"include         "+ ldap_home+"/schema/nis.schema\n"+
			"include         "+ ldap_home+"/schema/alien.schema\n"+
			"\n"+
			"#referral      ldap://root.openldap.org/\n"+
			"\n"+
			"pidfile        "+ldap_pid_file+"\n"+
			"argsfile       "+ldap_args_file+"\n"+
			"\n"+
			"#######################################################################\n"+
			"# ldbm database definitions\n"+
			"#######################################################################\n"+
			"\n"+
			"moduleload back_bdb.la\n"+
			"\n"+
			"database       bdb\n"+
			"suffix         \""+TestConfig.ldap_suffix+"\"\n"+
			"rootdn         \"cn=Manager,"+TestConfig.ldap_suffix+"\"\n"+
			"rootpw         "+pass_hash+"\n"+
			"\n"+
			"\n"+
			"# cleartext passwords, especially for the rootdn, should\n"+
			"# be avoid.  See slapd.conf(5) for details.\n"+
			"\n"+
			"directory    "+ ldap_home+"\n"+
//			"\n"+
//			"TLSCipherSuite         HIGH:MEDIUM:+SSLv3\n"+
//			"TLSCertificateFile     "+TestConfig.host_cert+"\n"+
//			"TLSCertificateKeyFile  "+TestConfig.host_key+"\n"+
//			"TLSVerifyClient allow\n"+
			"\n"+
			"cachesize 2000\n"+
			"\n"+
			"# Any user with cn that contain Manager will not be listed when browsing\n"+
			"access to dn=\"uid=.*Manager,ou=People,o=.*,"+TestConfig.ldap_suffix+"\" attr=\"userpassword\" by self write \n"+
			"access to dn=\"uid=.*Manager,ou=People,o=.*,"+TestConfig.ldap_suffix+"\" by self read by * none \n"+
			"access to * by * read\n"+
		//	"\n"+
		//	"access to dn=\"ou=.*,ou=Sites,o=alice,dc=cern,dc=ch\" by self write\n"+
			"\n"
		;
	}
}
