package alien.test.setup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NameAlreadyBoundException;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

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
	public static final String ldap_schema = TestConfig.jalien_home + "/slapd_schema";

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
		try{
		stopLDAP();
		} catch(Exception e){
			// ignore
		}
		createConfig();
		startLDAP();

		Thread.sleep(2000); 
		
		initializeLDAP();
		return true;
	}
	
	
	/**
	 * @throws Exception 
	 */
	public static void startLDAP() throws Exception{
		TestCommand slapd = new TestCommand(new String[] {
				TestBrain.cBash,"-c",TestBrain.cSlapd+ " -d 1 -s 0 -h ldap://127.0.0.1:"+ TestConfig.ldap_port + " -f " +
						TestConfig.ldap_conf_file + " > "+TestConfig.ldap_log+" 2>&1 &"});
		slapd.verbose();
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
		TestCommand link = new TestCommand(new String[] { TestBrain.cSlappasswd, "-n", "-s",pass });
		if(link.exec())
			return link.getStdOut();
		return null;
	}

	/**
	 * @throws Exception
	 */
	public static void createConfig() throws Exception {
			
		
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
			"include         "+ ldap_schema+"/core.schema\n"+
			"include         "+ ldap_schema+"/cosine.schema\n"+
			"include         "+ ldap_schema+"/nis.schema\n"+
			"include         "+ ldap_schema+"/alien.schema\n"+
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
			"rootdn         \""+TestConfig.ldap_root+"\"\n"+
			"rootpw         "+pass_hash+"\n"+
			"\n"+
			"# cleartext passwords, especially for the rootdn, should\n"+
			"# be avoid.  See slapd.conf(5) for details.\n"+
			"\n"+
			"directory    "+ TestConfig.ldap_home+"\n"+
//			"\n"+
//			"TLSCipherSuite         ALL\n"+
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
	
	

	/**
	 * @param sParam 
	 * @param sRootExt 
	 * @param sKey 
	 * @param lValues 
	 * @return status of the entry add
	 */
	public static final boolean initializeLDAP(){
		

		try {

			final DirContext context = getLdapContext(TestConfig.ldap_suffix);
			final String orgName = "o="+TestConfig.host_name+"," + TestConfig.ldap_suffix;		 
		
			addBaseTypesToLDAP(context, orgName);
			   
			addInitConfigToLDAP(context, orgName);
			
			addUserToLDAP(context,orgName,"jalien","1001","admin");
			
			addRoleToLDAP(context, orgName, "admin", "jalien");
			addRoleToLDAP(context, orgName, "jalien", "jalien");
			
			
			String sitename = "JTestSite";
			String logdir = "/tmp";
			String cachedir = "/tmp";
			String tmpdir = "/tmp";
			
			addSiteToLDAP(context, orgName, sitename, TestConfig.domain, logdir, cachedir,
					tmpdir);

			
			context.close();
			   
		}
		catch (NamingException ne) {    
			ne.printStackTrace();
			}
		   return true;
		
	}
	
	
	
	private static void addSiteToLDAP(final DirContext context, final String orgName,
			final String sitename, final String domain,
			final String logdir, final String cachedir, final String tmpdir) {

		ArrayList<String> objClasses = new ArrayList<String>(2);
		objClasses.add("organizationalUnit");
		objClasses.add("AliEnSite");
		HashMap<String, Object> config = new HashMap<String, Object>();
		config.put("domain", domain);
		config.put("logdir", logdir);
		config.put("cachedir", cachedir);
		config.put("tmpdir", tmpdir);
		addToLDAP(context, objClasses, config, "ou=" + sitename + ",ou=Sites," + orgName);

		objClasses = new ArrayList<String>(1);
		objClasses.add("organizationalUnit");
		config = new HashMap<String, Object>();
		config.put("ou", "Config");
		addToLDAP(context, objClasses, config, "ou=Config,ou=" + sitename + ",ou=Sites,"
				+ orgName);
		
		config = new HashMap<String, Object>();
		config.put("ou", "Services");
		addToLDAP(context, objClasses, config, "ou=Services,ou=" + sitename + ",ou=Sites,"
				+ orgName);

		final String[] services = { "SE", "CE", "FTD", "PackMan" };

		for (String service : Arrays.asList(services)) {
			config = new HashMap<String, Object>();
			config.put("ou", service);
			addToLDAP(context, objClasses, config, "ou=" + service + ",ou=Services,ou="
					+ sitename + ",ou=Sites," + orgName);
		}

	}

	private static void addRoleToLDAP(final DirContext context, final String orgName, final String role, final String user){
		ArrayList<String> objClasses = new ArrayList<String>(1);
		objClasses.add("AliEnRole");
		HashMap<String, Object> config = new HashMap<String, Object>();	
		config.put("users", user);			
		addToLDAP(context, objClasses, config, "uid="+role+",ou=Roles," + orgName);
	}
	

	private static void addUserToLDAP(final DirContext context, final String orgName, final String user,final String uid, final String roles){

		ArrayList<String> objClasses = new ArrayList<String>(3);
		objClasses.add("posixAccount");
		objClasses.add("AliEnUser");
		objClasses.add("pkiUser");
		
		HashMap<String, Object> config = new HashMap<String, Object>();
		config.put("cn",user);
		config.put("uid",user);
		config.put("uidNumber",uid);
		config.put("gidNumber","1");
		config.put("homeDirectory",TestConfig.base_home_dir+"/"+user.substring(0,1)+"/"+user);
		config.put("userPassword","{crypt}x");
		config.put("loginShell","false");
		config.put("subject",TestConfig.certSubjectuser);
		config.put("roles", roles);			
		
		
		addToLDAP(context, objClasses, config, "uid="+user+",ou=People," + orgName);
	}
	
	private static void addBaseTypesToLDAP(final DirContext context, final String orgName){
		addToLDAP(context, "domain", TestConfig.ldap_suffix);
		addToLDAP(context, "organization", orgName);
		addToLDAP(context, "organizationalUnit",
				"ou=Packages,"+orgName);
		addToLDAP(context, "organizationalUnit",
				"ou=Institutions,"+orgName);
		addToLDAP(context, "organizationalUnit",
				"ou=Partitions,"+orgName);
		addToLDAP(context, "organizationalUnit",
				"ou=People,"+orgName);
		addToLDAP(context, "organizationalUnit",
				"ou=Roles,"+orgName);
		addToLDAP(context, "organizationalUnit",
				"ou=Services,"+orgName);
		addToLDAP(context, "organizationalUnit",
				"ou=Sites,"+orgName);

	}
	
	private static void addInitConfigToLDAP(final DirContext context, final String orgName){

		
		ArrayList<String> objClasses = new ArrayList<String>(1);
		objClasses.add("AliEnVOConfig");
		HashMap<String, Object> config = new HashMap<String, Object>();
		config.put("authPort", "8080");
		config.put("catalogPort", "8081");
		config.put("queuePort", "8082");
		config.put("logPort", "8082");
		config.put("isPort", "8082");
		config.put("clustermonitorPort", "8082");
		config.put("brokerPort", "8082");
		config.put("ldapmanager", TestConfig.ldap_suffix);
		config.put("processPort", "8082");
		config.put("processPort", "8082");
		config.put("brokerHost", "8082");
		config.put("isHost", "8082");
		config.put("logHost", "8082");
		config.put("catalogHost", "8082");
		config.put("queueHost", "8082");
		config.put("authHost", "8082");
				
		config.put("authenDatabase", "ADMIN");
		config.put("catalogDatabase", "8082");
		config.put("isDatabase", "8082");
		config.put("queueDatabase", "8082");
		config.put("isDbHost", "8082");
		config.put("queueDbHost", "8082");
		config.put("catalogHost", "8082");
		config.put("authenHost", "8082");
		config.put("queueDriver", "8082");
		config.put("catalogDriver", "8082");
		config.put("authenDriver", "8082");
				
				
		config.put("isDriver", "testVO/user");
		config.put("userDir", "8082");
		config.put("clusterMonitorUser", "8082");
		config.put("transferManagerAddress", "8082");
		config.put("transferBrokerAddress", "8082");
		config.put("transferOptimizerAddress", "8082");
		config.put("transferDatabase", "8082");
		config.put("jobOptimizerAddress", "8082");
		config.put("jobDatabase", "8082");
		config.put("catalogueOptimizerAddress", "8082");
		config.put("catalogueDatabase", "8082");
		config.put("lbsgAddress", "8082");
		config.put("lbsgDatabase", "8082");
		config.put("jobManagerAddress", "8082");
		config.put("jobBrokerAddress", "8082");
		config.put("authenSubject", TestConfig.certSubjectuser);
		config.put("packmanmasterAddress", "8082");
		config.put("messagesmasterAddress", "8082");
		config.put("semasterManagerAddress", "8082");
		config.put("semasterDatabase", "8082");
		config.put("jobinfoManagerAddress", "8082");

		addToLDAP(context, objClasses, config, "ou=Config,"+orgName);
	}
	
	
	
	private static void addToLDAP(final DirContext context, 
			final String objClass, final String attribute) {

				ArrayList<String> objClasses = new ArrayList<String>(1);
				objClasses.add(objClass);
		addToLDAP(context,objClasses, new HashMap<String,Object>(0),attribute);
	}
	
	private static void addToLDAP(final DirContext context, final ArrayList<String> objClasses,
			final HashMap<String,Object> objDesc, final String attribute) {

		BasicAttribute objClass = new BasicAttribute("objectClass","top");
		for(String objc: objClasses)
			objClass.add(objc);
		
		Attributes attrs = new BasicAttributes();
		attrs.put(objClass);

			for(String key: objDesc.keySet())
				attrs.put(new BasicAttribute(key, objDesc.get(key)));

		try {
			context.createSubcontext(attribute, attrs);
		} catch (NameAlreadyBoundException e) {
			System.out.println("Entry Already Exists Exception.");
		} catch (NamingException e) {
			e.printStackTrace();
		}

	}
	
	
	
	
	/**
	 * @param ldapRoot
	 * @return connected LDAP context
	 */
	public static DirContext getLdapContext(String ldapRoot) {
		DirContext ctx = null;
		try {
			Hashtable<String, String> env = new Hashtable<String, String>();
			env.put(Context.INITIAL_CONTEXT_FACTORY,
					"com.sun.jndi.ldap.LdapCtxFactory");
			env.put(Context.SECURITY_AUTHENTICATION, "Simple");
			env.put(Context.SECURITY_PRINCIPAL, TestConfig.ldap_root);
			System.out.println("LDAP LOGIN:" + TestConfig.ldap_root);
			env.put(Context.SECURITY_CREDENTIALS, TestConfig.ldap_pass);
			System.out.println("LDAP PASS:" + TestConfig.ldap_pass);
			env.put(Context.PROVIDER_URL, "ldap://localhost:"
					+ TestConfig.ldap_port + "/");

			System.out.println("LDAP URL: ldap://localhost:"
					+ TestConfig.ldap_port + "/");

			ctx = new InitialDirContext(env);

		} catch (NamingException nex) {
			System.out.println("LDAP Connection: FAILED");
			System.out.println("suffix: " + TestConfig.ldap_suffix);
			nex.printStackTrace();
		}

		return ctx;
	}
}
