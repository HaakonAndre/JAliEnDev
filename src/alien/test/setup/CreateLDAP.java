package alien.test.setup;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.UUID;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.ModificationItem;


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
		
		addDefaultEntries();
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
	
	private static void addDefaultEntries(){
		
		String s_orgName = "alicechen";
		
		
		ArrayList<String> vals = new ArrayList<String>();
	//	vals.add("o");
		vals.add(s_orgName);
		vals.add("objectClass");
		vals.add("organization");

		addLdapEntry("o="+s_orgName,"dc="+TestConfig.ldap_suffix,"o",vals);
		
	}
	
	
	/**
	 * @throws Exception
	 */
	public static void createConfig() throws Exception {

		TestConfig.ldap_pass =  UUID.randomUUID().toString();
		
		TestConfig.ldap_pass = "abc";
		
			
		
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
			"rootdn         \""+TestConfig.ldap_root+"\"\n"+
			"rootpw         "+pass_hash+"\n"+
			"\n"+
			"# cleartext passwords, especially for the rootdn, should\n"+
			"# be avoid.  See slapd.conf(5) for details.\n"+
			"\n"+
			"directory    "+ ldap_home+"\n"+
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
	public static final boolean addLdapEntry(final String sParam, final String sRootExt, final String sKey,  ArrayList<String> lValues){
		

		try {
			final String dirRoot = TestConfig.ldap_suffix;
//			final String dirRoot = sRootExt+ldapRoot;

			final DirContext context = getLdapContext(dirRoot);
		//	DirContext schema = context.getSchema("");
//
//			dn: dc=localdomain
//					dc: localdomain
//					description: The best company in the whole world
//					objectClass: dcObject
//					objectClass: organization
//					o: localhost
		//	Attributes container = new BasicAttributes(true);
			
		//	Attribute objClasses = new BasicAttribute("o","horschd");
			//container.add("top");
			//container.add("organization");
		//	container.put(objClasses);
			//context.modifyAttributes(
			//	    "cn=localdomain,", DirContext.ADD_ATTRIBUTE, container);
			
//			
			   Attributes attrs = new BasicAttributes();
			   attrs.put(new BasicAttribute("objectClass","top"));
			   attrs.put(new BasicAttribute("objectClass","domain"));
			   attrs.put(new BasicAttribute("dc","localdomain"));
//			     
			   context.modifyAttributes("o=horschd,dc=localdomain",
					   DirContext.ADD_ATTRIBUTE,attrs);
//			   
//			   context.createSubcontext("dc=localdomain", attrs);
//			   
//			   context.bind("dc=localdomain", attrs);
			   
//			   
			   attrs = new BasicAttributes();
			   attrs.put(new BasicAttribute("objectClass","top"));
			   attrs.put(new BasicAttribute("objectClass","organization"));
			   attrs.put(new BasicAttribute("o","horschd"));
			     
			//   context.modifyAttributes("o=horschd",
			//		   DirContext.ADD_ATTRIBUTE,attrs);
			   
			//   context.createSubcontext("o=horschd", attrs);
			   
			   context.bind("o=horschd", attrs);
//			   
//			   
//			
//		    ModificationItem[] mods = new ModificationItem[2];
//			mods[0] = new ModificationItem(DirContext.ADD_ATTRIBUTE, new BasicAttribute("o", "horschd"));
//			mods[1] = new ModificationItem(DirContext.ADD_ATTRIBUTE, new BasicAttribute("o", "horschd"));
//
//			
//			context.modifyAttributes(dirRoot, mods);
//			
//			context.modifyAttributes("dc=localdomain",
//					   DirContext.ADD_ATTRIBUTE,
//					   new BasicAttributes("domain", "top"));
//			
//			context.modifyAttributes("o=horsch,dc=localdomain",
//					   DirContext.ADD_ATTRIBUTE,
//					   new BasicAttributes("organization","o"));
//			
//			context.modifyAttributes("uid=alee,ou=people,o=horsch,dc=localdomain",
//					   DirContext.ADD_ATTRIBUTE,
//					   new BasicAttributes("title", "CTO"));
			
			
			
			
//			   Attributes attrs = new BasicAttributes(true);
//			   Attribute objclass = new BasicAttribute("organization");
//			   objclass.add("dcObject");
//			  // objclass.add("organization");
//			   attrs.put(objclass);\
//			
//			 container = new BasicAttributes(true);
//
//			objClasses = new BasicAttribute("objectClass");
//	        objClasses.add("top");
//	        objClasses.add("organization");
//	        Attribute cn = new BasicAttribute("o","horschd");
//	      //  cn.add("horschd");
//	        container.put(objClasses);
//	        container.put(cn);
////			   attrs.put(sParam+","+sRootExt+","+sKey,"asdf");
////			   attrs.put("o, dc=localdomain, localhorschd"
//	        context.createSubcontext("o=horschd,dc=localdomain", container);
//	
			
			System.out.println("Context::: " + context);
			
			context.close();
			   
		}
		catch (NamingException ne) {    
			ne.printStackTrace();
			}
		   return true;
		
	}
	
	
	/**
	 * @param ldapRoot 
	 * @return connected LDAP context
	 */
	public static DirContext getLdapContext(String ldapRoot){
		DirContext ctx = null;
		try{
			Hashtable<String, String> env = new Hashtable<String, String>();
			env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
			env.put(Context.SECURITY_AUTHENTICATION, "Simple");
			env.put(Context.SECURITY_PRINCIPAL,  TestConfig.ldap_root);
			System.out.println("SECURITY_PRINCIPAL:" +  TestConfig.ldap_root);
			env.put(Context.SECURITY_CREDENTIALS, TestConfig.ldap_pass);
			System.out.println("SECURITY_CREDENTIALS:" + TestConfig.ldap_pass);
			env.put(Context.PROVIDER_URL, "ldap://localhost:"+TestConfig.ldap_port+"/");
			
			System.out.println("PROVIDER_URL: ldap://localhost:"+TestConfig.ldap_port+"/");
 	      // 	env.put(Context.SECURITY_PROTOCOL, "none");

 	  //     env.put(LdapContext.CONTROL_FACTORIES, "com.sun.jndi.ldap.ControlFactory");
 	       	
			//ctx = new InitialLdapContext(env,null);
	        
                // obtain initial directory context using the environment
                ctx = new InitialDirContext( env );
                
                
             // List the objects 
//                NamingEnumeration namingEnum = ctx.list(TestConfig.ldap_suffix);
//                while (namingEnum.hasMore()) {
//                     System.out.println(namingEnum.next());
//                }
               // ctx.close();
                
                
                // now, create the root context, which is just a subcontext
                // of this initial directory context.
                // ctx.createSubcontext( TestConfig.ldap_suffix);
       
	        // Now return the root directory context.
	        //
	        
//			ctx = new InitialDirContext(env);
			System.out.println("Connection Successful.");
			
//			try {
				//return (DirContext)(ctx.lookup("cn"));
				return ctx;
//			} catch (NamingException e) {
//				
//				e.printStackTrace();
//			}
			
		}catch(NamingException nex){
			System.out.println("LDAP Connection: FAILED");
			System.out.println("suffix: " + TestConfig.ldap_suffix);
			nex.printStackTrace();
		}
//		return ctx;

		return ctx;
	}
}
