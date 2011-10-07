package alien.test;

import java.io.File;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;

import alien.test.utils.Functions;
import alien.test.utils.TestException;

/**
 * @author ron
 * @since Sep 16, 2011
 */
public class TestConfig {

	
	/**
	 * default home of jalien
	 */
	public static final String jalien_home = System.getProperty("user.home")
			+ "/.alien";
	
	/**
	 * default home of the testVO
	 */
	public static final String tvo_home = jalien_home + "/testVO";

	/**
	 * testVO creation time stamp
	 */
	public static final String now = (new SimpleDateFormat("HHmmss-yyyyMMdd"))
			.format(new Date());

	/**
	 * home of the testVO
	 */
	public static final String tvo_real_home = tvo_home + "_" + now;
	/**
	 * the testVO certificate location
	 */
	public static final String tvo_certs = tvo_home + "/globus";

	/**
	 * the testVO certificate location
	 */
	public static final String tvo_trusts = tvo_home + "/trusts";

	/**
	 * the testVO config location
	 */
	public static final String tvo_config = tvo_home + "/config";
	

	/**
	 * the testVO config location
	 */
	public static final String tvo_logs = tvo_home + "/logs";
	

	/**
	 * the testVO ldap config location
	 */
	public static final String ldap_config = tvo_config + "/ldap.config";
	
	/**
	 * the testVO ldap conf file location
	 */
	public static final String ldap_conf_file = tvo_config + "/slapd.conf";

	/**
	 * the testVO ldap log file location
	 */
	public static final String ldap_log = tvo_logs + "/lapd.log";
	
	

	/**
	 * the user's private key
	 */
	public static String user_key = tvo_certs + "/userkey.pem";

	/**
	 * the user's cert
	 */
	public static String user_cert = tvo_certs + "/usercert.pem";

	/**
	 * the ca's private key
	 */
	public static String ca_key = tvo_certs + "/cakey.pem";

	/**
	 * the ca's cert
	 */
	public static String ca_cert = tvo_certs + "/cacert.pem";

	/**
	 * the host's private key
	 */
	public static String host_key = tvo_certs + "/hostkey.pem";

	/**
	 * the host's cert
	 */
	public static String host_cert = tvo_certs + "/hostcert.pem";

	/**
	 * port number for the LDAP server
	 */
	public static final int ldap_port = 8389;

	/**
	 * port number for the SQL server
	 */
	public static final int sql_port = 8389;

	/**
	 * port number for the API server
	 */
	public static final int api_port = 8998;

	/**
	 * the fully qualified host name
	 */
	public static String full_host_name;

	/**
	 * the LDAP root string
	 */
	public static String ldap_root;
	
	/**
	 * the LDAP suffix string
	 */
	public static String ldap_suffix;
	
	/**
	 * the LDAP pass string
	 */
	public static String ldap_pass;

	/**
	 * the LDAP root string
	 */
	public static String base_home_dir;

	/**
	 * @param tvohome
	 * @throws Exception
	 */
	public static void initialize() throws Exception {

		full_host_name = InetAddress.getByName("127.0.0.1")
				.getCanonicalHostName();
		
		

		String[] host = full_host_name.split(".");
		// fixed overwrite:
		//host = new String[] { "jtvo", "cern", "ch" };
		host = new String[] { "localhost", "localdomain"};
		ldap_suffix = "dc=" + host[1]; // + ",dc=" + host[2];
		ldap_root = "cn=Manager,"+TestConfig.ldap_suffix;
		base_home_dir = "/" + host[1] + "/" + host[0] // + "." + host[2]
				+ "/user/";

	}

	/**
	 * @throws Exception
	 */
	public static void createConfig() throws Exception {
		File config = new File(tvo_config);
		if (config.mkdir()){
			Functions.writeOutFile(tvo_config + "/config.properties",
					getConfigProperties());
			Functions.writeOutFile(tvo_config + "/alice_data.properties",
					getDatabaseProperties("pass","alice_data"));
			Functions.writeOutFile(tvo_config + "/alice_users.properties",
					getDatabaseProperties("pass","alice_users"));
			Functions.writeOutFile(tvo_config + "/processes.properties",
					getDatabaseProperties("pass","processes"));
			Functions.writeOutFile(tvo_config + "/logging.properties",
					getLoggingProperties());
		}
		File logs = new File(tvo_logs);
		if (!logs.mkdir())
			throw new TestException("Could not create log directory: " + tvo_logs);
	}

	/**
	 * @return the content for the config.properties file
	 */
	private static String getConfigProperties() {
		return "\n" + "ldap_server = " + full_host_name + ":" + ldap_port
				+ "\n" + "ldap_root = " + ldap_root + "\n"
				+ "alien.users.basehomedir = " + base_home_dir + "\n" + "\n"
				+ "apiService = + " + full_host_name + ":" + api_port + "\n"
				+ "\n" + "trusted.certificates.location = + " + tvo_trusts
				+ "\n" + "host.cert.priv.location = + " + host_key + "\n"
				+ "host.cert.pub.location = + " + host_cert + "\n"
				+ "user.cert.priv.location = + " + user_key + "\n"
				+ "user.cert.pub.location = + " + user_cert + "\n"
		
		
		+"\n";
	}
	
	/**
	 * @return the content for the config.properties file
	 */
	private static String getDatabaseProperties(String pass,String db) {
			return 	"\n" 
				+ "password=" + pass + "\n"
				+ "driver=com.mysql.jdbc.Driver\n"
				+ "host=127.0.0.1\n"
				+ "port=3307\n"
				+ "database="+db+"\n"
				+ "user=root\n";
	}

	/**
	 * @return the content for the config.properties file
	 */
	private static String getLoggingProperties() {
		return "\n"
				+ "handlers= java.util.logging.FileHandler\n"
				+ "java.util.logging.FileHandler.formatter = java.util.logging.SimpleFormatter\n"
				+ "java.util.logging.FileHandler.limit = 1000000\n"
				+ "java.util.logging.FileHandler.count = 4\n"
				+ "java.util.logging.FileHandler.append = true\n"
				+ "java.util.logging.FileHandler.pattern = alien%g.log\n"
				+ ".level = OFF\n" + "alien.level = OFF\n"
				+ "lia.level = OFF\n" + "lazyj.level = OFF\n"
				+ "apmon.level = OFF\n"
				+ "# tell LazyJ to use the same logging facilities\n"
				+ "use_java_logger=true\n";
	}

}
