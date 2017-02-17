package alien.site;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.JBoxServer;
import alien.config.ConfigUtils;
import alien.log.LogUtils;
import alien.user.LDAPHelper;

public class ComputingElement extends Thread {

	// Logger object
	static transient Logger logger = ConfigUtils.getLogger(ComputingElement.class.getCanonicalName());

	// Config, env, classad
	// private final ExtProperties config = ConfigUtils.getConfig();
	private Integer port = 10000;
	private String site;
	private HashMap<String, Object> siteMap = new HashMap<>();
	private HashMap<String, Object> ceConfig = null;
	private HashMap<String, Object> hostConfig = null;
	private HashMap<String, Object> siteConfig = null;
	private HashMap<String, String> ce_environment = null;

	public ComputingElement() {
		try {
			// JAKeyStore.loadClientKeyStorage();
			// JAKeyStore.loadServerKeyStorage();

			getCEconfigFromLDAP();

//			System.out.println("SiteConfig");
//			for (String key : siteConfig.keySet())
//				System.err.println(key + " - " + siteConfig.get(key));
//
//			System.out.println("HostConfig");
//			for (String key : hostConfig.keySet())
//				System.err.println(key + " - " + hostConfig.get(key));
//
//			System.out.println("CEConfig");
//			for (String key : ceConfig.keySet())
//				System.err.println(key + " - " + ceConfig.get(key));

			getSiteMap();

//			System.out.println(ce_environment);

			logger = LogUtils.redirectToCustomHandler(logger, hostConfig.get("logdir") + "CE");

		} catch (final Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		// DispatchSSLServer.overWriteServiceAndForward("siteProxyService");
		// try {
		// DispatchSSLServer.runService();
		// } catch (final IOException e1) {
		// e1.printStackTrace();
		// }
		logger.log(Level.INFO, "Starting ComputingElement in " + siteMap.get("host"));
		try {
//			 System.out.println("Trying to start JBox"); // TODO uncomment
//			 JBoxServer.startJBoxService(0); // TODO uncomment
//			 port = JBoxServer.getPort(); // TODO uncomment
		} catch (final Exception e) {
			System.err.println("Unable to start JBox.");
			e.printStackTrace();
		}

		// while (true) {
		// here we would have to poll the queue info and submit jobAgents....

		// }

		System.out.println("Exiting CE");
	}

	// Queries LDAP to get all the config values (site,host,CE)
	public void getCEconfigFromLDAP() {
		// Get hostname and domain
		String hostName = "";
		String domain = "";
		try {
			hostName = InetAddress.getLocalHost().getCanonicalHostName();
			hostName = hostName.replace("/.$/", "");
			domain = hostName.substring(hostName.indexOf(".") + 1, hostName.length());
		} catch (final UnknownHostException e) {
			logger.severe("Error: couldn't get hostname");
			e.printStackTrace();
		}

		// We get the site name from the domain and the site root info
		Set<String> siteset = LDAPHelper.checkLdapInformation("(&(domain=" + domain + "))", "ou=Sites,", "accountName");

		if (siteset == null || siteset.size() == 0 || siteset.size() > 1) {
			logger.severe("Error: " + siteset.size() + " sites found for domain: " + domain);
			System.exit(-1);
		}
		site = siteset.iterator().next();

		// Get the root site config based on site name
		siteConfig = LDAPHelper.checkLdapTree("(&(ou=" + site + ")(objectClass=AliEnSite))", "ou=Sites,");

		if (siteConfig == null || siteConfig.size() == 0) {
			logger.severe("Error: cannot find site root configuration in LDAP for site: " + site);
			System.exit(-1);
		}

		// Get the hostConfig from LDAP based on the site and hostname
		hostConfig = LDAPHelper.checkLdapTree("(&(host=" + hostName + "))", "ou=Config,ou=" + site + ",ou=Sites,");

		if (hostConfig == null || hostConfig.size() == 0) {
			logger.severe("Error: cannot find host configuration in LDAP for host: " + hostName);
			System.exit(-1);
		}

		if (!hostConfig.containsKey("ce")) {
			logger.severe("Error: cannot find ce configuration in hostConfig for host: " + hostName);
			System.exit(-1);
		}

		// Get the CE information based on the site and ce name for the host
		ceConfig = LDAPHelper.checkLdapTree("(&(name=" + hostConfig.get("ce") + "))", "ou=CE,ou=Services,ou=" + site + ",ou=Sites,");

		if (ceConfig == null || ceConfig.size() == 0) {
			logger.severe("Error: cannot find ce configuration in LDAP for CE: " + hostConfig.get("ce"));
			System.exit(-1);
		}

	}

	// Prepares a hash to create the sitemap
	public void getSiteMap() {
		HashMap<String, String> smenv = new HashMap<>();

		smenv.put("ALIEN_CM_AS_LDAP_PROXY", hostConfig.get("host") + ":" + port.toString());

		smenv.put("site", siteConfig.get("accountname").toString());

		smenv.put("CE", hostConfig.get("ce").toString());

		smenv.put("TTL", "86400"); // Will be recalculated in the loop depending on proxy lifetime

		smenv.put("Disk", "100000000");

		if (ceConfig.containsKey("cerequirements"))
			smenv.put("cerequirements", ceConfig.get("cerequirements").toString());

		if (ceConfig.containsKey("partition"))
			smenv.put("partition", ceConfig.get("partition").toString());

		if (hostConfig.containsKey("closese"))
			smenv.put("closeSE", hostConfig.get("closese").toString());
		else
			if (siteConfig.containsKey("closese"))
				smenv.put("closeSE", siteConfig.get("closese").toString());

		if (ceConfig.containsKey("environment")) {
			ce_environment = new HashMap<>();
			if (ceConfig.get("environment") instanceof TreeSet) {
				TreeSet<String> ce_env_set = (TreeSet<String>) ceConfig.get("environment");
				for (String env_entry : ce_env_set) {
					String[] ce_env_str = env_entry.split("=");
					ce_environment.put(ce_env_str[0], ce_env_str[1]);
				}
			}
			else {
				String[] ce_env_str = ((String) ceConfig.get("environment")).split("=");
				ce_environment.put(ce_env_str[0], ce_env_str[1]);
			}
		}

		siteMap = (new SiteMap()).getSiteParameters(smenv);
	}

}
