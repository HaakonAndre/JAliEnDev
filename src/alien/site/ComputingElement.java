package alien.site;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.taskQueue.GetNumberFreeSlots;
import alien.config.ConfigUtils;
import alien.log.LogUtils;
import alien.monitoring.MonitorFactory;
import alien.shell.commands.JAliEnCOMMander;
import alien.site.batchqueue.BatchQueue;
import alien.user.LDAPHelper;
import apmon.ApMon;
import apmon.ApMonException;

/**
 * @author mmmartin
 *
 */
public class ComputingElement extends Thread {

	/**
	 * ApMon sender
	 */
	static transient final ApMon apmon = MonitorFactory.getApMonSender();

	// Logger object
	static transient Logger logger = ConfigUtils.getLogger(ComputingElement.class.getCanonicalName());

	private final JAliEnCOMMander commander = JAliEnCOMMander.getInstance();

	// Config, env, classad
	private int port = 10000;
	private String site;
	private HashMap<String, Object> siteMap = new HashMap<>();
	private HashMap<String, Object> ceConfig = null;
	private HashMap<String, Object> hostConfig = null;
	private HashMap<String, Object> siteConfig = null;
	private HashMap<String, String> host_environment = null;
	private HashMap<String, String> ce_environment = null;
	private BatchQueue queue = null;

	/**
	 * 
	 */
	public ComputingElement() {
		try {
			// JAKeyStore.loadClientKeyStorage();
			// JAKeyStore.loadServerKeyStorage();

			getCEconfigFromLDAP();

			// System.out.println("SiteConfig");
			// for (String key : siteConfig.keySet())
			// System.err.println(key + " - " + siteConfig.get(key));
			//
			// System.out.println("HostConfig");
			// for (String key : hostConfig.keySet())
			// System.err.println(key + " - " + hostConfig.get(key));
			//
			// System.out.println("CEConfig");
			// for (String key : ceConfig.keySet())
			// System.err.println(key + " - " + ceConfig.get(key));

			getSiteMap();

			// System.out.println(ce_environment);

			logger = LogUtils.redirectToCustomHandler(logger, hostConfig.get("logdir") + "/CE");

			queue = getBatchQueue((String) ceConfig.get("type"));

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
			// System.out.println("Trying to start JBox"); // TODO uncomment
			// JBoxServer.startJBoxService(0); // TODO uncomment
			// port = JBoxServer.getPort(); // TODO uncomment
		} catch (final Exception e) {
			System.err.println("Unable to start JBox.");
			e.printStackTrace();
		}

		for (int i = 0; i < 5; i++) { // TODO replace for while(true)
			boolean should_submit = true;

			// Get free slots
			int free_slots = getNumberFreeSlots();

			if (should_submit && free_slots > 0)
				offerAgent();
		}

		System.out.println("Exiting CE");
	}

	private int getNumberFreeSlots() {
		// First we get the maxJobs and maxQueued from the Central Services
		final GetNumberFreeSlots jobSlots = commander.q_api.getNumberFreeSlots((String) hostConfig.get("host"), port, (String) hostConfig.get("ce"),
				ConfigUtils.getConfig().gets("version", "J-1.0").trim());

		List<Integer> slots = jobSlots.getJobSlots();
		int max_jobs = 0;
		int max_queued = 0;

		if (slots != null) {
			if (slots.get(0).intValue() == 0 && slots.size() >= 3) { // OK
				max_jobs = slots.get(1).intValue();
				max_queued = slots.get(2).intValue();
			}
			else { // Error
				switch (slots.get(0).intValue()) {
				case 1:
					logger.info("Failed getting or inserting host in getNumberFreeSlots");
					break;
				case 2:
					logger.info("Failed updating host in getNumberFreeSlots");
					break;
				case 3:
					logger.info("Failed getting slots in getNumberFreeSlots");
					break;
				case -2:
					logger.info("The queue is centrally locked!");
					break;
				default:
					logger.info("Unknown error in getNumberFreeSlots");
				}
				return 0;
			}
		}

		// Now we get the values from the batch interface and calculate the slots available
		int queued = queue.getNumberQueued();
		if (queued < 0) {
			logger.info("There was a problem getting the number of queued agents!");
			return 0;
		}
		logger.info("Agents queued: " + queued);

		int active = queue.getNumberActive();
		if (active < 0) {
			logger.info("There was a problem getting the number of active agents!");
			return 0;
		}
		logger.info("Agents active: " + active);

		int free = max_queued - queued;
		if ((max_jobs - active) < free)
			free = max_jobs - active;

		// Report slot status to ML
		final Vector<String> paramNames = new Vector<>();
		final Vector<Object> paramValues = new Vector<>();
		paramNames.add("jobAgents_queued");
		paramNames.add("jobAgents_running");
		paramNames.add("jobAgents_slots");
		paramValues.add(Integer.valueOf(queued));
		paramValues.add(Integer.valueOf(active - queued));
		paramValues.add(Integer.valueOf(free < 0 ? 0 : free));

		try {
			apmon.sendParameters(site + "_CE_" + siteMap.get("CE"), hostConfig.get("host").toString(), paramNames.size(), paramNames, paramValues);
		} catch (ApMonException | IOException e) {
			logger.severe("Can't send parameter to ML (getNumberFreeSlots): " + e);
		}

		return free;
	}

	private void offerAgent() {
		queue.submit(); // TODO delete

		return;
	}

	// Queries LDAP to get all the config values (site,host,CE)
	void getCEconfigFromLDAP() {
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
			logger.severe("Error: " + (siteset == null ? "null" : String.valueOf(siteset.size())) + " sites found for domain: " + domain);
			System.exit(-1);
			return;
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
	void getSiteMap() {
		HashMap<String, String> smenv = new HashMap<>();

		smenv.put("ALIEN_CM_AS_LDAP_PROXY", hostConfig.get("host") + ":" + port);

		smenv.put("site", siteConfig.get("accountname").toString());

		smenv.put("CE", "ALICE::" + siteConfig.get("accountname") + "::" + hostConfig.get("ce"));

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

		if (hostConfig.containsKey("environment")) {
			host_environment = new HashMap<>();
			if (hostConfig.get("environment") instanceof TreeSet) {
				@SuppressWarnings("unchecked")
				Set<String> host_env_set = (Set<String>) hostConfig.get("environment");
				for (String env_entry : host_env_set) {
					String[] host_env_str = env_entry.split("=");
					host_environment.put(host_env_str[0], host_env_str[1]);
				}
			}
			else {
				String[] host_env_str = ((String) hostConfig.get("environment")).split("=");
				host_environment.put(host_env_str[0], host_env_str[1]);
			}
		}

		if (ceConfig.containsKey("environment")) {
			ce_environment = new HashMap<>();
			if (ceConfig.get("environment") instanceof TreeSet) {
				@SuppressWarnings("unchecked")
				Set<String> ce_env_set = (Set<String>) ceConfig.get("environment");
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

		if (host_environment != null)
			smenv.putAll(host_environment);
		if (ce_environment != null)
			smenv.putAll(ce_environment);

		siteMap = (new SiteMap()).getSiteParameters(smenv);
	}

	BatchQueue getBatchQueue(String type) {
		Class<?> cl = null;
		try {
			cl = Class.forName("alien.site.batchqueue." + type);
		} catch (ClassNotFoundException e) {
			logger.severe("Cannot find class for type: " + type + "\n" + e);
			return null;
		}

		Constructor<?> con = null;
		try {
			con = cl.getConstructor(ceConfig.getClass(), logger.getClass());
		} catch (NoSuchMethodException | SecurityException e) {
			logger.severe("Cannot find class for ceConfig: " + e);
			return null;
		}

		try {
			queue = (BatchQueue) con.newInstance(ceConfig, logger);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			logger.severe("Cannot instantiate queue class for type: " + type + "\n" + e);
		}

		return queue;
	}

}
