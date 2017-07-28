package alien.site;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.JBoxServer;
import alien.api.taskQueue.GetNumberFreeSlots;
import alien.api.taskQueue.GetNumberWaitingJobs;
import alien.config.ConfigUtils;
import alien.log.LogUtils;
import alien.monitoring.MonitorFactory;
import alien.shell.commands.JAliEnCOMMander;
import alien.site.batchqueue.BatchQueue;
import apmon.ApMon;
import apmon.ApMonException;
import lazyj.commands.SystemCommand;

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
	private HashMap<String, Object> config = null;
	// private HashMap<String, Object> ceConfig = null;
	// private HashMap<String, Object> hostConfig = null;
	// private HashMap<String, Object> siteConfig = null;
	// private HashMap<String, Object> VOConfig = null;
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
			config = ConfigUtils.getConfigFromLdap();
			getSiteMap();

			logger = LogUtils.redirectToCustomHandler(logger, config.get("host_logdir") + "/CE");

			queue = getBatchQueue((String) config.get("ce_type"));

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
			System.out.println("Trying to start JBox");
			JBoxServer.startJBoxService(0);
			port = JBoxServer.getPort();
		} catch (final Exception e) {
			System.err.println("Unable to start JBox.");
			e.printStackTrace();
		}

		System.out.println("Looping");
		while (true) {
			offerAgent();

			System.out.println("Exiting CE");
			System.exit(0); // TODO delete
		}

	}

	private int getNumberFreeSlots() {
		// First we get the maxJobs and maxQueued from the Central Services
		final GetNumberFreeSlots jobSlots = commander.q_api.getNumberFreeSlots((String) config.get("host_host"), port, siteMap.get("CE").toString(),
				ConfigUtils.getConfig().gets("version", "J-1.0").trim());

		List<Integer> slots = jobSlots.getJobSlots();
		int max_jobs = 0;
		int max_queued = 0;

		if (slots == null) {
			logger.info("Cannot get values from getNumberFreeSlots");
			return 0;
		}

		if (slots.get(0).intValue() == 0 && slots.size() >= 3) { // OK
			max_jobs = slots.get(1).intValue();
			max_queued = slots.get(2).intValue();
			logger.info("Max jobs: " + max_jobs + " Max queued: " + max_queued);
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
			apmon.sendParameters(site + "_CE_" + siteMap.get("CE"), config.get("host_host").toString(), paramNames.size(), paramNames, paramValues);
		} catch (ApMonException | IOException e) {
			logger.severe("Can't send parameter to ML (getNumberFreeSlots): " + e);
		}

		return free;
	}

	private void offerAgent() {
		int slots_to_submit = getNumberFreeSlots();

		if (slots_to_submit <= 0) {
			logger.info("No slots available in the CE!");
			return;
		}
		logger.info("CE free slots: " + slots_to_submit);

		// We ask the broker how many jobs we could run
		final GetNumberWaitingJobs jobMatch = commander.q_api.getNumberWaitingForSite(siteMap);
		int waiting_jobs = jobMatch.getNumberJobsWaitingForSite().intValue();

		if (waiting_jobs <= 0) {
			logger.info("Broker returned 0 available waiting jobs");
			return;
		}
		logger.info("Waiting jobs: " + waiting_jobs);

		if (waiting_jobs < slots_to_submit)
			slots_to_submit = waiting_jobs;

		logger.info("Going to submit " + slots_to_submit + " agents");

		String script = createAgentStartup();
		if (script == null) {
			logger.info("Cannot create startup script");
			return;
		}

		while (slots_to_submit > 0) {
			queue.submit(script);
			slots_to_submit--;
		}

		return;
	}

	/*
	 * Creates script to execute on worker nodes
	 */
	private String createAgentStartup() {
		String startup = System.getenv("JALIEN_ROOT") + "jalien ";
		String before = "";
		String after = "";

		long time = new Timestamp(System.currentTimeMillis()).getTime();
		String file = config.get("host_tmpdir") + "/proxy." + time;

		int hours = ((Integer) siteMap.get("TTL")).intValue();
		hours = hours / 3600;

		String proxyfile = System.getenv("X509_USER_PROXY");
		if (proxyfile != null && proxyfile.length() > 0) {
			// WLCG site: get timeleft
		}
		else {
			proxyfile = "/tmp/x509_" + SystemCommand.bash("id -u").stdout;
			// JAliEn site: renew proxy (hours), get new timeleft

			String file_content = "";
			try (BufferedReader br = new BufferedReader(new FileReader(proxyfile))) {
				String line;
				while ((line = br.readLine()) != null) {
					file_content += line;
				}
			} catch (IOException e) {
				logger.info("Error reading the proxy file: " + e);
			}

			before += "echo 'Using the proxy'\n" + "mkdir -p " + config.get("host_tmpdir") + "\n" + "export ALIEN_USER=" + System.getenv("ALIEN_USER") + "\n" + "file=" + file + "\n" + "cat >" + file
					+ " <<EOF\n" + file_content + "\n" + "EOF\n" + "chmod 0400 " + file + "export X509_USER_PROXY=" + file + "\n" + "echo USING X509_USER_PROXY" + startup + " proxy-info";
		}

		// Check proxy timeleft is good

		// if (ceConfig.get("installmethod").equals("CVMFS")) {
		// startup = runFromCVMFS();
		// } TODO: uncomment

		// PrintWriter writer = new PrintWriter(hostConfig.get("tmpdir") + "/agent.startup." + time, "UTF-8");
		// writer.println("The first line");
		// writer.close();

		return startup;
	}

	// private String runFromCVMFS() {
	// logger.info("The worker node will install with the CVMFS method");
	// String alien_version = System.getenv("ALIEN_VERSION");
	// String cvmfs_path = "/cvmfs/alice.cern.ch/bin";
	//
	// alien_version = (alien_version != null ? alien_version = "--alien-version " + alien_version : "");
	//
	// if (ce_environment.containsKey("CVMFS_PATH"))
	// cvmfs_path = ce_environment.get("CVMFS_PATH");
	//
	// return cvmfs_path + "/alienv " + alien_version + " -jalien jalien";
	// } // TODO: uncomment

	// Prepares a hash to create the sitemap
	void getSiteMap() {
		HashMap<String, String> smenv = new HashMap<>();

		smenv.put("ALIEN_CM_AS_LDAP_PROXY", config.get("host_host") + ":" + port);
		config.put("ALIEN_CM_AS_LDAP_PROXY", config.get("host_host") + ":" + port);

		smenv.put("site", site);

		smenv.put("CE", "ALICE::" + site + "::" + config.get("host_ce"));

		// TTL will be recalculated in the loop depending on proxy lifetime
		if (config.containsKey("ce_ttl"))
			smenv.put("TTL", (String) config.get("ce_ttl"));
		else
			smenv.put("TTL", "86400");

		smenv.put("Disk", "100000000");

		if (config.containsKey("ce_cerequirements"))
			smenv.put("cerequirements", config.get("ce_cerequirements").toString());

		if (config.containsKey("ce_partition"))
			smenv.put("partition", config.get("ce_partition").toString());

		if (config.containsKey("host_closese"))
			smenv.put("closeSE", config.get("host_closese").toString());
		else
			if (config.containsKey("site_closese"))
				smenv.put("closeSE", config.get("site_closese").toString());

		if (config.containsKey("host_environment")) {
			host_environment = getValuesFromLDAPField(config.get("host_environment"));
		}

		// environment field can have n values
		if (config.containsKey("ce_environment")) {
			ce_environment = getValuesFromLDAPField(config.get("ce_environment"));
		}

		// submit,status and kill cmds can have n arguments
		if (config.containsKey("ce_submitcmd") && config.containsKey("ce_submitarg")) {
			// TODO
		}
		if (config.containsKey("ce_statuscmd") && config.containsKey("ce_statusarg")) {
			// TODO
		}
		if (config.containsKey("ce_killcmd") && config.containsKey("ce_killarg")) {
			// TODO
		}

		if (host_environment != null)
			config.putAll(host_environment);
		if (ce_environment != null)
			config.putAll(ce_environment);

		siteMap = (new SiteMap()).getSiteParameters(smenv);
	}

	/**
	 * @param field
	 * @return Map with the values of the LDAP field
	 */
	public static HashMap<String, String> getValuesFromLDAPField(Object field) {
		HashMap<String, String> map = new HashMap<>();
		if (field instanceof TreeSet) {
			@SuppressWarnings("unchecked")
			Set<String> host_env_set = (Set<String>) field;
			for (String env_entry : host_env_set) {
				String[] host_env_str = env_entry.split("=");
				map.put(host_env_str[0], host_env_str[1]);
			}
		}
		else {
			String[] host_env_str = ((String) field).split("=");
			map.put(host_env_str[0], host_env_str[1]);
		}
		return map;
	}

	/*
	 * Get queue clas with reflection
	 */
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
			con = cl.getConstructor(config.getClass(), logger.getClass());
		} catch (NoSuchMethodException | SecurityException e) {
			logger.severe("Cannot find class for ceConfig: " + e);
			return null;
		}

		try {
			queue = (BatchQueue) con.newInstance(config, logger);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			logger.severe("Cannot instantiate queue class for type: " + type + "\n" + e);
		}

		return queue;
	}

}
