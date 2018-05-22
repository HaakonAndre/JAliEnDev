package alien.site;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.Dispatcher;
import alien.api.JBoxServer;
import alien.api.aaa.GetTokenCertificate;
import alien.api.aaa.TokenCertificateType;
import alien.api.taskQueue.GetNumberFreeSlots;
import alien.api.taskQueue.GetNumberWaitingJobs;
import alien.config.ConfigUtils;
import alien.log.LogUtils;
import alien.monitoring.MonitorFactory;
import alien.shell.commands.JAliEnCOMMander;
import alien.site.batchqueue.BatchQueue;
import alien.test.utils.Functions;
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
	private HashMap<String, Object> config = null;
	private HashMap<String, String> host_environment = null;
	private HashMap<String, String> ce_environment = null;
	private BatchQueue queue = null;

	/**
	 *
	 */
	public ComputingElement() {
		try {
			// try {
			// if (!JAKeyStore.loadKeyStore()) {
			// logger.severe("Grid Certificate could not be loaded.");
			// System.err.println("Grid Certificate could not be loaded.");
			// System.err.println("Exiting...");
			// return;
			// }
			// } catch (final Exception e) {
			// logger.log(Level.SEVERE, "Error loading the key", e);
			// System.err.println("Error loading the key");
			// }

			config = ConfigUtils.getConfigFromLdap();
			site = (String) config.get("site_accountname");
			getSiteMap();

			final String host_logdir_resolved = Functions.resolvePathWithEnv((String) config.get("host_logdir"));

			logger = LogUtils.redirectToCustomHandler(logger, host_logdir_resolved + "/CE");

			queue = getBatchQueue((String) config.get("ce_type"));

		} catch (final Exception e) {
			logger.severe("Problem in construction of ComputingElement: " + e.toString());
		}
	}

	@Override
	public void run() {
		logger.log(Level.INFO, "Starting ComputingElement in " + config.get("host_host"));
		try {
			logger.info("Trying to start JBox");
			JBoxServer.startJBoxService(0);
			port = JBoxServer.getPort();
		} catch (final Exception e) {
			logger.severe("Unable to start JBox: " + e.toString());
		}

		logger.info("Looping");
		while (true) {
			offerAgent();

			try {
				Thread.sleep(System.getenv("ce_loop_time") != null ? Long.parseLong(System.getenv("ce_loop_time")) : 60000);
			} catch (InterruptedException e) {
				logger.severe("Unable to sleep: " + e.toString());
			}

		}

	}

	/**
	 * @param args
	 */
	public static void main(final String[] args) {
		final ComputingElement CE = new ComputingElement();
		CE.run();
	}

	private int getNumberFreeSlots() {
		// First we get the maxJobs and maxQueued from the Central Services
		final GetNumberFreeSlots jobSlots = commander.q_api.getNumberFreeSlots((String) config.get("host_host"), port, siteMap.get("CE").toString(),
				ConfigUtils.getConfig().gets("version", "J-1.0").trim());

		final List<Integer> slots = jobSlots.getJobSlots();
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
		final int queued = queue.getNumberQueued();
		if (queued < 0) {
			logger.info("There was a problem getting the number of queued agents!");
			return 0;
		}
		logger.info("Agents queued: " + queued);

		final int active = queue.getNumberActive();
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
		final int waiting_jobs = jobMatch.getNumberJobsWaitingForSite().intValue();

		if (waiting_jobs <= 0) {
			logger.info("Broker returned 0 available waiting jobs");
			return;
		}
		logger.info("Waiting jobs: " + waiting_jobs);

		if (waiting_jobs < slots_to_submit)
			slots_to_submit = waiting_jobs;

		logger.info("Going to submit " + slots_to_submit + " agents");

		final String script = createAgentStartup();
		if (script == null) {
			logger.info("Cannot create startup script");
			return;
		}
		logger.info("Created AgentStartup script: " + script);

		while (slots_to_submit > 0) {
			queue.submit(script);
			slots_to_submit--;
		}

		logger.info("Submitted " + slots_to_submit);

		return;
	}

	/*
	 * Creates script to execute on worker nodes
	 */
	private String createAgentStartup() {
		String startup_script = System.getenv("HOME") + "/jalien/jalien ";
		if (System.getenv("JALIEN_ROOT") != null) {
			startup_script = System.getenv("JALIEN_ROOT") + "/jalien ";
		}
		String before = "";
		String after = "";

		final long time = new Timestamp(System.currentTimeMillis()).getTime();
		String host_tempdir = (String) config.get("host_tmpdir");
		final String host_tempdir_resolved = Functions.resolvePathWithEnv(host_tempdir);
		if (this.queue.getClass() == alien.site.batchqueue.FORK.class)
			host_tempdir = host_tempdir_resolved;
		final String cert_file = host_tempdir + "/token_cert." + time;
		final String key_file = host_tempdir + "/token_key." + time;

		int ttl_hours = ((Integer) siteMap.get("TTL")).intValue();
		ttl_hours = ttl_hours / 3600;

		final String[] token_certificate_full = getTokenCertificate(ttl_hours / 24 + 1);
		final String token_cert_str = token_certificate_full[0];
		final String token_key_str = token_certificate_full[1];

		before += "echo 'Using token certificate'\n" + "mkdir -p " + host_tempdir + "\n" + "file=" + cert_file + "\n" + "cat >" + cert_file + " <<EOF\n" + token_cert_str + "EOF\n" + "chmod 0400 "
				+ cert_file + "\n" + "export JALIEN_TOKEN_CERT=" + cert_file + ";\n" + "echo USING JALIEN_TOKEN_CERT\n" + "file=" + key_file + "\n" + "cat >" + key_file + " <<EOF\n" + token_key_str
				+ "EOF\n" + "chmod 0400 " + key_file + "\n" + "export JALIEN_TOKEN_KEY=" + key_file + ";\n" + "echo USING JALIEN_TOKEN_KEY\n";

		after += "rm -rf " + cert_file + "\n";
		after += "rm -rf " + key_file + "\n";
		after += "rm -rf jobagent.jar\n";

		if (config.containsKey("ce_installmethod") && config.get("ce_installmethod").equals("CVMFS"))
			startup_script = runFromCVMFS();
		else
			startup_script = runFromDefault();

		final String content_str = before + startup_script + after;

		final String agent_startup_path = host_tempdir_resolved + "/agent.startup." + time;
		final File agent_startup_file = new File(agent_startup_path);
		try {
			agent_startup_file.createNewFile();
			agent_startup_file.setExecutable(true);
		} catch (final IOException e1) {
			logger.info("Error creating Agent Sturtup file: " + e1.toString());
			return null;
		}

		try (PrintWriter writer = new PrintWriter(agent_startup_path, "UTF-8")) {
			writer.println("#!/bin/bash");
			writer.println(content_str);
		} catch (final FileNotFoundException e) {
			logger.info("Agent Sturtup file not found: " + e.toString());
		} catch (final UnsupportedEncodingException e) {
			logger.info("Encoding error while writing Agent Sturtup file: " + e.toString());
		}

		return agent_startup_path;
	}

	private String[] getTokenCertificate(final int ttl_days) {
		GetTokenCertificate gtc = new GetTokenCertificate(commander.getUser(), commander.getUsername(), TokenCertificateType.JOB_AGENT_TOKEN, null, ttl_days);

		try {
			gtc = Dispatcher.execute(gtc);
			final String[] token_cert_and_key = new String[2];
			token_cert_and_key[0] = gtc.getCertificateAsString();
			token_cert_and_key[1] = gtc.getPrivateKeyAsString();
			return token_cert_and_key;
		} catch (Exception e) {
			logger.info("Getting JobAgent TokenCertificate failed: " + e);
		}

		return null;
	}

	private static String runFromCVMFS() {
		logger.info("The worker node will install with the CVMFS method");
		// String alien_version = System.getenv("ALIEN_VERSION");
		// String cvmfs_path = "/cvmfs/alice.cern.ch/bin";
		//
		// alien_version = (alien_version != null ? alien_version = "--alien-version " + alien_version : ""); // TODO: uncomment
		//
		// if (ce_environment.containsKey("CVMFS_PATH"))
		// cvmfs_path = ce_environment.get("CVMFS_PATH");
		//
		// return cvmfs_path + "/alienv " + alien_version + " -jalien jalien";

		// return System.getenv("HOME") + "/jalien/jalien"; //TODO: local version
		return "curl -o alien.jar \"https://alien.cern.ch/jalien/alien.jar\"\n" + "echo [DEBUG] Downloaded the jar package\n"
				+ "/cvmfs/alice.cern.ch/x86_64-2.6-gnu-4.1.2/Packages/AliEn/v2-19-395/java/MonaLisa/java/bin/java -jar alien.jar\n";
	}

	private static String runFromDefault() {
		logger.info("The worker node will install with the no method");
		return "java -jar alien.jar\n";
	}

	// Prepares a hash to create the sitemap
	void getSiteMap() {
		final HashMap<String, String> smenv = new HashMap<>();

		smenv.put("ALIEN_CM_AS_LDAP_PROXY", config.get("host_host") + ":" + port);
		config.put("ALIEN_CM_AS_LDAP_PROXY", config.get("host_host") + ":" + port);

		smenv.put("site", site);

		smenv.put("CE", "ALICE::" + site + "::" + config.get("host_ce"));

		// TTL will be recalculated in the loop depending on proxy lifetime
		if (config.containsKey("ce_ttl"))
			smenv.put("TTL", (String) config.get("ce_ttl"));
		else
			smenv.put("TTL", "86400");

		smenv.put("Disk", "100000000"); // TODO: df

		if (config.containsKey("ce_cerequirements"))
			smenv.put("cerequirements", config.get("ce_cerequirements").toString());

		if (config.containsKey("ce_partition"))
			smenv.put("partition", config.get("ce_partition").toString());

		if (config.containsKey("host_closese"))
			smenv.put("closeSE", config.get("host_closese").toString());
		else
			if (config.containsKey("site_closese"))
				smenv.put("closeSE", config.get("site_closese").toString());

		if (config.containsKey("host_environment"))
			host_environment = getValuesFromLDAPField(config.get("host_environment"));

		// environment field can have n values
		if (config.containsKey("ce_environment"))
			ce_environment = getValuesFromLDAPField(config.get("ce_environment"));

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
	public static HashMap<String, String> getValuesFromLDAPField(final Object field) {
		final HashMap<String, String> map = new HashMap<>();
		if (field instanceof TreeSet) {
			@SuppressWarnings("unchecked")
			final Set<String> host_env_set = (Set<String>) field;
			for (final String env_entry : host_env_set) {
				final String[] host_env_str = env_entry.split("=");
				map.put(host_env_str[0], host_env_str[1]);
			}
		}
		else {
			final String[] host_env_str = ((String) field).split("=");
			map.put(host_env_str[0], host_env_str[1]);
		}
		return map;
	}

	/*
	 * Get queue class with reflection
	 */
	BatchQueue getBatchQueue(final String type) {
		Class<?> cl = null;
		try {
			cl = Class.forName("alien.site.batchqueue." + type);
		} catch (final ClassNotFoundException e) {
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

		// prepare some needed fields for the queue
		if (!config.containsKey("host_host") || (config.get("host_host") == null)) {
			InetAddress ip;
			String hostname = "";
			try {
				ip = InetAddress.getLocalHost();
				hostname = ip.getCanonicalHostName();
			} catch (final UnknownHostException e) {
				logger.log(Level.WARNING, "Problem identifying local host", e);
			}
			config.put("host_host", hostname);
		}
		if (!config.containsKey("host_port") || (config.get("host_port") == null))
			config.put("host_port", Integer.valueOf(this.port));

		try {
			queue = (BatchQueue) con.newInstance(config, logger);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			logger.severe("Cannot instantiate queue class for type: " + type + "\n" + e);
		}

		return queue;
	}

}
