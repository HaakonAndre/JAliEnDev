package alien.site;

import java.io.ByteArrayOutputStream;
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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import alien.api.JBoxServer;
import alien.api.taskQueue.GetNumberFreeSlots;
import alien.api.taskQueue.GetNumberWaitingJobs;
import alien.config.ConfigUtils;
import alien.log.LogUtils;
import alien.monitoring.MonitorFactory;
import alien.shell.commands.JAliEnCOMMander;
import alien.shell.commands.JSONPrintWriter;
import alien.shell.commands.UIPrintWriter;
import alien.site.batchqueue.BatchQueue;
import alien.test.utils.Functions;
import alien.user.JAKeyStore;
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
			try {
				if (!JAKeyStore.loadKeyStore()) {
					System.err.println("Grid Certificate could not be loaded.");
					System.err.println("Exiting...");
					return;
				}
			} catch (final Exception e) {
				logger.log(Level.SEVERE, "Error loading the key", e);
				System.err.println("Error loading the key");
			}

			// JAKeyStore.loadClientKeyStorage();
			// JAKeyStore.loadServerKeyStorage();
			config = ConfigUtils.getConfigFromLdap();
			site = (String) config.get("site_accountname");
			getSiteMap();

			final String host_logdir_resolved = Functions.resolvePathWithEnv((String) config.get("host_logdir"));

			logger = LogUtils.redirectToCustomHandler(logger, host_logdir_resolved + "/CE");

			queue = getBatchQueue((String) config.get("ce_type"));

		} catch (final Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		logger.log(Level.INFO, "Starting ComputingElement in " + config.get("host_host"));
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
			// System.exit(0); // TODO delete
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
		logger.info("Created AgentStartup script: " + script);
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
		String startup_script = System.getenv("JALIEN_ROOT") + "/jalien ";
		if (System.getenv("JALIEN_ROOT") == null) { // We don't have the env variable set
			logger.warning("Environment variable JALIEN_ROOT not set. Trying default location.");
			startup_script = System.getenv("HOME") + "/jalien/jalien ";
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

		// String proxyfile = System.getenv("X509_TOKEN_CERT");
		// if (proxyfile != null && proxyfile.length() > 0) {
		// // WLCG site: get timeleft
		// }
		// else {
		// proxyfile = "/tmp/x509_" + SystemCommand.bash("id -u").stdout;
		// JAliEn site: renew proxy (hours), get new timeleft

		// String file_content = "";
		// try (BufferedReader br = new BufferedReader(new FileReader(proxyfile))) {
		// String line;
		// while ((line = br.readLine()) != null) {
		// file_content += line;
		// }
		// } catch (IOException e) {
		// logger.info("Error reading the proxy file: " + e);
		// }
		final String[] token_certificate_full = getTokenCertificate(ttl_hours / 24);
		final String token_cert_str = token_certificate_full[0];
		final String token_key_str = token_certificate_full[1];

		before += "echo 'Using token certificate'\n" + "mkdir -p " + host_tempdir + "\n" + "file=" + cert_file + "\n" + // TODO: check and remove this statement if indeed not needed
				"cat >" + cert_file + " <<EOF\n" + token_cert_str + "EOF\n" + "chmod 0400 " + cert_file + "\n" + "export JALIEN_TOKEN_CERT=" + cert_file + ";\n" + "echo USING JALIEN_TOKEN_CERT\n"
				+ "file=" + key_file + "\n" + // TODO: check and remove this statement if indeed not needed
				"cat >" + key_file + " <<EOF\n" + token_key_str + "EOF\n" + "chmod 0400 " + key_file + "\n" + "export JALIEN_TOKEN_KEY=" + key_file + ";\n" + "echo USING JALIEN_TOKEN_KEY\n";

		// + startup_script + " proxy-info\n";
		after += "rm -rf " + cert_file + "\n";
		after += "rm -rf " + key_file + "\n";
		after += "rm -rf jobagent.jar\n";
		// }

		// Check proxy timeleft is good

		if (config.get("ce_installmethod").equals("CVMFS"))
			startup_script = runFromCVMFS();

		final String content_str = before + startup_script + after;

		final String agent_startup_path = host_tempdir_resolved + "/agent.startup." + time;
		final File agent_startup_file = new File(agent_startup_path);
		try {
			agent_startup_file.createNewFile();
			agent_startup_file.setExecutable(true);
		} catch (final IOException e1) {
			logger.info("Error creating Agent Sturtup file");
			e1.printStackTrace();
		}

		try (PrintWriter writer = new PrintWriter(agent_startup_path, "UTF-8")) {
			writer.println("#!/bin/bash");
			writer.println(content_str);
		} catch (final FileNotFoundException e) {
			logger.info("Agent Sturtup file not found");
			e.printStackTrace();
		} catch (final UnsupportedEncodingException e) {
			logger.info("Encoding error while writing Agent Sturtup file");
			e.printStackTrace();
		}

		startup_script = agent_startup_path; // not sure why we do this. copied from perl

		return startup_script;
	}

	private String[] getTokenCertificate(final int ttl_days) {
		final String[] token_cmd = new String[5];
		token_cmd[0] = "token";
		token_cmd[1] = "-t";
		token_cmd[2] = "jobagent";
		token_cmd[3] = "-v";
		token_cmd[4] = String.format("%d", Integer.valueOf(ttl_days));
		try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
			final UIPrintWriter out = new JSONPrintWriter(os);

			if (!commander.isAlive())
				commander.start();

			// Send the command to executor and send the result back to
			// client via OutputStream
			synchronized (commander) {
				commander.status.set(1);
				commander.setLine(out, token_cmd);
				commander.notifyAll();
			}
			waitCommandFinish();

			// Now parse the reply from JCentral
			final JSONParser jsonParser = new JSONParser();
			JSONObject readf = null;
			try {
				readf = (JSONObject) jsonParser.parse(os.toString());

				final JSONArray jsonArray = (JSONArray) readf.get("results");
				for (final Object object : jsonArray) {
					final String[] token_cert_and_key = new String[2];

					final JSONObject aJson = (JSONObject) object;

					token_cert_and_key[0] = (String) aJson.get("tokencert");
					token_cert_and_key[1] = (String) aJson.get("tokenkey");

					return token_cert_and_key;
				}
			} catch (final ParseException e) {
				logger.warning("Error parsing json.");
				e.printStackTrace();
			}
		} catch (@SuppressWarnings("unused") final IOException e1) {
			// cannot throw IOException on an inmemory operation
		}

		return null;
	}

	private void waitCommandFinish() {
		// wait for the previous command to finish
		if (commander == null)
			return;
		while (commander.status.get() == 1)
			try {
				synchronized (commander.status) {
					commander.status.wait(1000);
				}
			} catch (@SuppressWarnings("unused") final InterruptedException ie) {
				// ignore
			}
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
		return "curl -o jobagent.jar \"https://doc-14-4k-docs.googleusercontent.com/docs/securesc/ha0ro937gcuc7l7deffksulhg5h7mbp1/qnsc320nlbfmktguqgu83qckicc984g1/1510927200000/03129700828163697278/*/1YriohTbeoaSLx8ppglq322XqCdCA2HKd?e=download\"\n"
				+ "echo [DEBUG] Downloaded the jar package\n" + "/cvmfs/alice.cern.ch/x86_64-2.6-gnu-4.1.2/Packages/AliEn/v2-19-395/java/MonaLisa/java/bin/java -jar jobagent.jar\n";
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
