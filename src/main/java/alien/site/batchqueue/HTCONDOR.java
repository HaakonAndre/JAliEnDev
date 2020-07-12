package alien.site.batchqueue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import alien.test.utils.Functions;
import lia.util.process.ExternalProcess.ExitStatus;
import utils.ProcessWithTimeout;

/**
 * @author mmmartin
 */
public class HTCONDOR extends BatchQueue {

	private Map<String, String> environment;
	private TreeSet<String> envFromConfig;
	private String submitCmd;
	private String submitArgs = "";
	private String killCmd;
	private File temp_file;

	//
	// 2020-06-24 - Maarten Litmaath, Maxim Storetvedt
	//
	// to support weighted, round-robin load-balancing over a CE set:
	//

	private ArrayList<String> ce_list = new ArrayList<>();
	private HashMap<String, Double> ce_weight = new HashMap<>();
	private int next_ce = 0;
	private HashMap<String, Integer> running = new HashMap<>();
	private HashMap<String, Integer> waiting = new HashMap<>();
	private int tot_running = 0;
	private int tot_waiting = 0;
	private long job_numbers_timestamp = 0;

	/**
	 * @param conf
	 * @param logr
	 */
	@SuppressWarnings("unchecked")
	public HTCONDOR(HashMap<String, Object> conf, Logger logr) {
		this.environment = System.getenv();
		this.config = conf;
		this.logger = logr;
		// String host_logdir = (String) config.get("host_logdir");
		// this.logger = LogUtils.redirectToCustomHandler(this.logger, (Functions.resolvePathWithEnv(host_logdir) + "/JAliEn." + (new Timestamp(System.currentTimeMillis()).getTime() + ".out")));

		this.logger.info("This VO-Box is " + config.get("ALIEN_CM_AS_LDAP_PROXY") + ", site is " + config.get("site_accountname"));

		try {
			this.envFromConfig = (TreeSet<String>) this.config.get("ce_environment");
		} catch(ClassCastException e){
			this.envFromConfig = new TreeSet<String>(Arrays.asList((String)this.config.get("ce_environment")));
		}

		this.temp_file = null;
		this.submitCmd = (config.get("CE_SUBMITCMD") != null ? (String) config.get("CE_SUBMITCMD") : "condor_submit");

		for (String env_field : envFromConfig) {
			if (Pattern.matches("^CE_LCGCE=.*", env_field)) {
				String val = env_field.split("=")[1];
				double tot = 0;

				logger.info("Load-balancing over these CEs with configured weights:");

				for (String str : val.split(",")) {
					double w = 1;
					String ce = str;
					Pattern p = Pattern.compile("(\\d+)\\s*\\*\\s*(\\S+)");
					Matcher m = p.matcher(str);

					if (m.find()) {
						w = Double.parseDouble(m.group(1));
						ce = m.group(2);
					} else {
						ce = ce.replaceAll("\\s+", "");
					}

					if (!Pattern.matches(".*:.*", ce)) {
						ce += ":9619";
					}

					logger.info(ce + " --> " + String.format("%5.3f", w));

					ce_list.add(ce);
					ce_weight.put(ce, w);
					tot += w;
				}

				if (tot <= 0) {
					logger.log(Level.WARNING, "CE_LCGCE invalid: " + val);
					// need to abort...
					ce_list.clear();
				}

				if (ce_weight.size() != ce_list.size()) {
					logger.log(Level.WARNING, "CE_LCGCE has duplicate CEs: " + val);
					// need to abort...
					tot = 0;
					ce_list.clear();
				}

				if (tot > 0) {
					logger.info("Load-balancing over these CEs with normalized weights:");

					for (String ce : ce_list) {
						double w = ce_weight.get(ce) / tot;
						ce_weight.replace(ce, w);
						logger.info(ce + " --> " + String.format("%5.3f", w));
					}
				}
			}

			if (env_field.contains("SUBMIT_ARGS")) {
				this.submitArgs = env_field.split("SUBMIT_ARGS=")[1];
			}
		}
		if (environment.get("SUBMIT_ARGS") != null) {
			this.submitArgs = environment.get("SUBMIT_ARGS");
		}

		this.killCmd = (config.get("CE_KILLCMD") != null ? (String) config.get("CE_KILLCMD") : "condor_rm");
	}

	/**
	 * @param classad
	 * @return classad or null
	 */
	public String prepareForSubmission(String classad) {
		if (classad == null) {
			return null;
		}
		String vo_str = (config.get("LCGVO") != null ? (String) config.get("LCGVO") : "alice");
		String proxy_renewal_str = String.format("/etc/init.d/%s-box-proxyrenewal", vo_str);

		File proxy_check_file = new File(environment.get("HOME") + "/no-proxy-check");
		File proxy_renewal_file = new File(proxy_renewal_str);
		if (proxy_check_file.exists() || environment.get("X509_USER_PROXY") == null || !proxy_renewal_file.exists()) {
			return classad;
		}

		int threshold = (config.get("CE_PROXYTHRESHOLD") != null ? ((Integer) config.get("CE_PROXYTHRESHOLD")).intValue() : 46 * 3600);
		this.logger.info(String.format("X509_USER_PROXY is %s", environment.get("X509_USER_PROXY")));
		this.logger.info("Checking remaining proxy lifetime");

		String proxy_info_cmd = "voms-proxy-info -acsubject -actimeleft 2>&1";
		ArrayList<String> proxy_info_output = executeCommand(proxy_info_cmd);

		String dn_str = "";
		String time_left_str = "";
		Pattern dn_pattern = Pattern.compile("^/");
		Pattern time_left_pattern = Pattern.compile("^\\d+$");
		for (String line : proxy_info_output) {
			line = line.trim();
			Matcher dn_matcher = dn_pattern.matcher(line);
			if (dn_matcher.matches()) {
				dn_str = line;
				continue;
			}
			Matcher time_left_matcher = time_left_pattern.matcher(line);
			if (time_left_matcher.matches()) {
				time_left_str = line;
				continue;
			}
		}
		if (dn_str.length() == 0) {
			this.logger.warning("[LCG] No valid proxy found.");
			return null;
		}
		this.logger.info(String.format("DN  is %s", dn_str));
		this.logger.info(String.format("Proxy timeleft is %s (threshold is %d)", time_left_str, Integer.valueOf(threshold)));
		if (Integer.parseInt(time_left_str) > threshold) {
			return classad;
		}

		// the proxy shall be managed by the proxy renewal service for the VO;
		// restart it as needed...

		String proxy_renewal_cmd = String.format("%s start 2>&1", proxy_renewal_str);
		this.logger.info("Checking proxy renewal service");
		ArrayList<String> proxy_renewal_output = null;
		try {
			proxy_renewal_output = executeCommand(proxy_renewal_cmd);
		}
		catch (Exception e) {
			this.logger.info(String.format("[HTCONDOR] Problem while executing command: %s", proxy_renewal_cmd));
			e.printStackTrace();
		}
		finally {
			if (proxy_renewal_output != null) {
				this.logger.info("Proxy renewal output:\n");
				for (String line : proxy_renewal_output) {
					line = line.trim();
					this.logger.info(line);
				}
			}
		}

		return classad;
	}

	@Override
	public void submit(final String script) {
		this.logger.info("Submit HTCONDOR");
		String cm = String.format("%s:%s", this.config.get("host_host"), this.config.get("host_port"));

		//DateFormat date_format = new SimpleDateFormat("yyyy-MM-dd");
		//String current_date_str = date_format.format(new Date());

		String host_logdir = (environment.get("HTCONDOR_LOG_PATH") != null ? environment.get("HTCONDOR_LOG_PATH") : (String) config.get("host_logdir"));
		String log_folder_path = String.format("%s/%s", host_logdir, "jobagents"); //let's just use the dir "jobagents" for now
		File log_folder = new File(log_folder_path);
		if (!(log_folder.exists()) || !(log_folder.isDirectory())) {
			try {
				log_folder.mkdir();
			}
			catch (SecurityException e) {
				this.logger.info(String.format("[HTCONDOR] Couldn't create log folder: %s", log_folder_path));
				e.printStackTrace();
			}
		}

		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		String file_base_name = String.format("%s/jobagent_%s_%d_%d", Functions.resolvePathWithEnv(log_folder_path), this.config.get("host"), this.config.get("CLUSTERMONITOR_PORT"),
				Long.valueOf(timestamp.getTime()));
		String log_cmd = String.format("log = %s.log", file_base_name);
		String out_cmd = "";
		String err_cmd = "";
		File enable_sandbox_file = new File(environment.get("HOME") + "/enable-sandbox");
		if (enable_sandbox_file.exists() || (this.logger.getLevel() != null)) {
			out_cmd = String.format("output = %s.out", file_base_name);
			err_cmd = String.format("error = %s.err", file_base_name);
		}

		// String per_hold = "periodic_hold = JobStatus == 1 && "
		// + "( GridJobStatus =?= undefined && CurrentTime - EnteredCurrentStatus > 1800 ) || "
		// + "JobStatus <= 2 && ( CurrentTime - EnteredCurrentStatus > 172800 )";
		// String per_remove = "periodic_remove = CurrentTime - QDate > 259200";
		// String osb = "+TransferOutput = \"\"";

		// ===========

		String submit_cmd = String.format("cmd = %s\n", script);
		if (log_folder.exists()) {
			submit_cmd += String.format("%s\n%s\n%s\n", out_cmd, err_cmd, log_cmd);
		}

		// --- via JobRouter or direct

		boolean use_job_agent = false;
		for (String env_field : envFromConfig) {
			if (env_field.contains("USE_JOB_ROUTER")) {
				use_job_agent = Integer.parseInt(env_field.split("=")[1]) == 1;
			}
		}
		if (environment.get("USE_JOB_ROUTER") != null) {
			use_job_agent = Integer.parseInt(environment.get("USE_JOB_ROUTER")) == 1;
		}
		String grid_resource = null;
		for (String env_field : envFromConfig) {
			if (env_field.contains("GRID_RESOURCE")) {
				grid_resource = env_field.split("=")[1];
			}
		}
		if (environment.get("GRID_RESOURCE") != null) {
			grid_resource = environment.get("GRID_RESOURCE");
		}

		if (ce_list.size() > 0) {
			logger.info("Determining the next CE to use:");

			for (int i = 0; i < ce_list.size(); i++) {
				String ce = ce_list.get(next_ce);
				int idle  = waiting.get(ce);
				double w  = ce_weight.get(ce);
				double f  = tot_waiting > 0 ? (double) idle / tot_waiting : 0;

				logger.info(String.format(
					"--> %s has idle fraction %d / %d = %5.3f vs. weight %5.3f",
					ce, idle, tot_waiting, f, w
				));

				if (f < w) {
					break;
				}

				next_ce++;
				next_ce %= ce_list.size();
			}

			String ce = ce_list.get(next_ce);

			logger.info("--> next CE to use: " + ce);

			int idle  = waiting.get(ce);

			waiting.put(ce, idle + 1);
			tot_waiting++;

			next_ce++;
			next_ce %= ce_list.size();

			String h = ce.replaceAll(":.*", "");
			grid_resource = "condor " + h + " " + ce;
			use_job_agent = false;
		}

		if (use_job_agent) {
			submit_cmd += "" + "universe = vanilla\n" + "+WantJobRouter = True\n" + "job_lease_duration = 7200\n" + "ShouldTransferFiles = YES\n";
		}
		else {
			submit_cmd += "universe = grid\n";
			if (grid_resource != null) {
				submit_cmd += String.format("grid_resource = %s\n", grid_resource);
			}
		}

		// --- further common attributes

		if (grid_resource != null) {
			// this needs to be taken from LDAP !!!
			// submit_cmd += "+WantExternalCloud = True\n";
		}
		submit_cmd += ""
				// + "$osb\n"
				// + "$per_hold\n" // TODO: inspect what this does in perl
				// + "$per_remove\n"
				+ "use_x509userproxy = true\n";

		String env_cmd = String.format("ALIEN_CM_AS_LDAP_PROXY=\'%s\'", cm);
		submit_cmd += String.format("environment = \"%s\"\n", env_cmd);

		// --- allow preceding attributes to be overridden and others added if needed

		String custom_jdl_path = String.format("%s/custom-classad.jdl", environment.get("HOME"));
		if ((new File(custom_jdl_path)).exists()) { // Check if we should add the custom attributes
			String custom_attr_str = "\n#\n# custom attributes start\n#\n\n";
			custom_attr_str += this.readJdlFile(custom_jdl_path);
			custom_attr_str += "\n#\n# custom attributes end\n#\n\n";
			submit_cmd += custom_attr_str;
		}

		// --- finally

		submit_cmd += "queue 1\n";

		// =============

		if (this.temp_file != null) {
			List<String> temp_file_lines = null;
			try {
				temp_file_lines = Files.readAllLines(Paths.get(this.temp_file.getAbsolutePath()), StandardCharsets.UTF_8);
			}
			catch (IOException e1) {
				this.logger.info("Error reading old temp file");
				e1.printStackTrace();
			}
			finally {
				if (temp_file_lines != null) {
					String temp_file_lines_str = "";
					for (String line : temp_file_lines) {
						temp_file_lines_str += line + '\n';
					}
					if (!temp_file_lines_str.equals(submit_cmd)) {
						if (!this.temp_file.delete()) {
							this.logger.info("Could not delete temp file");
						}
						try {
							this.temp_file = File.createTempFile("htc-submit.", ".jdl");
						}
						catch (IOException e) {
							this.logger.info("Error creating temp file");
							e.printStackTrace();
							return;
						}
					}
				}
			}
		}
		else {
			try {
				this.temp_file = File.createTempFile("htc-submit.", ".jdl");
			}
			catch (IOException e) {
				this.logger.info("Error creating temp file");
				e.printStackTrace();
				return;
			}
		}

		this.temp_file.setReadable(true);
		this.temp_file.setExecutable(true);

		try (PrintWriter out = new PrintWriter(this.temp_file.getAbsolutePath())) {
			out.println(submit_cmd);
			out.close();
		}
		catch (FileNotFoundException e) {
			this.logger.info("Error writing to temp file");
			e.printStackTrace();
		}

		String temp_file_cmd = String.format("%s %s %s", this.submitCmd, this.submitArgs, this.temp_file.getAbsolutePath());
		ArrayList<String> output = executeCommand(temp_file_cmd);
		for (String line : output) {
			String trimmed_line = line.trim();
			this.logger.info(trimmed_line);
		}

	}

	private String readJdlFile(String path) {
		String file_contents = "";

		String line;
		try (BufferedReader br = new BufferedReader(new FileReader(path))) {
			Pattern comment_pattern = Pattern.compile("^\\s*(#.*|//.*)?$");
			Pattern err_spaces_pattern = Pattern.compile("\\\\\\s*$");
			Pattern endl_spaces_pattern = Pattern.compile("\\s+$");
			while ((line = br.readLine()) != null) {
				Matcher comment_matcher = comment_pattern.matcher(line);
				// skip over comment lines
				if (comment_matcher.matches()) {
					continue;
				}
				// remove erroneous spaces
				line = line.replaceAll(err_spaces_pattern.pattern(), "\\\\\n");
				line = line.replaceAll(endl_spaces_pattern.pattern(), "");
				if (line.lastIndexOf('\n') == -1) {
					line += '\n';
				}
				file_contents += line;
			}

			this.logger.info(String.format("Custom attributes added from file: %s.", path));
		}
		catch (FileNotFoundException e) {
			this.logger.info(String.format("Could not find file: %s.\n", path));
			e.printStackTrace();
			return "";
		}
		catch (IOException e) {
			this.logger.info(String.format("Error while working with file: %s.\n", path));
			e.printStackTrace();
			return file_contents;
		}

		return file_contents;
	}

	private boolean getJobNumbers() {

		long now = System.currentTimeMillis();
		long dt  = now - job_numbers_timestamp;

		if (dt < 60) {
			logger.info("Reusing cached job numbers collected " + dt + " seconds ago");
			return true;
		}

		String cmd = "condor_q -const 'JobStatus < 3' -af JobStatus GridResource";
		ArrayList<String> job_list = executeCommand(cmd);

		if (job_list == null) {
			logger.info("Couldn't retrieve the list of active jobs!");
			return false;
		}

		for (String ce : ce_list) {
			running.put(ce, 0);
			waiting.put(ce, 0);
		}

		tot_running = tot_waiting = 0;
		Pattern p = Pattern.compile("^\\s*([12]).*\\s(\\S+)");

		for (String line : job_list) {
			Matcher m = p.matcher(line);

			if (m.matches()) {
				int job_status = Integer.parseInt(m.group(1));
				String ce = m.group(2);

				if (job_status == 1) {
					Integer w = waiting.get(ce);

					if (w != null) {
						waiting.put(ce, w + 1);
					}

					tot_waiting++;
				} else {
					Integer r = running.get(ce);

					if (r != null) {
						running.put(ce, r + 1);
					}

					tot_running++;
				}
			}
		}

		logger.info("Found " + tot_waiting + " idle (and " + tot_running + " running) jobs:");

		for (String ce : ce_list) {
			logger.info(String.format("%5d (%5d) for %s", waiting.get(ce), running.get(ce), ce));
		}

		job_numbers_timestamp = now;
		return true;
	}

	@Override
	public int getNumberActive() {

		if (!getJobNumbers()) {
			return -1;
		}

		return tot_running + tot_waiting;
	}

	@Override
	public int getNumberQueued() {

		if (!getJobNumbers()) {
			return -1;
		}

		return tot_waiting;
	}

	@Override
	public int kill() {
		this.logger.info("Checking proxy renewal service");
		ArrayList<String> kill_cmd_output = null;
		try {
			kill_cmd_output = executeCommand(this.killCmd);
		}
		catch (Exception e) {
			this.logger.info(String.format("[HTCONDOR] Problem while executing command: %s", this.killCmd));
			e.printStackTrace();
			return -1;
		}
		finally {
			if (kill_cmd_output != null) {
				this.logger.info("Kill cmd output:\n");
				for (String line : kill_cmd_output) {
					line = line.trim();
					this.logger.info(line);
				}
			}
		}
		if (temp_file != null && temp_file.exists()) {
			this.logger.info(String.format("Deleting temp file  %s after command.", this.temp_file.getAbsolutePath()));
			if (!temp_file.delete()) {
				this.logger.info(String.format("Could not delete temp file: %s", this.temp_file.getAbsolutePath()));
			}
			else {
				this.temp_file = null;
			}
		}
		return 0;
	}

	// Previously named "_system" in perl
	private ArrayList<String> executeCommand(String cmd) {
		ArrayList<String> proc_output = new ArrayList<>();
		try {
			ArrayList<String> cmd_full = new ArrayList<>();
			cmd_full.add("/bin/bash");
			cmd_full.add("-c");
			cmd_full.add(cmd);
			final ProcessBuilder proc_builder = new ProcessBuilder(cmd_full);

			Map<String, String> env = proc_builder.environment();
			env.clear();

			final HashMap<String, String> additional_env_vars = new HashMap<>();
			additional_env_vars.put("X509_USER_PROXY", System.getenv("X509_USER_PROXY"));
			additional_env_vars.put("LD_LIBRARY_PATH", System.getenv("LD_LIBRARY_PATH"));
			additional_env_vars.put("PATH", System.getenv("PATH"));
			env.putAll(additional_env_vars);

			proc_builder.redirectErrorStream(false);

			final Process proc = proc_builder.start();

			final ProcessWithTimeout pTimeout = new ProcessWithTimeout(proc, proc_builder);

			pTimeout.waitFor(60, TimeUnit.SECONDS);

			final ExitStatus exitStatus = pTimeout.getExitStatus();
			logger.info("Process exit status: " + exitStatus.getExecutorFinishStatus());

			if (exitStatus.getExtProcExitStatus() == 0) {
				final BufferedReader reader = new BufferedReader(new StringReader(exitStatus.getStdOut()));

				String output_str;

				while ((output_str = reader.readLine()) != null)
					proc_output.add(output_str.trim());
			}
		}
		catch (final Throwable t) {
			logger.log(Level.WARNING, "Exception executing command: " + cmd, t);
		}
		// this.logger.info("[HTCONDOR] Command output: " + proc_output);
		return proc_output;
	}

}
