package alien.site.batchqueue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Date;

import org.apache.tomcat.jni.Proc;

import alien.log.LogUtils;
import lia.util.process.ExternalProcess.ExitStatus;
import utils.ProcessWithTimeout;

/**
 * @author mmmartin
 */
public class HTCONDOR extends BatchQueue {

	private Map<String, String> _environment;
	private HashMap<String, String> _env_from_config;
	private String _submit_cmd;
	private String _submit_args;
	private String _kill_cmd;
	private File _temp_file;

	/**
	 * @param conf
	 * @param logr
	 *            this.logger
	 */
	public HTCONDOR(HashMap<String, Object> conf, Logger logr) {
		this.config = conf;
		this.logger = logr;
		this.logger = LogUtils.redirectToCustomHandler(this.logger, ((String) config.get("host_logdir")) + "JAliEn." + (new Timestamp(System.currentTimeMillis()).getTime() + ".out"));

		this.logger.info("This VO-Box is " + config.get("ALIEN_CM_AS_LDAP_PROXY") + ", site is " + config.get("site_accountName"));
		
		this._env_from_config = (HashMap<String, String>) this.config.get("ce_environment");
		this._environment = System.getenv();
		this._temp_file = null;
		this._submit_cmd = (config.get("CE_SUBMITCMD") != null ? (String)config.get("CE_SUBMITCMD") : "condor_submit");
		
		if (_env_from_config.containsKey("SUBMIT_ARGS")) {
			this._submit_args = _env_from_config.get("SUBMIT_ARGS");
		}
		if (_environment.get("SUBMIT_ARGS") != null) {
			this._submit_args = _environment.get("SUBMIT_ARGS");
		}

		this._kill_cmd = (config.get("CE_KILLCMD") != null ? (String) config.get("CE_KILLCMD") : "condor_rm");
	}
	
	/**
	 * @param classad
	 * @return classad or null
	 */
	public String prepareForSubmission(String classad) {
		if(classad == null) {
			return null;
		}
		String vo_str = (config.get("LCGVO") != null ? (String) config.get("LCGVO") : "alice");
		String proxy_renewal_str = String.format("/etc/init.d/%s-box-proxyrenewal", vo_str);
		
		File proxy_check_file= new File(_environment.get("HOME") + "/no-proxy-check"); 
		File proxy_renewal_file= new File(proxy_renewal_str); 
		if (proxy_check_file.exists() || _environment.get("X509_USER_PROXY") == null || !proxy_renewal_file.exists()) {
			return classad;
		}
		
		int threshold = (config.get("CE_PROXYTHRESHOLD") != null ? ((Integer) config.get("CE_PROXYTHRESHOLD")) : 46 * 3600);
		this.logger.info(String.format("X509_USER_PROXY is %s", _environment.get("X509_USER_PROXY")));
		this.logger.info("Checking remaining proxy lifetime");
		
		String proxy_info_cmd = "voms-proxy-info -acsubject -actimeleft 2>&1";
		ArrayList<String> proxy_info_output = executeCommand(proxy_info_cmd);
		
		String dn_str = "";
		String time_left_str = "";
		Pattern dn_pattern = Pattern.compile("^/");
		Pattern time_left_pattern = Pattern.compile("^\\d+$");
		for (String line : proxy_info_output) {
			line.trim();
			Matcher dn_matcher = dn_pattern.matcher(line);
	    	if(dn_matcher.matches()) {
	    		dn_str = line;
	    		continue;
	    	}
	    	Matcher time_left_matcher = time_left_pattern.matcher(line);
	    	if(time_left_matcher.matches()) {
	    		time_left_str = line;
	    		continue;
	    	}
		}
		if (dn_str == ""){
			this.logger.warning("[LCG] No valid proxy found.");
			return null;
		}
		this.logger.info(String.format("DN  is %s", dn_str));
		this.logger.info(String.format("Proxy timeleft is %s (threshold is %d)", time_left_str, threshold));
		if(Integer.parseInt(time_left_str) > threshold) {
			return classad;
		}
		
//		the proxy shall be managed by the proxy renewal service for the VO;
//		restart it as needed...
		
		String proxy_renewal_cmd = String.format("%s start 2>&1", proxy_renewal_str);
		this.logger.info("Checking proxy renewal service");
		ArrayList<String> proxy_renewal_output = null;
		try {
			proxy_renewal_output = executeCommand(proxy_renewal_cmd);
		} catch(Exception e){
			this.logger.info(String.format("[HTCONDOR] Prolem while executing command: %s", proxy_renewal_cmd));
			e.printStackTrace();
		}
		finally {
			if(proxy_renewal_output != null) {
				this.logger.info("Proxy renewal output:\n");
				for (String line : proxy_renewal_output) {
					line.trim();
					this.logger.info(line);
				}
			}
		}
		
		return classad;
	}

	@Override
	public void submit(final String script) {
		this.logger.info("Submit HTCONDOR");
		String cm = String.format("%s:%s", this.config.get("host"), this.config.get("CLUSTERMONITOR_PORT"));
		
		DateFormat date_format = new SimpleDateFormat("yyyy-MM-dd");
		String current_date_str = date_format.format(new Date());
		
		String host_logdir = (_environment.get("HTCONDOR_LOG_PATH") != null ? _environment.get("HTCONDOR_LOG_PATH") : (String) config.get("host_logdir"));
		String log_folder_path = String.format("%s/%s", host_logdir, current_date_str);
		File log_folder = new File(log_folder_path);
		if (!(log_folder.exists()) || !(log_folder.isDirectory())) {
			try {
				log_folder.mkdir();
			} catch (SecurityException e) {
				this.logger.info(String.format("[HTCONDOR] Couldn't create log folder: %s", log_folder_path));
				e.printStackTrace();
			}
		}
		String file_base_name = String.format("%s/jobagent_%s", log_folder_path, (String)this.config.get("ALIEN_JOBAGENT_ID"));
		String log_cmd = String.format("log = %s.log", file_base_name);
		String out_cmd = "";
		String err_cmd = "";
		File enable_sandbox_file= new File(_environment.get("HOME") + "/enable-sandbox"); 
		if (enable_sandbox_file.exists() || (this.logger.getLevel() != null)) {
			out_cmd = String.format("output = %s.out", file_base_name);
			err_cmd = String.format("error = %s.err", file_base_name);
		}
		
//		String per_hold = "periodic_hold = JobStatus == 1 && "
//				+ "( GridJobStatus =?= undefined && CurrentTime - EnteredCurrentStatus > 1800 ) || "
//				+ "JobStatus <= 2 && ( CurrentTime - EnteredCurrentStatus > 172800 )";
//		String per_remove = "periodic_remove = CurrentTime - QDate > 259200";
//		String osb = "+TransferOutput = \"\"";
		
		// ===========
		
		String submit_cmd = String.format("cmd = %s\n", script);
		if (host_logdir != null) {
			submit_cmd += String.format("%s\n%s\n%s\n", out_cmd, err_cmd, log_cmd);
		}
		
		// --- via JobRouter or direct
		
		boolean use_job_agent = false;
		if (_env_from_config.containsKey("USE_JOB_ROUTER")) {
			use_job_agent = Integer.parseInt(_env_from_config.get("USE_JOB_ROUTER")) == 1;
		}
		if (_environment.get("USE_JOB_ROUTER") != null) {
			use_job_agent = Integer.parseInt(_environment.get("USE_JOB_ROUTER")) == 1;
		}
		String grid_resource = null;
		if (_env_from_config.containsKey("GRID_RESOURCE")) {
			grid_resource = _env_from_config.get("GRID_RESOURCE");
		}
		if (_environment.get("GRID_RESOURCE") != null) {
			grid_resource = _environment.get("GRID_RESOURCE");
		}
		
		if (use_job_agent) {
			submit_cmd += ""
					+ "universe = vanilla\n"
					+ "+WantJobRouter = True\n"
					+ "job_lease_duration = 7200\n"
					+ "ShouldTransferFiles = YES\n";
		}
		else {
			submit_cmd += "universe = grid\n";
			if(grid_resource != null) {
				submit_cmd += String.format("grid_resource = %s\n", grid_resource);
			}
		}
		
		// --- further common attributes
		
		if(grid_resource != null) {
			submit_cmd += "+WantExternalCloud = True\n";
		}
		submit_cmd += ""
				+ "$osb\n"
				+ "$per_hold\n"
				+ "$per_remove\n"
				+ "use_x509userproxy = true\n";

		String env_cmd = String.format("ALIEN_CM_AS_LDAP_PROXY=\'%s\' ", cm)
				+ String.format("ALIEN_JOBAGENT_ID=\'%s\'", _environment.get("ALIEN_JOBAGENT_ID"));
		submit_cmd += String.format("environment = \"%s\"\n", env_cmd);

		// --- allow preceding attributes to be overridden and others added if needed
		
		String custom_jdl_path = String.format("%s/custom-classad.jdl", _environment.get("HOME"));
		String custom_attr_str = "\n#\n# custom attributes start\n#\n\n";
		custom_attr_str += this.readJdlFile(custom_jdl_path);
		custom_attr_str += "\n#\n# custom attributes end\n#\n\n";
		submit_cmd += custom_attr_str;
		
		// --- finally

		submit_cmd += "queue 1\n";

		// =============
		
		if (this._temp_file != null) {
			List<String> temp_file_lines = null;
			try {
				temp_file_lines = Files.readAllLines(Paths.get(this._temp_file.getAbsolutePath()), StandardCharsets.UTF_8);
			} catch (IOException e1) {
				this.logger.info("Error reading old temp file");
				e1.printStackTrace();
			} finally {
				if(temp_file_lines != null) {
					String temp_file_lines_str = "";
					for (String line : temp_file_lines) {
						temp_file_lines_str += line + '\n';
					}
					if (temp_file_lines_str != submit_cmd) {
						if (!this._temp_file.delete()) {
							this.logger.info("Could not delete temp file");
						}
						try {
							this._temp_file = File.createTempFile("htc-submit.", ".jdl");
						} catch(IOException e) {
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
				this._temp_file = File.createTempFile("htc-submit.", ".jdl");
			} catch(IOException e) {
				this.logger.info("Error creating temp file");
				e.printStackTrace();
				return;
			}
		}
		
		try(PrintWriter out = new PrintWriter( this._temp_file.getAbsolutePath() )){
		    out.println( submit_cmd );
		    out.close();
		} catch (FileNotFoundException e) {
			this.logger.info("Error writing to temp file");
			e.printStackTrace();
		}
		
		String temp_file_cmd = String.format("%s %s %s", this._submit_cmd, this._submit_args, this._temp_file.getAbsolutePath());
		ArrayList<String> output = executeCommand(temp_file_cmd);
		for (String line : output) {
			String trimmed_line = line.trim();
			this.logger.info(trimmed_line);
		}
		
	}
	
	private String readJdlFile(String path) {
		String file_contents = "";
		
		String line;
		try {
		    InputStream fis = new FileInputStream(path);
		    InputStreamReader isr = new InputStreamReader(fis);
		    BufferedReader br = new BufferedReader(isr);
		    
			Pattern comment_pattern = Pattern.compile("^\\s*(#.*|//.*)?$");
			Pattern err_spaces_pattern = Pattern.compile("\\\\\\s*$");
			Pattern endl_spaces_pattern = Pattern.compile("\\s+$");
		    while ((line = br.readLine()) != null) {
				Matcher comment_matcher = comment_pattern.matcher(line);
		    	// skip over comment lines
		    	if(comment_matcher.matches()) {
		    		continue;
		    	}
		    	// remove erroneous spaces
		    	line = line.replaceAll(err_spaces_pattern.pattern(), "\\\\\n");
		    	line = line.replaceAll(endl_spaces_pattern.pattern(), "");
		    	if(line.lastIndexOf('\n') == -1) {
		    		line += '\n';
		    	}
		    	file_contents += line;
		    }
		    
		    br.close();
		    isr.close();
		    fis.close();
		    this.logger.info(String.format("Custom attributes added from file: %s.", path));
		} catch (FileNotFoundException e) {
			this.logger.info(String.format("Could not find file: %s.\n", path));
			e.printStackTrace();
			return "";
		} catch (IOException e) {
			this.logger.info(String.format("Error while working with file: %s.\n", path));
			e.printStackTrace();
			return file_contents;
		}
		
		return file_contents;
	}

	@Override
	public int getNumberActive() {
		ArrayList<String> output_list = this.executeCommand("condor_status -schedd -af totalRunningJobs totalIdleJobs");
		if(output_list == null) {
			this.logger.info("Couldn't retrieve the number of active jobs.");
			return -1;
		}
		for( String str : output_list) {
			if(Pattern.matches("(\\d+)\\s+(\\d+)", str)) {
				String[] result_pair = str.split("\\s+");
				int total_running_jobs = Integer.parseInt(result_pair[0]);
				int total_idle_jobs = Integer.parseInt(result_pair[1]);
				int number_active = total_running_jobs + total_idle_jobs;
				return number_active;
			}
		}
		return 0;
	}

	@Override
	public int getNumberQueued() {
		ArrayList<String> output_list = this.executeCommand("condor_status -schedd -af totalIdleJobs");
		if(output_list == null) {
			this.logger.info("Couldn't retrieve the number of queued jobs.");
			return -1;
		}
		for( String str : output_list) {
			if(Pattern.matches("(\\d+)", str)) {
				int total_idle_jobs = Integer.parseInt(str);
				return total_idle_jobs;
			}
		}
		return 0;
	}

	@Override
	public int kill() {
		this.logger.info("Checking proxy renewal service");
		ArrayList<String> kill_cmd_output = null;
		try {
			kill_cmd_output = executeCommand(this._kill_cmd);
		} catch(Exception e){
			this.logger.info(String.format("[HTCONDOR] Prolem while executing command: %s", this._kill_cmd));
			e.printStackTrace();
			return -1;
		}
		finally {
			if(kill_cmd_output != null) {
				this.logger.info("Kill cmd output:\n");
				for (String line : kill_cmd_output) {
					line.trim();
					this.logger.info(line);
				}
			}
		}
		if (_temp_file != null && _temp_file.exists()) {
			this.logger.info(String.format("Deleting temp file  %s after command.", this._temp_file.getAbsolutePath()));
			if (!_temp_file.delete()) {
				this.logger.info(String.format("Could not delete temp file: %s", this._temp_file.getAbsolutePath()));
			}
			else {
				this._temp_file = null;
			}
		}
		return 0;
	}
	// Previously named "_system" in perl
	private ArrayList<String> executeCommand(String cmd) {
		ArrayList<String> proc_output = new ArrayList<String>();
		try {
			final ProcessBuilder proc_builder = new ProcessBuilder(cmd);

			Map<String, String> env = proc_builder.environment();
			env.clear();
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
		} catch (final Throwable t) {
			logger.log(Level.WARNING, String.format("Exception executing command: ", cmd), t);
		}
		this.logger.info(String.format("[HTCONDOR] Command output: %s", proc_output));
		return proc_output;
	}

}
