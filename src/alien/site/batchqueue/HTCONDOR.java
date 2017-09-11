package alien.site.batchqueue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Date;

import org.apache.tomcat.jni.Proc;

import alien.log.LogUtils;

/**
 * @author mmmartin
 */
public class HTCONDOR extends BatchQueue {

	private Map<String, String> _environment;
	private ArrayList<Process> _process_list;
	private int _counter;
	private String _log_dir_path;
	private String _submit_cmd;
	private String _submit_args;
	private String _user_name;
	private String _kill_cmd;
	private String _status_cmd;

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
		
		this._environment = System.getenv();
		this._process_list = new ArrayList<Process>();
		this._counter = 0;
		this._log_dir_path = (String) config.get("LOG_DIR");
		this._submit_cmd = (config.get("CE_SUBMITCMD") != null ? (String)config.get("CE_SUBMITCMD") : "condor_submit");
		this._submit_args = (_environment.get("SUBMIT_ARGS") != null ? _environment.get("SUBMIT_ARGS") : "");
		if(_environment.get("LOGNAME") != null) {
			this._user_name = _environment.get("LOGNAME");
		}
		else if(_environment.get("USER") != null) {
			this._user_name = _environment.get("LOGNAME");
		}
		else {
			this._user_name = "Unknown User"; // TODO: get process name from PID?
		}

		this._kill_cmd = (config.get("CE_KILLCMD") != null ? (String) config.get("CE_KILLCMD") : "condor_rm");
		this._status_cmd = (config.get("CE_STATUSCMD") != null ? (String) config.get("CE_STATUSCMD") : "condor_q");
	}
	
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
		String cm = String.format("%s:%d", this.config.get("host"), this.config.get("CLUSTERMONITOR_PORT"));
		
		DateFormat date_format = new SimpleDateFormat("yyyy-MM-dd");
		String current_date_str = date_format.format(new Date());
		
		String log_folder_path = String.format("%s/%s", _environment.get("HTCONDOR_LOG_PATH"), current_date_str);
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
		
		String per_hold = "periodic_hold = JobStatus == 1 && "
				+ "( GridJobStatus =?= undefined && CurrentTime - EnteredCurrentStatus > 1800 ) || "
				+ "JobStatus <= 2 && ( CurrentTime - EnteredCurrentStatus > 172800 )";
		String per_remove = "periodic_remove = CurrentTime - QDate > 259200";
		String osb = "+TransferOutput = \"\"";
		
		// ===========
		
		String submit_cmd = String.format("cmd = %s\n", script);
		if (_environment.get("HTCONDOR_LOG_PATH") != null) {
			submit_cmd += String.format("%s\n%s\n%s\n", out_cmd, err_cmd, log_cmd);
		}
		
		// --- via JobRouter or direct
		
		if (_environment.get("USE_JOB_ROUTER") != null) {
			submit_cmd += ""
					+ "universe = vanilla\n"
					+ "+WantJobRouter = True\n"
					+ "job_lease_duration = 7200\n"
					+ "ShouldTransferFiles = YES\n";
		}
		else if(_environment.get("GRID_RESOURCE") != null) {
			submit_cmd += ""
					+ "universe = grid\n"
					+ String.format("grid_resource = %s\n", _environment.get("GRID_RESOURCE"));
		}
		
		// --- further common attributes
		
		if(_environment.get("GRID_RESOURCE") != null) {
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
		this.logger.info(String.format("Custom attributes added from file: %s.", custom_jdl_path));
		
		// --- finally

		submit_cmd += "queue 1\n";

		// =============
		
		this._counter++;
		long time = System.currentTimeMillis() / 1000L;
		time = time >>> 6;
		File jdl_file;
		try {
			jdl_file = File.createTempFile("htc-submit.", ".jdl");
		} catch(IOException e) {
			this.logger.info("Error creating temp file\n");
			e.printStackTrace();
			return;
		}
		
		try(PrintWriter out = new PrintWriter( jdl_file.getAbsolutePath() )){
		    out.println( submit_cmd );
		    out.close();
		} catch (FileNotFoundException e) {
			this.logger.info("Error writing to temp file\n");
			e.printStackTrace();
		}
		
		String temp_file_cmd = String.format("%s %s %s", this._submit_cmd, this._submit_args, jdl_file.getAbsolutePath());
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
				Matcher endl_matcher = err_spaces_pattern.matcher(line);
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
			this.logger.info(String.format("[HTCONDOR] Prolem while executing command: %s", proxy_renewal_cmd));
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
		return 0;
	}
	
	// Previously named "_system" in perl
	private ArrayList<String> executeCommand(String cmd) {
		ProcessBuilder proc_builder = new ProcessBuilder(cmd);
		Map<String, String> env = proc_builder.environment();
		env.clear();
		ArrayList<String> proc_output = new ArrayList<String>();
		try {
			Process proc = proc_builder.start();
			_process_list.add(proc);
			if(!proc.waitFor(60, TimeUnit.SECONDS)){
				this.logger.info(String.format("LCG Timeout for: %s\nKilling the process with id %i", cmd, proc));
				proc.destroyForcibly();
				_process_list.remove(proc);
				throw new InterruptedException("Timeout");
			}
			BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			String output_str = null;
			while ( (output_str = reader.readLine()) != null) {
				proc_output.add(output_str);
			}
		} catch (IOException e) {
			this.logger.info(String.format("[HTCONDOR] Could not execute command: %s", cmd));
			e.printStackTrace();
			return null;
		} catch (InterruptedException e) {
			this.logger.info(String.format("[HTCONDOR] Command interrupted: %s", cmd));
			e.printStackTrace();
			return null;
		}
		this.logger.info(String.format("[HTCONDOR] Command output: %s", proc_output));
		return proc_output;
	}

}
