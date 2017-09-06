package alien.site.batchqueue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.Date;

import org.apache.tomcat.jni.Proc;

import alien.log.LogUtils;

/**
 * @author mmmartin
 */
public class HTCONDOR extends BatchQueue {

	private Map<String, String> environment = System.getenv();
	private ArrayList<Process> process_list = new ArrayList<Process>();

	/**
	 * @param conf
	 * @param logr
	 *            logger
	 */
	public HTCONDOR(HashMap<String, Object> conf, Logger logr) {
		this.config = conf;
		logger = logr;
		logger = LogUtils.redirectToCustomHandler(logger, ((String) config.get("host_logdir")) + "JAliEn." + (new Timestamp(System.currentTimeMillis()).getTime() + ".out"));

		logger.info("This VO-Box is " + config.get("ALIEN_CM_AS_LDAP_PROXY") + ", site is " + config.get("site_accountName"));
	}

	@Override
	public void submit(final String script) {
		logger.info("Submit HTCONDOR");
		String cm = String.format("%s:%d", this.config.get("host"), this.config.get("CLUSTERMONITOR_PORT"));
		
		DateFormat date_format = new SimpleDateFormat("yyyy-MM-dd");
		String current_date_str = date_format.format(new Date());
		
		String log_folder_path = String.format("%s/%s", environment.get("HTCONDOR_LOG_PATH"), current_date_str);
		File log_folder = new File(log_folder_path);
		if (!(log_folder.exists()) || !(log_folder.isDirectory())) {
			try {
				log_folder.mkdir();
			} catch (SecurityException e) {
				logger.info(String.format("[HTCONDOR] Couldn't create log folder: %s", log_folder_path));
				e.printStackTrace();
			}
		}
		String file_base_name = String.format("%s/jobagent_%s", log_folder_path, (String)this.config.get("ALIEN_JOBAGENT_ID"));
		String log_cmd = String.format("log = %s.log", file_base_name);
		String out_cmd = "";
		String err_cmd = "";
		File enable_sandbox_file= new File(environment.get("HOME") + "/enable-sandbox"); 
		if (enable_sandbox_file.exists() || (this.logger.getLevel() != null)) {
			out_cmd = String.format("output = %s.out", file_base_name);
			err_cmd = String.format("error = %s.err", file_base_name);
		}
		
		String per_hold = "periodic_hold = JobStatus == 1 && "
				+ "( GridJobStatus =?= undefined && CurrentTime - EnteredCurrentStatus > 1800 ) || "
				+ "JobStatus <= 2 && ( CurrentTime - EnteredCurrentStatus > 172800 )";
		String per_remove = "periodic_remove = CurrentTime - QDate > 259200";
		String osb = "+TransferOutput = \"\"";
		String submit_cmd = String.format("cmd = %s\n", script);
		if (environment.get("HTCONDOR_LOG_PATH") != null) {
			submit_cmd += String.format("%s\n%s\n%s\n", out_cmd, err_cmd, log_cmd);
		}
	}

	@Override
	public int getNumberActive() {
		ArrayList<String> output_list = this.executeCommand("condor_status -schedd -af totalRunningJobs totalIdleJobs");
		if(output_list == null) {
			logger.info("Couldn't retrieve the number of active jobs.");
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
			logger.info("Couldn't retrieve the number of queued jobs.");
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
			process_list.add(proc);
			if(!proc.waitFor(60, TimeUnit.SECONDS)){
				logger.info(String.format("LCG Timeout for: %s\nKilling the process with id %i", cmd, proc));
				proc.destroyForcibly();
				process_list.remove(proc);
				throw new InterruptedException("Timeout");
			}
			BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			String output_str = null;
			while ( (output_str = reader.readLine()) != null) {
				proc_output.add(output_str);
			}
		} catch (IOException e) {
			logger.info(String.format("[HTCONDOR] Could not execute command: %s", cmd));
			e.printStackTrace();
			return null;
		} catch (InterruptedException e) {
			logger.info(String.format("[HTCONDOR] Command interrupted: %s", cmd));
			e.printStackTrace();
			return null;
		}
		logger.info(String.format("[HTCONDOR] Command output: %s", proc_output));
		return proc_output;
	}

}
