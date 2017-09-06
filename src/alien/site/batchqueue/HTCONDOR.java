package alien.site.batchqueue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.tomcat.jni.Proc;

import alien.log.LogUtils;

/**
 * @author mmmartin
 */
public class HTCONDOR extends BatchQueue {

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
			if(!proc.waitFor(60, TimeUnit.SECONDS)){
				logger.info(String.format("LCG Timeout for: %s\nKilling the process with id %i", cmd, proc));
				proc.destroyForcibly();
			BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			String output_str = null;
			while ( (output_str = reader.readLine()) != null) {
				proc_output.add(output_str);
			}
			}
		} catch (IOException e) {
			logger.info(String.format("[HTCONDOR] Could not execute comand: %s", cmd));
			e.printStackTrace();
			return null;
		} catch (InterruptedException e) {
			logger.info(String.format("[HTCONDOR] Comand interrupted: %s", cmd));
			e.printStackTrace();
			return null;
		}
		logger.info(String.format("[HTCONDOR] Command output: %s", proc_output));
		return proc_output;
	}

}
