package alien.site.batchqueue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import alien.log.LogUtils;
import lia.util.process.ExternalProcess.ExitStatus;
import utils.ProcessWithTimeout;

/**
 * @author mmmartin
 */
public class FORK extends BatchQueue {

	/**
	 * @param conf
	 * @param logr
	 *            logger
	 */
	public FORK(HashMap<String, Object> conf, Logger logr) {
		this.config = conf;
		logger = logr;
		logger = LogUtils.redirectToCustomHandler(logger, ((String) config.get("host_logdir")) + "JAliEn." + (new Timestamp(System.currentTimeMillis()).getTime() + ".out"));

		logger.info("This VO-Box is " + config.get("ALIEN_CM_AS_LDAP_PROXY") + ", site is " + config.get("site_accountName"));
	}

	@Override
	public void submit(final String script) {
		logger.info("Submit FORK");
		System.out.println("[FORK] Submitting with FORK.");		//TODO: remove
		
		ArrayList<String> cmd = new ArrayList<String>();
		cmd.add("/bin/bash");
		cmd.add(script);
		
		ArrayList<String> proc_output = new ArrayList<String>();
		try {
			final ProcessBuilder proc_builder = new ProcessBuilder(cmd);

			Map<String, String> env = proc_builder.environment();
			env.clear();
			proc_builder.redirectErrorStream(false);

			final Process proc = proc_builder.start();
			BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			String output_str;
			while ((output_str = reader.readLine()) != null) 
			{
				proc_output.add(output_str);
			}
		} catch (final Throwable t) {
			logger.info(String.format("[FORK] Exception executing command: ", cmd));
			t.printStackTrace();
		}
		logger.info(String.format("[FORK] Command output: %s", proc_output));
		System.out.println(String.format("[FORK] Command output:\n %s", proc_output));		//TODO: remove
	}

	@Override
	public int getNumberActive() {
		return 0;
	}

	@Override
	public int getNumberQueued() {
		return 0;
	}

	@Override
	public int kill() {
		return 0;
	}

}
