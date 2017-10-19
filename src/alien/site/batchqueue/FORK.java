package alien.site.batchqueue;

import java.io.BufferedReader;
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
		System.out.println("[ishelest DEBUG] Submitting with FORK.");		//TODO: remove
		
		ArrayList<String> cmd = new ArrayList<String>();
		cmd.add("/bin/bash");
		
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
			System.out.println("[ishelest DEBUG] Process exit status: " + exitStatus.getExecutorFinishStatus());

			if (exitStatus.getExtProcExitStatus() == 0) {
				final BufferedReader reader = new BufferedReader(new StringReader(exitStatus.getStdOut()));

				String output_str;

				while ((output_str = reader.readLine()) != null)
					proc_output.add(output_str.trim());
			}
		} catch (final Throwable t) {
			System.out.println(String.format("[ishelest DEBUG] Exception executing command: ", cmd));
			t.printStackTrace();
		}
		System.out.println(String.format("[FORK] Command output: %s", proc_output));
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
