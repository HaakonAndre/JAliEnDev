package utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import lia.util.process.ExternalProcess.ExecutorFinishStatus;
import lia.util.process.ExternalProcess.ExitStatus;

/**
 * @author costing
 * @since 2016-10-05
 */
public class ProcessWithTimeout extends Thread {
	static transient final Logger logger = ConfigUtils.getLogger(ProcessWithTimeout.class.getCanonicalName());

	private final Process p;

	private final StringBuilder sbOut = new StringBuilder();
	private final StringBuilder sbErr = new StringBuilder();

	private final String command;

	/**
	 * Wrap the process with an output reading thread and helpers for timeout operations
	 *
	 * @param p
	 * @param command
	 *            command that is executed
	 */
	public ProcessWithTimeout(final Process p, final String command) {
		this.p = p;
		this.command = command;

		String title = "ProcessWithTimeout - " + command;

		if (title.length() > 100)
			title = title.substring(0, 100);

		setName(title);
		start();
	}

	private final byte[] buff = new byte[1024];

	private void drain(final InputStream is, final StringBuilder sb) {
		try {
			while (is.available() > 0) {
				final int count = is.read(buff);

				if (count > 0)
					sb.append(new String(buff, 0, count));
				else
					break;
			}
		} catch (@SuppressWarnings("unused") final IOException e) {
			// ignore
		}
	}

	/**
	 * @return stdout of the process
	 */
	public StringBuilder getStdout() {
		return sbOut;
	}

	/**
	 * @return stderr of the process
	 */
	public StringBuilder getStderr() {
		return sbErr;
	}

	@Override
	public void run() {
		try (InputStream stdout = p.getInputStream(); InputStream stderr = p.getErrorStream()) {
			while (!shouldTerminate) {
				drain(stdout, sbOut);
				drain(stderr, sbErr);
			}
		} catch (@SuppressWarnings("unused") final IOException e) {
			// ignore
		}
	}

	private boolean exitedOk = false;

	private int exitValue = -1;

	private boolean shouldTerminate = false;

	/**
	 * @param timeout
	 * @param unit
	 * @return <code>true</code> if the process exited on its own, or <code>false</code> if it was killed forcibly
	 * @throws InterruptedException
	 */
	public boolean waitFor(final long timeout, final TimeUnit unit) throws InterruptedException {
		exitedOk = p.waitFor(timeout, unit);

		if (exitedOk)
			exitValue = p.exitValue();
		else {
			if (logger.isLoggable(Level.FINE))
				logger.log(Level.FINE, "Forcibly terminating command: " + command);

			p.destroyForcibly();
		}

		shouldTerminate = true;

		return exitedOk;
	}

	/**
	 * @return command exit value
	 */
	public int exitValue() {
		return exitValue;
	}

	/**
	 * @return <code>true</code> if the process exited on its own or <code>false</code> if it was killed
	 */
	public boolean exitedOk() {
		return exitedOk;
	}

	@Override
	public String toString() {
		if (shouldTerminate) {
			if (exitedOk)
				return "Process exited normally with code " + exitValue;

			return "Process was forcefully terminated";
		}

		return "Process is still running";
	}

	/**
	 * @return the entire state as an ExitStatus object
	 */
	public ExitStatus getExitStatus() {
		final ExecutorFinishStatus status = (exitedOk ? (exitValue == 0 ? ExecutorFinishStatus.NORMAL : ExecutorFinishStatus.ERROR) : ExecutorFinishStatus.TIMED_OUT);
		return new ExitStatus(-1, exitValue, status, sbOut.toString(), sbErr.toString());
	}
}
