package alien;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcessBuilder;
import alien.api.APIServer;
import alien.shell.BusyBox;

/**
 * @author ron
 * @since Jun 21, 2011
 */
public class JAliEnSh {
	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		if (args.length > 0 && args[0].equals("-k"))
			JAliEnSh.killAPIService();
		else {

			//JAliEnSh.startAPIService();
			if (JAliEnSh.APIServiceRunning())
				new BusyBox(addr, port, password);
			else
				System.err
						.println("APIService isn't running/couldn't be started, so we won't start jaliensh.");
		}
	}

	private static final String kill = "/bin/kill";
	private static final String fuser = "/bin/fuser";

	private static String addr;
	private static String password;
	private static int port = 0;
	private static int pid;

	private static void startAPIService() {
		if (!JAliEnSh.APIServiceRunning()) {
			
			
			//APIServer.startAPIService();
			
			final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(
					new String[] { "nohup","./run.sh","alien.APIService","&" });

	
			pBuilder.returnOutputOnExit(false);
			pBuilder.redirectErrorStream(false);

			pBuilder.timeout(356, TimeUnit.DAYS);
//			try {
//				pBuilder.start();
//			} catch (Exception e) {
//				e.printStackTrace();
//				System.err.println("Could not start APIService.");
//			}
			System.out.println();
		}
			JAliEnSh.getAPIServicePID();
	}

	private static void killAPIService() {
		if (JAliEnSh.APIServiceRunning()) {

			final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(
					new String[] { kill, pid + "" });

			pBuilder.returnOutputOnExit(true);
			pBuilder.timeout(2, TimeUnit.SECONDS);
			pBuilder.redirectErrorStream(true);
			final ExitStatus exitStatus;
			try {
				exitStatus = pBuilder.start().waitFor();
				if (exitStatus.getExtProcExitStatus() == 0)
					System.out.println("[" + pid + "] APIService killed.");
				else
					System.err.println("Could not kill the APIService, PID:"
							+ pid);

			} catch (Exception e) {
				System.err.println("Could not kill the APIService, PID:" + pid);
			}
		} else
			System.out.println("We didn't find any APIService running.");
	}

	private static boolean APIServiceRunning() {

		JAliEnSh.getAPIServicePID();

		if(port==0)
			return false;
		
		final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(
				new String[] { fuser, port + "/tcp" });

		pBuilder.returnOutputOnExit(true);
		pBuilder.timeout(2, TimeUnit.SECONDS);
		pBuilder.redirectErrorStream(true);
		final ExitStatus exitStatus;
		try {
			exitStatus = pBuilder.start().waitFor();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Could not get information on port/PID over.");
			return false;
		}
		if (exitStatus.getExtProcExitStatus() == 0) {
			// To check what process (if any) is listening on a given port:
			// fuser 10100/tcp
			// 10100/tcp: 5995
			String line[] = exitStatus.getStdOut().trim().split(":");
			if (!line[0].trim().equals(port + "/tcp")
					|| !line[1].trim().equals(pid + ""))
				return false;

		} else {
			return false;
		}

		File f = new File("/proc/" + pid + "/cmdline");
		if (f.exists()) {
			String buffer = "";
			BufferedReader fi = null;
			try {
				fi = new BufferedReader(new InputStreamReader(
						new FileInputStream(f)));
				buffer = fi.readLine();
			} catch (IOException e) {
				e.printStackTrace();
				System.err.println("Could not get information on PID.");
				return false;
			} finally {
				if (fi != null)
					try {
						fi.close();
					} catch (IOException e) {
						// ignore
					}
			}
			if (buffer.contains("alien.APIService"))
				return true;
		}
		return false;
	}

	private static void getAPIServicePID() {

		File f = new File(System.getProperty("user.home")
				+ "/.alien/.uisession");
		if (f.exists()) {
			byte[] buffer = new byte[(int) f.length()];
			BufferedInputStream fi = null;
			try {
				fi = new BufferedInputStream(new FileInputStream(f));
				fi.read(buffer);
			} catch (IOException e) {
				port = 0;
				return;
			} finally {
				if (fi != null)
					try {
						fi.close();
					} catch (IOException e) {
						// ignore
					}
			}
			String[] specs = new String(buffer).split("\n");
			String[] connect = specs[0].split(":");
			addr = connect[0];
			try {
				port = Integer.parseInt(connect[1]);
			} catch (NumberFormatException e) {
				port = 0;
			}
			password = specs[1] + "\n";
			try {
				pid = Integer.parseInt(specs[2]);
			} catch (Exception e) {
				pid = 0;
			}
		}
	}

}
