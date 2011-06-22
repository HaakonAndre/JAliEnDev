package alien.site;

import java.security.SecureRandom;

import alien.api.taskQueue.TaskQueueApiUtils;
import alien.shell.commands.JobAgentCOMMander;
import alien.taskQueue.Job;
import alien.user.JAKeyStore;

/**
 * @author ron
 * @since Jun 05, 2011
 */
public class ComputingElement extends Thread  {

	/**
	 * 
	 */
	public JobAgentCOMMander commander;
	
	static {
		try {
			JAKeyStore.loadPilotKeyStorage();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 
	 */
	public ComputingElement() {

			commander = new JobAgentCOMMander();
	}


	@SuppressWarnings("static-access")
	public void run(){

		while (true) {
			Job j = TaskQueueApiUtils.getJob();
			if (j != null) {
				
				JobAgent jA = new JobAgent(this, j);
				jA.start();
				
			} else{
				System.out.println("Nothing to run right now. Idling 5secs...");
				try {
					Thread.currentThread().sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

		}

	}
}
