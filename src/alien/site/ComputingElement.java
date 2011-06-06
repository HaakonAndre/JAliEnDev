package alien.site;

import alien.api.taskQueue.TaskQueueApiUtils;


/**
 * @author ron
 *  @since Jun 05, 2011
 */
public class ComputingElement {
	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		JobAgent jA = new JobAgent(TaskQueueApiUtils.getJob());
		jA.start();

	}
}
