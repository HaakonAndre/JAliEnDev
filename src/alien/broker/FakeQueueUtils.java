package alien.broker;

import java.io.File;
import java.io.IOException;

import alien.taskQueue.JDL;

/**
 * @author ron
 *  @since Jun 05, 2011
 */
public class FakeQueueUtils {

	private static boolean queueBlocked = false;
	
	/**
	 * @return JDL
	 */
	public static JDL getJDL(){
		if(!queueBlocked){
			File jdl = new File("/tmp/myFirst.jdl");
			queueBlocked = true;
			try {
				return new JDL(jdl);
			} catch (IOException e) {
				queueBlocked = false;
				e.printStackTrace();
			}
		}
		return null;
	}
}
