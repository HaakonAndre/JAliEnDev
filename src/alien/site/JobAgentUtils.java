package alien.site;

import java.io.IOException;

import alien.taskQueue.JDL;
import alien.ui.Dispatcher;


/**
 * Get the JDL object
 * 
 * @author ron
 * @since Jun 05, 2011
 */
public class JobAgentUtils {
	
	/**
	 * @return a JDL
	 */
	public static JDL getJDL() {

		try {
			GetJDL jdl = (GetJDL) Dispatcher.execute(new GetJDL(),true);

			return jdl.getJDL();
		} catch (IOException e) {
			System.out.println("Could not a JDL: ");
			e.printStackTrace();
		}
		return null;

	}

}
