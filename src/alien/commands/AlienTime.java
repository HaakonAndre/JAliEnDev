package alien.commands;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author ron
 *
 */
public class AlienTime {

	private static final DateFormat formatter = new SimpleDateFormat("MMM dd HH:mm:ss"); 
	
	/**
	 * @return current timestamp for logging
	 */
	public static String getStamp() {
		synchronized (formatter){
			return formatter.format(new Date())+"	jAuthen_v0.2		";
		}
	}
}
