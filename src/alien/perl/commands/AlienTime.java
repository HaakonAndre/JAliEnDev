package alien.perl.commands;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import alien.config.JAliEnIAm;

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
			// return formatter.format(new Date())+"	"+ JAliEnIAm.whatsMyFullName()+"		";
			return JAliEnIAm.whatsMyFullName()+",";
		}
	}

}
