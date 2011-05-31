package alien.config;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author ron
 *
 */
public class JAliEnIAm {
	
	private static final String thathsMe = "jAliEn";
	
	private static final String myVersion = "v0.3";

	
	public static String whatsMyName(){
		return thathsMe;
	}
	public static String whatsMyFullName(){
		return thathsMe +" "+myVersion;
	}
	
}
