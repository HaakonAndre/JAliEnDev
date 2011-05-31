package alien.config;


/**
 * @author ron
 *
 */
public class JAliEnIAm {
	
	private static final String thathsMe = "jAliEn";
	
	private static final String myVersion = "v0.3";

	private static final String myFullName = thathsMe +" "+myVersion; 
	
	/**
	 * @return my name
	 */
	public static String whatsMyName(){
		return thathsMe;
	}
	/**
	 * @return me and the version
	 */
	public static String whatsMyFullName(){
		return myFullName;
	}
	
}
