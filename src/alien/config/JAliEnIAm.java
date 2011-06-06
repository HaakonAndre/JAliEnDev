package alien.config;


/**
 * @author ron
 *
 */
public class JAliEnIAm {
	
	private static final String thathsMe = "jAliEn";
	
	private static final String myVersion = "v0.3";
	
	private static final String alienshprompt = "Jaliensh[alice]";

	private static final String myFullName = thathsMe +" "+myVersion; 
	
	/**
	 * @return the name for the shell prompt
	 */
	public static String myPromptName(){
		return alienshprompt;
	}
	
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
	/**
	 * @return my version
	 */
	public static String whatsVersion(){
		return myVersion;
	}
	
}
