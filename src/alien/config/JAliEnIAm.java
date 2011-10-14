package alien.config;


/**
 * @author ron
 *
 */
public class JAliEnIAm {
	
	private static final String thathsMe = "jSh";
	
	private static final String myVersion = "v0.7";
	
	private static final String alienshprompt = "JSh[alice]";

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
