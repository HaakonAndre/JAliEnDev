package alien.test;

import alien.test.utils.Functions;

/**
 * @author ron
 * @since Sep 09, 2011
 */
public class TestBrain {


	/**
	 * location of the bash binary
	 */
	public static String cBash = "";

	/**
	 * location of the bash binary
	 */
	public static String cKill = "";

	/**
	 * location of the openssl binary
	 */
	public static String cOpenssl = "";
	
	/**
	 * location of the chmod binary
	 */
	public static String cChmod = "";
	
	/**
	 * location of the cp binary
	 */
	public static String cCp = "";
	
	/**
	 * location of the slapd binary
	 */
	public static String cSlapd = "";

	/**
	 * location of the slapd binary
	 */
	public static String cSlappasswd = "";

	/**
	 * location of the mysql binary
	 */
	public static String cMysql = "";
	
	/**
	 * @return if we found all commands
	 */
	public static boolean findCommands(){
		
		boolean state = true;

		cBash = Functions.which("bash");
		if(cBash==null){
			System.err.println("Couldn't find command: bash");
			state = false;
		}

		cKill = Functions.which("kill");
		if(cKill==null){
			System.err.println("Couldn't find command: kill");
			state = false;
		}
		
		cOpenssl = Functions.which("openssl");
		if(cOpenssl==null){
			System.err.println("Couldn't find command: openssl");
			state = false;
		}
		
		cCp = Functions.which("cp");
		if(cCp==null){
			System.err.println("Couldn't find command: cp");
			state = false;
		}
		
		cChmod = Functions.which("chmod");
		if(cChmod==null){
			System.err.println("Couldn't find command: chmod");
			state = false;
		}
		
		cSlapd = Functions.which("slapd");
		if(cSlapd==null){
			System.err.println("Couldn't find command: slapd");
			state = false;
		}
		
		cSlappasswd = Functions.which("slappasswd");
		if(cSlappasswd==null){
			System.err.println("Couldn't find command: slappasswd");
			state = false;
		}
		
		cMysql = Functions.which("mysql");
		if(cMysql==null){
			System.err.println("Couldn't find command: mysql");
			state = false;
		}
		 
		return state;
		
	}

}
