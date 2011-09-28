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
	 * @return if we found all commands
	 */
	public static boolean findCommands(){
		
		boolean state = true;

		cBash = Functions.which("bash");
		if(cBash==null)
			state = false;

		cKill = Functions.which("kill");
		if(cKill==null)
			state = false;

		cOpenssl = Functions.which("openssl");
		if(cOpenssl==null)
			state = false;
		
		cCp = Functions.which("cp");
		if(cCp==null)
			state = false;
		
		cChmod = Functions.which("chmod");
		if(cChmod==null)
			state = false;
		
		cSlapd = Functions.which("slapd");
		if(cSlapd==null)
			state = false;
		
		cSlappasswd = Functions.which("slappasswd");
		if(cSlappasswd==null)
			state = false;
		 
		return state;
		
	}

}
