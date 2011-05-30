package alien.commands;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import lazyj.Log;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.quotas.Quota;
import alien.quotas.QuotaUtilities;
import alien.user.AliEnPrincipal;

/**
 * @author Steffen Schreiner
 * @since May 30, 2011 implements AliEn fquota list command
 * */
public class AlienCommandfquotalist extends AlienCommand {
	/**
	 * ls command arguments : -help/l/a
	 */
	private static ArrayList<String> lsArguments = new ArrayList<String>();


	/**
	 * unit formatting
	 */
	private static HashMap<String,Long> unit = null;
	
	static {
		lsArguments.add("unit");
		
		  unit = new HashMap<String,Long>(4);
		  unit.put("B",new Long(1));
		  unit.put("K",new Long(1024));			  
		  unit.put("M",new Long(1024 * 1024));
		  unit.put("G",new Long(1024 * 1024 * 1024));
	}

	/**
	 * marker for -help argument
	 */
	private boolean bHelp = false;

	/**
	 * marker for -l argument
	 */
	private char bU = 'M';

	/**
	 * marker for -a argument
	 */
	private String user = null;

	/**
	 * marker for -g argument
	 */
	private final static String Iam = "fquota";

	/**
	 * @param p
	 *            AliEn principal received from https request
	 * @param al
	 *            all arguments received from SOAP request, contains user,
	 *            current directory and command
	 * @throws Exception
	 */
	public AlienCommandfquotalist(final AliEnPrincipal p, final ArrayList<Object> al)
			throws Exception {
		super(p, al);
	}

	/**
	 * @param p
	 *            AliEn principal received from https request
	 * @param sUsername
	 *            username received from SOAP request, can be different than the
	 *            one from the https request is the user make a su
	 * @param sCurrentDirectory
	 *            the directory from the user issued the command
	 * @param sCommand
	 *            the command requested through the SOAP request
	 * @param iDebugLevel 
	 * @param alArguments
	 *            command arguments, can be size 0 or null
	 * @throws Exception
	 */
	public AlienCommandfquotalist(final AliEnPrincipal p, final String sUsername,
			final String sCurrentDirectory, final String sCommand,
			final int iDebugLevel, final List<?> alArguments) throws Exception {
		super(p, sUsername, sCurrentDirectory, sCommand, iDebugLevel,
				alArguments);
	}

	/**
	 * @return a map of <String, List<String>> with only 2 keys
	 *         <ul>
	 *         <li>rcvalues - file list</li>
	 *         <li>rcmessages - file list with an extra \n at the end of the
	 *         file name</li>
	 *         </ul>
	 */
	@Override
	public HashMap<String, ArrayList<String>> executeCommand() {
		HashMap<String, ArrayList<String>> hmReturn = new HashMap<String, ArrayList<String>>();

		ArrayList<String> alrcValues = new ArrayList<String>();
		ArrayList<String> alrcMessages = new ArrayList<String>();

		// we got arguments for fquota list
		if (this.alArguments != null && this.alArguments.size() > 0) {

			for (Object oArg : this.alArguments) {
				String sArg = (String) oArg;

				// we got an argument
				if (sArg.startsWith("-")) {
					if (sArg.length() == 1) {
						alrcMessages.add("Expected argument after \"-\" \n "
								+ Iam + " -help for more help\n");
					} else {
						String sLocalArg = sArg.substring(1);

						if (sLocalArg.startsWith("h")) {
							bHelp = true;
						} else {
							if(sArg.startsWith("-unit=")){
								char u = sArg.charAt(6);
								if(!unit.containsKey(u)){
									alrcMessages.add("Unknown unit. Allowed are [BKMG], default M.\n");
									bU = u;
								}
							}
						}
					}
				} else {
					// get the username
					user = sArg;
				}
			}
		}

		if (!bHelp) {

				if (user!=null){
					System.out.println("you are: " + this.pAlienUser);
					System.out.println("you want: " + user);

					if(this.pAlienUser.canBecome(user))
						System.out.println("you can become this user");
					
					if(!this.pAlienUser.canBecome(user))
					 user=null;
				} else
						user = this.pAlienUser.getName();
				
				
			
				// you are allowed to view quota of ...
				if(user!=null){
					
					Quota quota = QuotaUtilities.getFQuota(user);
					
					System.out.println("quota is: " + quota.toString());
					System.out.println("quota totalSize is: " + quota.totalSize);

						  alrcMessages.add("\n------------------------------------------------------------------------------------------\n"
						  + "             user, nbFiles, totalSize("+ bU +") \n"
						  + "------------------------------------------------------------------------------------------\n");
						  
						  long totalSize = quota.totalSize + quota.tmpIncreasedTotalSize / unit.get(bU) ;
						  
						  long maxTotalSize = quota.maxTotalSize / unit.get(bU) ;
						  
						  if(quota.maxTotalSize==-1)
							  maxTotalSize = -1;
						  
						
						  alrcMessages.add(" [1]    "+ user +"  "+quota.nbFiles+"   "+quota.tmpIncreasedNbFiles+"  /  " +
								  "  "  + quota.maxNbFiles +"    "+ totalSize +"  /  "+ maxTotalSize );
								  
						      alrcMessages.add("------------------------------------------------------------------------------------------\n");
						
					
					
					  
					} else{
						alrcMessages.add("You are not allowed to view quotas of this user.\n");
					}
			
		} else {
			
			
				
			alrcMessages.add(AlienTime.getStamp()
					+ "Usage: ls [-laFn|b|h] [<directory>]\n");
			alrcMessages.add("		-l : long format\n");
			alrcMessages.add("		-a : show hidden .* files\n");
			alrcMessages.add("		-F : add trailing / to directory names\n");
			alrcMessages
					.add("		-n: switch off the colour output	[NOT IMPLEMENTED]\n");
			alrcMessages.add("		-b : print in guid format\n");
			alrcMessages.add("		-h : print the help text\n");
			alrcMessages
					.add("		-e : display also the expire date	[NOT IMPLEMENTED]\n");

		}

		hmReturn.put("rcvalues", alrcValues);
		hmReturn.put("rcmessages", alrcMessages);

		return hmReturn;
	}

	private static final DateFormat formatter = new SimpleDateFormat("MMM dd HH:mm");

	private static synchronized String format(final Date d){
		return formatter.format(d);
	}

}
