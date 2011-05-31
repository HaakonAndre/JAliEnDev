package alien.commands;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import lazyj.Log;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.config.ConfigUtils;
import alien.config.Context;
import alien.config.SOAPLogger;
import alien.user.AliEnPrincipal;

/**
 * @author Alina Grigoras
 * @since May 10, 2011 implements AliEn ls command
 * */
public class AlienCommandls extends AlienCommand {
	/**
	 * ls command arguments : -help/l/a
	 */
	private static ArrayList<String> lsArguments = new ArrayList<String>();

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils
			.getLogger(GUIDUtils.class.getCanonicalName());
	
	static {
		lsArguments.add("h");
		lsArguments.add("l");
		lsArguments.add("a");
		lsArguments.add("F");
		lsArguments.add("n");
		lsArguments.add("b");
	}

	/**
	 * marker for -help argument
	 */
	private boolean bHelp = false;

	/**
	 * marker for -l argument
	 */
	private boolean bL = false;

	/**
	 * marker for -a argument
	 */
	private boolean bA = false;

	/**
	 * marker for -F argument
	 */
	private boolean bF = false;

	/**
	 * marker for -n argument
	 */
	@SuppressWarnings("unused")
	private boolean bN = false;

	/**
	 * marker for -b argument
	 */
	private boolean bB = false;

	/**
	 * marker for -g argument
	 */
	private final static String Iam = "ls";

	/**
	 * @param p
	 *            AliEn principal received from https request
	 * @param al
	 *            all arguments received from SOAP request, contains user,
	 *            current directory and command
	 * @throws Exception
	 */
	public AlienCommandls(final AliEnPrincipal p, final ArrayList<Object> al)
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
	public AlienCommandls(final AliEnPrincipal p, final String sUsername,
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

		ArrayList<String> alPaths = new ArrayList<String>();

		// we got arguments for ls
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
							char[] sLetters = sLocalArg.toCharArray();

							for (char cLetter : sLetters) {

								if (!lsArguments.contains(cLetter + "")) {
									alrcMessages.add("Unknown argument "
											+ cLetter + "! \n " + Iam
											+ " -help for more help\n");
								} else {
									if ("l".equals(cLetter + ""))
										bL = true;

									else if ("a".equals(cLetter + ""))
										bA = true;

									else if ("F".equals(cLetter + ""))
										bF = true;

									else if ("b".equals(cLetter + ""))
										bB = true;

								}
							}
						}
					}
				} else {
					// we got paths
					alPaths.add(sArg);
				}
			}
		} else {
			alPaths.add(this.sCurrentDirectory);
		}

		if (!bHelp) {

			int iDirs = alPaths.size();

			if (iDirs == 0)
				alPaths.add(this.sCurrentDirectory);

			for (String sPath : alPaths) {
				// listing current directory
				if (!sPath.startsWith("/"))
					sPath = this.sCurrentDirectory + sPath;

				Log.log(Log.INFO, "Spath = \"" + sPath + "\"");

				final LFN entry = LFNUtils.getLFN(sPath);

				// what message in case of error?
				if (entry != null) {

					List<LFN> lLFN;

					if (entry.type == 'd') {
						lLFN = entry.list();
					} else
						lLFN = Arrays.asList(entry);

//					if (iDirs != 1) {
//						alrcMessages.add(sPath + "\n");
//					}

					for (LFN localLFN : lLFN) {

						if (!bA && localLFN.getFileName().startsWith("."))
							continue;

						String ret = "";
						if (bB){
							if(localLFN.type=='d')
								continue;
							ret += localLFN.guid.toString().toUpperCase() + "	"
									+ localLFN.getName();
						}
						else {

							if (bL)
								ret += FileSystemUtils
										.getFormatedTypeAndPerm(localLFN)
										+ "   "
										+ localLFN.owner
										+ " "
										+ localLFN.gowner
										+ " "
										+ padLeft(String.valueOf(localLFN.size), 12)
										+ " "
										+ format(localLFN.ctime)
										+ "            " + localLFN.getFileName();
							else
								ret += localLFN.getFileName();

							if (bF && (localLFN.type == 'd'))
								ret += "/";
						}

						alrcMessages.add(ret + "\n");
					}
				} else {
					alrcMessages.add("No such file or directory\n");
				}
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
		
		logger.log(Level.SEVERE, "buuuuuuuuuuuuuuuuubuuuuuuuuuubbbbbbbbbbbbbbbbbbbbbbb");

		hmReturn.put("rcvalues", alrcValues);
		
		final Object o = Context.getTheadContext("logger");
		
		if (o!=null){
			final SOAPLogger soaplogger = (SOAPLogger) o;
			
			final String message = soaplogger.getLog();
			
			if (message.length()>0)
				alrcMessages.add(0, message+"\n");
		}
		
		hmReturn.put("rcmessages", alrcMessages);

		return hmReturn;
	}

	private static final DateFormat formatter = new SimpleDateFormat("MMM dd HH:mm");

	private static synchronized String format(final Date d){
		return formatter.format(d);
	}

}
