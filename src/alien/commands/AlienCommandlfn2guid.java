package alien.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import lazyj.Log;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;

/**
 * @author ron
 * @since May 28, 2011 implements AliEn whereis command
 * */
public class AlienCommandlfn2guid extends AlienCommand {
	/**
	 * ls command arguments : -help/l/a
	 */
	private static ArrayList<String> lsArguments = new ArrayList<String>();

	static {
		lsArguments.add("help");
		lsArguments.add("h");
		lsArguments.add("l");
		lsArguments.add("s");
		lsArguments.add("g");
		lsArguments.add("r");
	}

	/**
	 * marker for -help argument
	 */
	private boolean bHelp = false;


	/**
	 * textual Name of the command
	 */
	private final static String Iam = "lfn2guid";

	/**
	 * marker for -s argument
	 */
	private String slfn = null;

	
	/**
	 * @param p
	 *            AliEn principal received from https request
	 * @param al
	 *            all arguments received from SOAP request, contains user,
	 *            current directory and command
	 * @throws Exception
	 */
	public AlienCommandlfn2guid(final AliEnPrincipal p,
			final ArrayList<Object> al) throws Exception {
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
	 * @param alArguments
	 *            command arguments, can be size 0 or null
	 * @throws Exception
	 */
	public AlienCommandlfn2guid(final AliEnPrincipal p, final String sUsername,
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
								}
							}
						}
					}
				} else {
					// we got paths
					slfn = sArg;
				}
			}
			if (slfn == null)
				bHelp = true;
		} else {
			bHelp = true;
		}

		if (!bHelp) {

			// listing current directory
			if (!slfn.startsWith("/"))
				slfn = this.sCurrentDirectory + slfn;

			Log.log(Log.INFO, "Spath = \"" + slfn + "\"");

			LFN lfn = LFNUtils.getLFN(slfn);

			if (lfn != null) {

					alrcMessages.add(padLeft(lfn.getFileName(), 40) 
							+ lfn.guid.toString().toUpperCase()+"\n");
		
					alrcValues.add(lfn.guid.toString().toUpperCase());

			} else {
				alrcMessages.add(AlienTime.getStamp()
						+ "No such file or directory\n");
			}

		} else {
			alrcMessages.add("Usage:\n");
			alrcMessages.add("	lfn2guid lfn\n");
			alrcMessages.add("\n");

		}

		hmReturn.put("rcvalues", alrcValues);
		hmReturn.put("rcmessages", alrcMessages);

		return hmReturn;
	}

}
