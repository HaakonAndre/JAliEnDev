package alien.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import lazyj.Log;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;

/**
 * @author ron
 * @since May 28, 2011 implements AliEn whereis command
 * */
public class AlienCommandwhereis extends AlienCommand {
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
	 * marker for -l argument
	 */
	private boolean bL = false;

	/**
	 * marker for -s argument
	 */
	private boolean bR = false;

	/**
	 * marker for -s argument
	 */
	private String slfn = null;

	/**
	 * marker for -s argument
	 */
	private boolean bS = false;

	/**
	 * marker for -g argument
	 */
	private boolean bG = false;

	/**
	 * @param p
	 *            AliEn principal received from https request
	 * @param al
	 *            all arguments received from SOAP request, contains user,
	 *            current directory and command
	 * @throws Exception
	 */
	public AlienCommandwhereis(final AliEnPrincipal p,
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
	public AlienCommandwhereis(final AliEnPrincipal p, final String sUsername,
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
						alrcMessages
								.add("Expected argument after \"-\" \n ls -help for more help\n");
					} else {
						String sLocalArg = sArg.substring(1);

						if (sLocalArg.startsWith("h")) {
							bHelp = true;
						} else {
							char[] sLetters = sLocalArg.toCharArray();

							for (char cLetter : sLetters) {

								if (!lsArguments.contains(cLetter + "")) {
									alrcMessages.add("Unknown argument "
											+ cLetter
											+ "! \n ls -help for more help\n");
								} else {
									if ("l".equals(cLetter + ""))
										bL = true;

									if ("g".equals(cLetter + ""))
										bG = true;

									if ("s".equals(cLetter + ""))
										bS = true;

									if ("r".equals(cLetter + ""))
										bR = true;

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
			if (!slfn.startsWith("/") && !bG)
				slfn = this.sCurrentDirectory  + slfn;

			Log.log(Log.INFO, "Spath = \"" + slfn + "\"");

			GUID guid = null;
			if (bG)
				guid = GUIDUtils.getGUID(UUID.fromString(slfn));
			else{
				LFN lfn = LFNUtils.getLFN(slfn);
			
				if(lfn!=null)
					if(lfn.guid!=null)
					guid = GUIDUtils.getGUID(lfn.guid);
			}
			// what message in case of error?
			if (guid != null) {

				Set<PFN> pfns = guid.getPFNs();

				if (bR)
					if (pfns.toArray()[0] != null)
						if (((PFN) pfns.toArray()[0]).pfn.toLowerCase()
								.startsWith("guid://"))
							pfns = GUIDUtils
									.getGUID(
											UUID.fromString(((PFN) pfns
													.toArray()[0]).pfn
													.substring(8, 44)))
									.getPFNs();

				if (!bS)
					alrcMessages.add(AlienTime.getStamp()
						+ "	The file "+slfn.substring(slfn.lastIndexOf("/"),slfn.length())+" is in\n");
				for (PFN pfn : pfns) {

					String se = SEUtils.getSE(pfn.seNumber).seName;
					alrcValues.add(se);
					if (!bL)
						alrcValues.add(pfn.pfn);
					if (!bS)
						alrcMessages.add("	SE => " + se + "	PFN => " + pfn.pfn
								+ "\n");
				}
			} else {
				alrcMessages.add(AlienTime.getStamp() + "No such file or directory\n");
			}

		} else {
			alrcMessages.add("Usage:\n");
			alrcMessages.add("	whereis [-lg] lfn\n");
			alrcMessages.add("\n");
			alrcMessages.add("Options:\n");
			alrcMessages.add("	-l: Get only the list of SE (not the pfn)\n");
			alrcMessages.add("	-g: Use the lfn as guid\n");
			alrcMessages
					.add("	-r: Resolve links (do not give back pointers to zip archives)\n");
			alrcMessages.add("	-s: Silent\n");

		}

		hmReturn.put("rcvalues", alrcValues);
		hmReturn.put("rcmessages", alrcMessages);

		return hmReturn;
	}

}
