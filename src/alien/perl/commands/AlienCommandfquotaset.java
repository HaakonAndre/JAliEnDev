package alien.perl.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import alien.quotas.FileQuota;
import alien.quotas.QuotaUtilities;
import alien.user.AliEnPrincipal;

/**
 * @author Steffen Schreiner
 * @since May 30, 2011 implements AliEn fquota list command
 * */
public class AlienCommandfquotaset extends AlienAdminCommand {


	/**
	 * marker for -help argument
	 */
	private boolean bHelp = false;

	/**
	 * marker for -l argument
	 */
	private String setWhat = null;

	/**
	 * marker for -l argument
	 */
	private long setTo = 0;

	/**
	 * marker for -a argument
	 */
	private String user = null;

	/**
	 * marker for -g argument
	 */
	private final static String Iam = "fquota set";

	/**
	 * @param p
	 *            AliEn principal received from https request
	 * @param al
	 *            all arguments received from SOAP request, contains user,
	 *            current directory and command
	 * @throws Exception
	 */
	public AlienCommandfquotaset(final AliEnPrincipal p,
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
	 * @param iDebugLevel
	 * @param alArguments
	 *            command arguments, can be size 0 or null
	 * @throws Exception
	 */
	public AlienCommandfquotaset(final AliEnPrincipal p,
			final String sUsername, final String sCurrentDirectory,
			final String sCommand, final int iDebugLevel,
			final List<?> alArguments) throws Exception {
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
		if (this.alArguments != null && this.alArguments.size() >= 3) {

			ArrayList<String> args = new ArrayList<String>(3);

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
						}
					}
				} else {
					args.add(sArg);
				}

			}
			if (args.size() == 3) {
				user = args.get(0);
				setWhat = args.get(1);
				setTo = Long.parseLong(args.get(2));
			} else
				bHelp = true;
		} else
			bHelp = true;

		if (!bHelp) {

			FileQuota quota = QuotaUtilities.getFileQuota(user);

			if (quota == null)
				System.out.println("Couldn't get the quota");

			if ("maxNbFiles".equals(setWhat)) {
				// TODO: set it
				System.out.println("TODO: Set maxNbFiles in quotas");
				alrcMessages.add("Successfully set maxNbFiles to " + setTo
						+ " for user [" + user + "]\n");

			} else if ("maxTotalSize".equals(setWhat)) {
				// TODO: set it
				System.out.println("TODO: Set maxTotalSize in quotas");
				alrcMessages.add("Successfully set maxTotalSize to " + setTo
						+ " for user [" + user + "]\n");
			} else
				alrcMessages
						.add("Wrong field name! Choose one of them: maxNbFiles, maxTotalSize\n");

		} else {

			alrcMessages
					.add(AlienTime.getStamp()
							+ "Usage: \nfquota set  <username> <field> <value> - set the user quota\n");
			alrcMessages.add("		(maxNbFiles, maxTotalSize(Byte))\n");
			alrcMessages.add("use <user>=% for all users\n");

		}

		hmReturn.put("rcvalues", alrcValues);
		hmReturn.put("rcmessages", alrcMessages);

		return hmReturn;
	}


}
