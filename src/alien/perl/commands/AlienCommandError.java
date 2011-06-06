package alien.perl.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import alien.user.AliEnPrincipal;

/**
 * @author ron
 *
 */
public class AlienCommandError extends AlienCommand {

	
	/**
	 * 
	 */
	public String errorMessage = "";
	
	/**
	 * @param p
	 *            AliEn principal received from https request
	 * @param al
	 *            all arguments received from SOAP request, contains user,
	 *            current directory and command
	 * @throws Exception
	 */
	public AlienCommandError(final AliEnPrincipal p,
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
	public AlienCommandError(final AliEnPrincipal p,
			final String sUsername, final String sCurrentDirectory,
			final String sCommand, final int iDebugLevel,
			final List<?> alArguments) throws Exception {
		super(p, sUsername, sCurrentDirectory, sCommand, iDebugLevel,
				alArguments);
	}


	@Override
	public Object executeCommand() throws Exception {
		HashMap<String, ArrayList<String>> hmReturn = new HashMap<String, ArrayList<String>>();

		ArrayList<String> alrcValues = new ArrayList<String>();
		ArrayList<String> alrcMessages = new ArrayList<String>();

		alrcMessages.add(errorMessage+"\n");

		hmReturn.put("rcvalues", alrcValues);
		hmReturn.put("rcmessages", alrcMessages);

		return hmReturn;
	}

}
