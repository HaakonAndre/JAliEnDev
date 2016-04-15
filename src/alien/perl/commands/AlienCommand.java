package alien.perl.commands;

import java.security.Principal;
import java.util.List;

import alien.config.Context;
import alien.config.SOAPLogger;
import alien.user.AliEnPrincipal;
import lazyj.Format;
import lazyj.Log;

/**
 * @since 2011, 12 May
 * @author Alina Grigoras <br>
 *         Abstract class to implement Alien shell commands <br>
 *         A commands expects at least 3 paramenters:
 *         <ul>
 *         <li>the username that issued the command</li>
 *         <li>current directory from where the user issued the command</li>
 *         <li>the command</li>
 *         <li>extra arguments for the command, this parameter is not mandatory</li>
 *         <li>debug level - optional parameter</li>
 *         </ul>
 */
public abstract class AlienCommand {
	/**
	 * the principal received from http request
	 */
	protected AliEnPrincipal pAlienUser;

	/**
	 * the username received from SOAP Request
	 */
	protected String sUsername = null;

	/**
	 * current directory received from SOAP request
	 */
	protected String sCurrentDirectory;

	/**
	 * current directory received from SOAP request, checked to be absolute path
	 */
	protected String sAbsoluteCurrentDirectory;

	/**
	 * command received from SOAP request
	 */
	protected String sCommand;

	/**
	 * the argument list for the command received through the SOAP request
	 */
	protected List<?> alArguments;

	/**
	 * debug level
	 */
	protected int iDebug = 0;

	/**
	 * Constructor based on the array received from the request <br>
	 * The minimum size of the array is 3:
	 * <ul>
	 * <li>the username that issued the command</li>
	 * <li>current directory from where the user issued the command</li>
	 * <li>the command</li>
	 * <li>extra arguments for the command, this parameter is not mandatory</li>
	 * </ul>
	 *
	 * @param pAlienUser
	 *            AliEn Principal received from the https request. <br>
	 *            It can be different than the user received from SOAP request if the user is using su
	 * @param al
	 *            array containg the user that issued the command, the current directory from where the user issued the command, the command and its arguments
	 * @throws Exception
	 */
	public AlienCommand(final AliEnPrincipal pAlienUser, final List<?> al) throws Exception {
		Log.log(Log.FINER, "Entering AliEn Command constructor with 2 params");

		if (pAlienUser == null)
			throw new SecurityException("No Alien Principal! We hane no credentials");

		if (al.size() < 3)
			throw new Exception("Alien Command did not receive minimum number of arguments (in this order): username, current directory, command (+ arguments)? ");

		this.pAlienUser = pAlienUser;

		final String sLocalUsername = (String) al.get(0);
		final String sLocalCurrentDirectory = (String) al.get(1);
		final String sLocalCommand = (String) al.get(2);

		if (sUsername != null) {
			if (!pAlienUser.canBecome(sUsername))
				throw new PerlSecurityException("You are not allowed to execute this command as [sUsername]");

			this.sUsername = pAlienUser.getName();
		}

		Log.log(Log.FINER, "Inside AliEn command constructor with the values: " + sLocalUsername + " / " + sLocalCurrentDirectory + " / " + sLocalCommand);

		final int alSize = al.size();

		List<?> alLocalArguments = null;

		if (alSize > 3) {

			final Object o = al.get(alSize - 1);

			if (o instanceof String) {
				final String sDebug = (String) o;

				if (sDebug.startsWith("-debug=")) {
					final String sDebugLevel = Format.replace(sDebug, "-debug=", "");

					try {
						this.iDebug = Integer.parseInt(sDebugLevel);

					} catch (final Exception e) {
						Log.log(Log.ERROR, "Wrong debug level = " + sDebug + " : " + e.getMessage());
					}

					alLocalArguments = al.subList(3, alSize - 1);
				} else
					alLocalArguments = al.subList(3, alSize);

			} else
				alLocalArguments = al.subList(3, alSize);
		}

		this.sUsername = sLocalUsername;
		this.sCurrentDirectory = sLocalCurrentDirectory;
		this.sCommand = sLocalCommand;
		this.alArguments = alLocalArguments;

		Log.log(Log.FINER, "Exiting AliEn Command constructor with 2 params");
	}

	/**
	 * @param pAlienPrincipal
	 *            AliEn principal received from https request. It can be different from the user received from the SOAP request if the used is using su
	 * @param sUsername
	 *            the username received from the SOAP request
	 * @param sCurrentDirectory
	 *            the directory from where the user issued the command
	 * @param sCommand
	 *            the command
	 * @param iDebugLevel
	 *            the debug level received from the soap request
	 * @param alArguments
	 *            command arguments
	 * @throws Exception
	 */
	public AlienCommand(final AliEnPrincipal pAlienPrincipal, final String sUsername, final String sCurrentDirectory, final String sCommand, final int iDebugLevel, final List<?> alArguments)
			throws Exception {
		Log.log(Log.FINER, "Entering AliEn Command constructor with 5 params");

		if (sUsername == null || sUsername.length() == 0)
			throw new Exception("Empty username");

		if (sCurrentDirectory == null || sCurrentDirectory.length() == 0)
			throw new Exception("Empty current directory");

		if (sCommand == null || sCommand.length() == 0)
			throw new Exception("Empty command");

		this.pAlienUser = pAlienPrincipal;
		this.sUsername = sUsername;
		this.sCurrentDirectory = sCurrentDirectory;
		this.sCommand = sCommand;
		this.alArguments = alArguments;
		this.iDebug = iDebugLevel;

		if (!pAlienUser.canBecome(sUsername))
			throw new PerlSecurityException("You are not allowed to execute this command as [sUsername]");

		this.sUsername = pAlienUser.getName();

		Log.log(Log.FINER, "Inside AliEn command constructor with the values: " + sUsername + " / " + sCurrentDirectory + " / " + sCommand);

		Log.log(Log.FINER, "Exiting AliEn Command constructor with 5 params");
	}

	/**
	 * @return debug level
	 */
	public int getiDebug() {
		return iDebug;
	}

	/**
	 * @param iDebug
	 *            setting the debug level
	 */
	public void setiDebug(final int iDebug) {
		this.iDebug = iDebug;
	}

	/**
	 * @return the username that issued the command
	 */
	public String getsUsername() {
		return sUsername;
	}

	/**
	 * @return the current directory of the user that issued the command
	 */
	public String getsCurrentDirectory() {
		return sCurrentDirectory;
	}

	/**
	 * @return the command issued by the user
	 */
	public String getsCommand() {
		return sCommand;
	}

	/**
	 * @return the arguments of the command issued by the user. This can be null or empty
	 */
	public List<?> getAlArguments() {
		return alArguments;
	}

	/**
	 * @return alien principal received from http request
	 */
	public Principal getpAlienUser() {
		return pAlienUser;
	}

	/**
	 * to be implemented by all AliEn commands
	 *
	 * @return the command output
	 * @throws Exception
	 */
	public abstract Object executeCommand() throws Exception;

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("AlienCommand { \n");

		sb.append("		Username = " + this.sUsername + " \n");
		sb.append("		Current directory = " + this.sCurrentDirectory + " \n");
		sb.append("		Command = " + this.sCommand + " \n");
		sb.append("		User principal = " + this.pAlienUser.getName() + " \n");
		sb.append("		Arguments = ");

		if (this.alArguments != null)
			for (final Object o : this.alArguments)
				sb.append(o.toString() + " ");

		sb.append(" } \n");

		return sb.toString();
	}

	/**
	 * @param s
	 * @param n
	 * @return left-padded string
	 */
	public static String padLeft(final String s, final int n) {
		return String.format("%1$#" + n + "s", s);
	}

	/**
	 * @param s
	 * @param n
	 * @return right-padded string
	 */
	public static String padRight(final String s, final int n) {
		return String.format("%1$-" + n + "s", s);
	}

	/**
	 * @return the log message, if any, otherwise <code>null</code>
	 */
	protected static String getLogMessages() {
		final Object o = Context.getTheadContext("logger");

		if (o != null) {
			final SOAPLogger soaplogger = (SOAPLogger) o;

			String message = soaplogger.toString();

			if (message.length() > 0) {
				if (!message.endsWith("\n"))
					message += "\n";

				return message;
			}
		}

		return null;
	}
}
