package alien.shell.commands;

import java.util.List;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.aaa.GetTokenCertificate;
import alien.api.aaa.TokenCertificateType;
import alien.user.AliEnPrincipal;
import alien.user.UserFactory;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author yuw
 *
 */
public class JAliEnCommandtoken extends JAliEnBaseCommand {

	private TokenCertificateType tokentype = TokenCertificateType.USER_CERTIFICATE;
	private String requestedUser = null; // user1 can ask for token for user2
	private int validity = 2; // Default validity is two days
	private String extension = null; // Token extension (jobID for job tokens)

	/**
	 * @param commander
	 * @param alArguments
	 */
	public JAliEnCommandtoken(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);

		try {

			final OptionParser parser = new OptionParser();

			parser.accepts("u").withRequiredArg();
			parser.accepts("jobid").withRequiredArg();
			parser.accepts("v").withRequiredArg().ofType(Integer.class);
			parser.accepts("t").withRequiredArg();

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			if (options.has("u")) {
				requestedUser = (String) options.valueOf("u");
			}
			if (options.has("t")) {
				switch ((String) options.valueOf("t")) {
				case "job":
					tokentype = TokenCertificateType.JOB_TOKEN;
					break;
				case "jobagent":
					tokentype = TokenCertificateType.JOB_AGENT_TOKEN;
					break;
				default:
					tokentype = TokenCertificateType.USER_CERTIFICATE;
					break;
				}
			}
			if (options.has("v")) {
				validity = ((Integer) options.valueOf("v")).intValue();
			}

			if (tokentype == TokenCertificateType.JOB_TOKEN && options.has("jobid")) {
				extension = (String) options.valueOf("jobid");
			}
		} catch (final OptionException e) {
			printHelp();
			throw e;
		}
	}

	@Override
	public void run() {
		GetTokenCertificate tokenreq = new GetTokenCertificate(commander.user, requestedUser, tokentype, extension, validity);

		try {
			tokenreq = Dispatcher.execute(tokenreq);
		} catch (ServerException e1) {
			e1.printStackTrace();
		}

		// Try to switch user
		java.security.cert.X509Certificate[] cert = commander.user.getUserCert();
		String user = commander.user.getDefaultUser();
		AliEnPrincipal switchUser;

		if (requestedUser != null) {
			if (commander.user.canBecome(requestedUser)) {
				if ((switchUser = UserFactory.getByUsername(requestedUser)) != null)
					commander.user = switchUser;
				else
					if ((switchUser = UserFactory.getByRole(requestedUser)) != null)
						commander.user = switchUser;
					else
						commander.printErrln("User " + requestedUser + " cannot be found. Abort");

				commander.user.setUserCert(cert);
				commander.user.setDefaultUser(user);
			}
			else
				commander.printErrln("Switching user " + commander.user.getName() + " to [" + requestedUser + "] failed");
		}

		// Return tokens
		if (tokenreq != null) {
			commander.printOut("tokencert", tokenreq.getCertificateAsString());
			commander.printOut("tokenkey", tokenreq.getPrivateKeyAsString());
			commander.printOut(tokenreq.getCertificateAsString());
			commander.printOut(tokenreq.getPrivateKeyAsString());
		}
		else
			commander.printErrln("User " + requestedUser + " cannot be found. Abort");
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("token", "[-options]"));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-u <user>"));
		commander.printOutln(helpOption("-v <validity (days)>"));
		commander.printOutln(helpOption("-t <tokentype>"));
		commander.printOutln(helpOption("-jobid <jobID>"));
		commander.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

}
