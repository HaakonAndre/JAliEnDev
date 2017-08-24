package alien.shell.commands;

import java.util.ArrayList;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.aaa.GetTokenCertificate;
import alien.api.aaa.TokenCertificateType;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class JAliEnCommandtoken extends JAliEnBaseCommand {

	private TokenCertificateType tokentype = TokenCertificateType.USER_CERTIFICATE;
	private String requestedUser = null; 	// user1 can ask for token for user2
	private int validity = 2; 				// Default validity is two days
	private String extension = null; 		// Token extension (jobID for job tokens)

	public JAliEnCommandtoken(JAliEnCOMMander commander, UIPrintWriter out, ArrayList<String> alArguments) {
		super(commander, out, alArguments);

		try {

			final OptionParser parser = new OptionParser();

			parser.accepts("u").withRequiredArg();
			parser.accepts("jobid").withRequiredArg();
			parser.accepts("v").withRequiredArg();
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
				validity = Integer.parseInt((String) options.valueOf("v"));
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
		GetTokenCertificate tokenreq = new GetTokenCertificate(commander.user, requestedUser, tokentype, extension, validity, commander.user.getUserCert()[0]);

		try {
			tokenreq = Dispatcher.execute(new GetTokenCertificate(commander.user, requestedUser, tokentype, extension, validity, commander.user.getUserCert()[0]));
		} catch (ServerException e1) {
			e1.printStackTrace();
		}

		if (out.isRootPrinter()) {
			out.setField("tokencert", tokenreq.getCertificateAsString());
			out.setField("tokenkey", tokenreq.getPrivateKeyAsString());
		}
		else {
			out.printOut(tokenreq.getCertificateAsString());
			out.printOut(tokenreq.getPrivateKeyAsString());
		}
	}

	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln(helpUsage("token", "[-options]"));
		out.printOutln(helpStartOptions());
		out.printOutln(helpOption("-u <user>"));
		out.printOutln(helpOption("-v <validity (days)>"));
		out.printOutln(helpOption("-t <tokentype>"));
		out.printOutln(helpOption("-jobid <jobID>"));
		out.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

}
