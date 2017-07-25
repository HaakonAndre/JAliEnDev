package alien.shell.commands;

import java.security.KeyStoreException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;

import alien.api.DispatchSSLClient;
import alien.api.ServerException;
import alien.api.aaa.GetTokenCertificate;
import alien.api.aaa.TokenCertificateType;
import alien.user.JAKeyStore;

public class JAliEnCommandtoken extends JAliEnBaseCommand {

	public JAliEnCommandtoken(JAliEnCOMMander commander, UIPrintWriter out, ArrayList<String> alArguments) {
		super(commander, out, alArguments);
	}

	@Override
	public void run() {
		Certificate cert = null;
		try {
			cert = JAKeyStore.clientCert.getCertificate("User.cert");
		} catch (KeyStoreException e1) {
			e1.printStackTrace();
		}
		X509Certificate x509cert = (X509Certificate) cert;

		GetTokenCertificate tokenreq = new GetTokenCertificate(commander.user, commander.role,
				TokenCertificateType.USER_CERTIFICATE, null, 1, x509cert);

		try {
			tokenreq = DispatchSSLClient.dispatchRequest(tokenreq);
		} catch (ServerException e1) {
			e1.printStackTrace();
		}

		if (out.isRootPrinter()) {
			out.setField("tokencert", tokenreq.getCertificateAsString());
			out.setField("tokenkey", tokenreq.getPrivateKeyAsString());
		} else {
			out.printOut(tokenreq.getCertificateAsString());
			out.printOut(tokenreq.getPrivateKeyAsString());
		}
	}

	@Override
	public void printHelp() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

}
