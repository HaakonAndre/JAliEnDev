package alien.api.aaa;

import java.io.IOException;
import java.io.StringWriter;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;

import org.bouncycastle.openssl.jcajce.JcaPEMWriter;

import alien.api.Cacheable;
import alien.api.Request;
import alien.config.ConfigUtils;
import alien.user.AliEnPrincipal;
import io.github.olivierlemasle.ca.CA;
import io.github.olivierlemasle.ca.Certificate;
import io.github.olivierlemasle.ca.CsrWithPrivateKey;
import io.github.olivierlemasle.ca.DistinguishedName;
import io.github.olivierlemasle.ca.DnBuilder;
import io.github.olivierlemasle.ca.RootCertificate;

/**
 * Get a limited duration (token) certificate for users to authenticate with
 *
 * @author costing
 * @since 2017-07-04
 */
public class GetTokenCertificate extends Request implements Cacheable {
	private static final long serialVersionUID = 7799371357160254760L;

	private static final RootCertificate rootCert;

	static {
		final String caFile = ConfigUtils.getConfig().gets("ca.file",
				System.getProperty("user.home") + System.getProperty("file.separator") + ".globus" + System.getProperty("file.separator") + "alien.p12");

		final String caAlias = ConfigUtils.getConfig().gets("ca.alias", "alien");

		final String caPassword = ConfigUtils.getConfig().gets("ca.password");

		RootCertificate rootCertTemp = null;

		try {
			rootCertTemp = CA.loadRootCertificate(caFile, caPassword.toCharArray(), caAlias);
		} catch (final Throwable t) {
			System.err.println("Exception loading root CA certificate from " + caFile + " (alias " + caAlias + "), password '" + caPassword + "'");
			System.err.println(t.getMessage());
			t.printStackTrace();
		}

		rootCert = rootCertTemp;
	}

	// outgoing fields
	final TokenCertificateType certificateType;
	final String extension;
	final int validity;

	// incoming fields

	private X509Certificate certificate = null;
	private PrivateKey privateKey = null;

	/**
	 * Create a token certificate request for a specific user and role plus the other required fields
	 *
	 * @param user
	 * @param role
	 * @param certificateType
	 * @param extension
	 * @param validity
	 */
	public GetTokenCertificate(final AliEnPrincipal user, final String role, final TokenCertificateType certificateType, final String extension, final int validity) {
		setRequestUser(user);
		setRoleRequest(role);

		if (certificateType == null)
			throw new IllegalArgumentException("Certificate type cannot be null");

		this.certificateType = certificateType;

		if (certificateType == TokenCertificateType.JOB_TOKEN && (extension == null || extension.length() == 0))
			throw new IllegalArgumentException("Job token requires the job ID to be passed as certificate extension");

		this.extension = extension;
		this.validity = validity;
	}

	@Override
	public void run() {
		DnBuilder builder = CA.dn().setC("ch").setO("AliEn");

		switch (certificateType) {
		case USER_CERTIFICATE:
			builder = builder.setCn("Users").setCn(getEffectiveRequester().getName()).setOu(getEffectiveRequesterRole());
			break;
		case JOB_TOKEN:
			builder = builder.setCn("Jobs").setCn(getEffectiveRequester().getName()).setOu(getEffectiveRequesterRole());
			break;
		default:
			// there is no other type at the moment
			break;
		}

		if (extension != null && extension.length() > 0)
			builder = builder.setOu(extension);

		final DistinguishedName userDN = builder.build();

		final CsrWithPrivateKey csr = CA.createCsr().generateRequest(userDN);

		final TemporalAmount amount = Period.ofDays(validity <= 0 || validity > certificateType.getMaxValidity() ? 2 : validity);

		final Certificate cert = rootCert.signCsr(csr).setRandomSerialNumber().setNotAfter(ZonedDateTime.now().plus(amount)).sign();

		certificate = cert.getX509Certificate();
		privateKey = csr.getPrivateKey();
	}

	/**
	 * Get the signed public key of the user token certificate
	 *
	 * @return public key
	 */
	public X509Certificate getCertificate() {
		return certificate;
	}

	/**
	 * Get the signed public key of the user token certificate in PEM format
	 *
	 * @return the PEM formatted public key
	 */
	public String getCertificateAsString() {
		return convertToPEM(certificate);
	}

	/**
	 * Get the private key of the user token certificate pair
	 *
	 * @return the private key
	 */
	public PrivateKey getPrivateKey() {
		return privateKey;
	}

	/**
	 * Get the private key of the user token certificate pair in PEM format
	 *
	 * @return the PEM formatted private key
	 */
	public String getPrivateKeyAsString() {
		return convertToPEM(privateKey);
	}

	/**
	 * Helper function to convert any security object to String
	 *
	 * @param securityObject
	 *            one of X509Certificate or PrivateKey object types
	 * @return the PEM representation of the given object
	 */
	public static final String convertToPEM(final Object securityObject) {
		if (securityObject == null)
			return null;

		final StringWriter sw = new StringWriter(2000);
		try (JcaPEMWriter writer = new JcaPEMWriter(sw)) {
			writer.writeObject(securityObject);
		} catch (final IOException e) {
			e.printStackTrace();
		}
		return sw.toString();
	}

	@Override
	public String getKey() {
		// only cache user tokens, job tokens have the job ID in them and cannot be effectively cached
		if (certificateType == TokenCertificateType.USER_CERTIFICATE)
			return getEffectiveRequester().getName() + "/" + getEffectiveRequesterRole();

		return null;
	}

	@Override
	public long getTimeout() {
		// for the same user don't generate another certificate for 10 minutes but return the same one
		return 1000L * 60 * 10;
	}
}
