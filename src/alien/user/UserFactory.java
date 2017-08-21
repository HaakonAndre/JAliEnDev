package alien.user;

import java.io.ByteArrayInputStream;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;

/**
 * @author costing
 * @since 2010-11-11
 */
public final class UserFactory {
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(UserFactory.class.getCanonicalName());

	private UserFactory() {
		// factory
	}

	/**
	 * Get the account for the given username
	 *
	 * @param username
	 * @return the account, or <code>null</code> if it is not a valid username
	 */
	public static AliEnPrincipal getByUsername(final String username) {
		if (username == null)
			return null;

		if (username.equals("admin"))
			return new AliEnPrincipal(username);

		final Set<String> check = LDAPHelper.checkLdapInformation("uid=" + username, "ou=People,", "uid");

		if (check != null && check.size() >= 1)
			return new AliEnPrincipal(username);

		return null;
	}
	
	/**
	 * Get the account for the given role
	 *
	 * @param role
	 * @return the account, or <code>null</code> if it is not a valid role
	 */
	public static AliEnPrincipal getByRole(final String role) {
		if (logger.isLoggable(Level.FINER))
			logger.log(Level.FINER, "Checking for role " + role);
		if (role == null)
			return null;

		if (role.equals("admin"))
			return new AliEnPrincipal(role);

		final Set<String> check = LDAPHelper.checkLdapInformation("uid=" + role, "ou=Roles,", "uid");

		if (check != null && check.size() >= 1)
			return new AliEnPrincipal(role);

		return null;
	}

	/**
	 * Get the account corresponding to this certificate chain
	 *
	 * @param certChain
	 * @return account, or <code>null</code> if no account has this certificate
	 *         associated to it
	 */
	public static AliEnPrincipal getByCertificate(final javax.security.cert.X509Certificate[] certChain) {
		final ArrayList<X509Certificate> certs = new ArrayList<>(certChain.length);
		for (final javax.security.cert.X509Certificate c : certChain)
			certs.add(convert(c));
		if (certs.isEmpty())
			return null;

		final X509Certificate[] c = new X509Certificate[certs.size()];
		certs.toArray(c);
		return getByCertificate(c);
	}

	/**
	 * Get the account corresponding to this certificate chain
	 *
	 * @param certChain
	 * @return account, or <code>null</code> if no account has this certificate
	 *         associated to it
	 */
	public static AliEnPrincipal getByCertificate(final X509Certificate[] certChain) {
		for (int i = 0; i < certChain.length; i++) {
			final String sDN = certChain[i].getSubjectX500Principal().getName();
			final String sDNTransformed = transformDN(sDN);

			if (logger.isLoggable(Level.FINER))
				logger.log(Level.FINER, "Checking for chain " + i + ": " + sDNTransformed);

			final AliEnPrincipal p = getByDN(sDNTransformed);

			if (p != null) {
				if (logger.isLoggable(Level.FINER))
					logger.log(Level.FINER, "Account for " + i + " (" + sDNTransformed + ") is: " + p);

				return p;
			}

			final int idx = sDNTransformed.lastIndexOf('=');

			if (idx < 0 || idx == sDNTransformed.length() - 1)
				return null;

			try {
				Long.parseLong(sDNTransformed.substring(idx + 1));
			} catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
				// try the next certificate in chain only if the last item is a
				// number, so it might be a proxy
				// certificate in fact
				return null;
			}
		}

		return null;
	}

	/**
	 * Get the account corresponding to this certificate DN
	 *
	 * @param dn
	 * @return account, or <code>null</code> if no account has this certificate
	 *         associated to it
	 */
	public static AliEnPrincipal getByDN(final String dn) {
		final Set<AliEnPrincipal> allPrincipal = getAllByDN(dn);

		if (allPrincipal != null && allPrincipal.size() > 0)
			return allPrincipal.iterator().next();

		return null;
	}

	/**
	 * Transform a DN from the comma notation to slash notation, in reverse
	 * order. Example:
	 *
	 * Input: CN=Alina Gabriela
	 * Grigoras,CN=659434,CN=agrigora,OU=Users,OU=Organic Units,DC=cern,DC=ch
	 * Output: /DC=ch/DC=cern/OU=Organic
	 * Units/OU=Users/CN=agrigora/CN=659434/CN=Alina Gabriela Grigoras
	 *
	 * @param subject
	 * @return AliEn-style subject
	 */
	public static String transformDN(final String subject) {
		final StringTokenizer st = new StringTokenizer(subject, ",");
		String sNewDn = "";

		while (st.hasMoreTokens()) {
			final String sToken = st.nextToken();

			sNewDn = sToken.trim() + (sNewDn.length() == 0 ? "" : "/") + sNewDn;
		}

		if (!sNewDn.startsWith("/"))
			sNewDn = "/" + sNewDn;

		return sNewDn;
	}

	/**
	 * Get all accounts to which this certificate subject is associated
	 *
	 * @param dn
	 *            subject, in slash notation
	 * @return all accounts, or <code>null</code> if none matches in fact
	 * @see #transformDN(String)
	 */
	public static Set<AliEnPrincipal> getAllByDN(final String dn) {
		if (logger.isLoggable(Level.FINER))
			logger.log(Level.FINER, "Checking for chain: " + dn);
		// If it is a token cert
		if (dn.startsWith("/C=ch/O=AliEn")) {
			AliEnPrincipal p = null;

			if (dn.contains("/CN=JobAgent")) {
				p = getByUsername("jobagent");
				p.setJobAgent();
			}
			else
			if (dn.contains("/CN=Job")) {
				// Assuming we have user or job token, parse role to switch identity to that user
				// /C=ch/O=AliEn/CN=Job/CN=username/OU=role/OU=extension
				//                                     ^  ^     jobID
				p = getByRole(dn.substring(dn.indexOf("/OU=") + 4, dn.lastIndexOf("/OU=")));
				if (p == null)
					p = getByUsername(dn.substring(dn.lastIndexOf("/CN=") + 4, dn.indexOf("/OU=")));
				p.setJob(Long.valueOf(dn.substring(dn.lastIndexOf("/OU=") + 4)));
				p.setDefaultUser(dn.substring(dn.lastIndexOf("/CN=") + 4, dn.indexOf("/OU=")));
			}
			else
			if (dn.contains("/CN=Users")) {
				// /C=ch/O=AliEn/CN=Users/CN=username/OU=role
				//                                       ^  ^	
				p = getByRole(dn.substring(dn.indexOf("/OU=") + 4));
				if (p == null)
					p = getByUsername(dn.substring(dn.lastIndexOf("/CN=") + 4, dn.indexOf("/OU=")));
				p.setDefaultUser(dn.substring(dn.lastIndexOf("/CN=") + 4, dn.indexOf("/OU=")));
			}

			if (p != null) {
				final Set<AliEnPrincipal> ret = new LinkedHashSet<>();
				ret.add(p);
				return ret;
			}
		}

		final Set<String> check = LDAPHelper.checkLdapInformation("subject=" + dn, "ou=People,", "uid");

		if (check != null && check.size() > 0) {
			final Set<AliEnPrincipal> ret = new LinkedHashSet<>();

			for (final String username : check) {
				final AliEnPrincipal p = new AliEnPrincipal(username);

				p.setNames(check);

				ret.add(p);
			}

			return ret;
		}

		return null;
	}

	/**
	 * @param cert
	 * @return the other type of certificate
	 */
	public static X509Certificate convert(final javax.security.cert.X509Certificate cert) {
		try {
			final byte[] encoded = cert.getEncoded();
			final ByteArrayInputStream bis = new ByteArrayInputStream(encoded);
			final java.security.cert.CertificateFactory cf = java.security.cert.CertificateFactory.getInstance("X.509");
			return (java.security.cert.X509Certificate) cf.generateCertificate(bis);
		} catch (final javax.security.cert.CertificateEncodingException | java.security.cert.CertificateException e) {
			logger.log(Level.FINE, "Cannot convert javax to java X509 Certificate", e);
		}
		return null;
	}

	/**
	 * @param cert
	 * @return the other type of certificate
	 */
	public static javax.security.cert.X509Certificate convert(final X509Certificate cert) {
		try {
			final byte[] encoded = cert.getEncoded();
			return javax.security.cert.X509Certificate.getInstance(encoded);
		} catch (final java.security.cert.CertificateEncodingException | javax.security.cert.CertificateException e) {
			logger.log(Level.FINE, "Cannot convert java to javax X509 Certificate", e);
		}

		return null;
	}

}
