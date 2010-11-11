package alien.user;

import java.security.cert.X509Certificate;

/**
 * @author costing
 * @since 2010-11-11
 */
public final class UserFactory {

	private UserFactory(){
		// factory
	}
	
	/**
	 * Get the account for the given username
	 * 
	 * @param username
	 * @return the account, or <code>null</code> if it is not a valid username
	 */
	public static AliEnPrincipal getByUsername(final String username){
		return null;
	}
	
	/**
	 * Get the account corresponding to this certificate chain
	 * 
	 * @param certChain
	 * @return account, or <code>null</code> if no account has this certificate associated to it
	 */
	public static AliEnPrincipal getByCertificate(final X509Certificate[] certChain) {
		final String sDN = certChain[0].getSubjectDN().getName();
		
		return getByDN(sDN);
	}
	
	/**
	 * Get the account corresponding to this certificate DN
	 * 
	 * @param dn
	 * @return account, or <code>null</code> if no account has this certificate associated to it
	 */
	public static AliEnPrincipal getByDN(final String dn){
		return new AliEnPrincipal(dn);
	}
}
