package alien.user;

import java.io.ByteArrayInputStream;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.StringTokenizer;

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
		if (username==null)
			return null;
		
		if (username.equals("admin"))
			return new AliEnPrincipal(username);
		
		final Set<String> check = LDAPHelper.checkLdapInformation("uid="+username, "ou=People,", "uid");
		
		if (check!=null && check.size()>=1)
			return new AliEnPrincipal(username);
		
		return null;
	}
	
	/**
	 * Get the account corresponding to this certificate chain
	 * 
	 * @param certChain
	 * @return account, or <code>null</code> if no account has this certificate associated to it
	 */
	public static AliEnPrincipal getByCertificate(final javax.security.cert.X509Certificate[] certChain) {
		ArrayList<X509Certificate> certs = new ArrayList<X509Certificate>(certChain.length);
		for(javax.security.cert.X509Certificate c: certChain)
			certs.add(convert(c));		
		if(certs.isEmpty())
			return null;
		
		X509Certificate[] c = new X509Certificate[certs.size()];
		certs.toArray(c);
		return getByCertificate(c);
	}
	
	/**
	 * Get the account corresponding to this certificate chain
	 * 
	 * @param certChain
	 * @return account, or <code>null</code> if no account has this certificate associated to it
	 */
	public static AliEnPrincipal getByCertificate(final X509Certificate[] certChain) {
		for (int i=0; i<certChain.length; i++){
			final String sDN = certChain[i].getSubjectX500Principal().getName();
			final String sDNTransformed = transformDN(sDN);
			
			final AliEnPrincipal p = getByDN(sDNTransformed);
			
			if (p!=null)
				return p;
			
			final int idx = sDNTransformed.lastIndexOf('=');
			
			if (idx<0 || idx==sDNTransformed.length()-1)
				return null;
			
			try{
				Long.parseLong(sDNTransformed.substring(idx+1));
			}
			catch (NumberFormatException nfe){
				// try the next certificate in chain only if the last item is a number, so it might be a proxy certificate in fact
				return null;
			}
		}
		
		return null;
	}
	
	/**
	 * Get the account corresponding to this certificate DN
	 * 
	 * @param dn
	 * @return account, or <code>null</code> if no account has this certificate associated to it
	 */
	public static AliEnPrincipal getByDN(final String dn){
		final Set<AliEnPrincipal> allPrincipal = getAllByDN(dn);
		
		if (allPrincipal!=null && allPrincipal.size()>0)
			return allPrincipal.iterator().next();
		
		return null;
	}
	
	/**
	 * Transform a DN from the comma notation to slash notation, in reverse order. Example:
	 * 
	 * Input: CN=Alina Gabriela Grigoras,CN=659434,CN=agrigora,OU=Users,OU=Organic Units,DC=cern,DC=ch
	 * Output: /DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=agrigora/CN=659434/CN=Alina Gabriela Grigoras
	 * 
	 * @param subject
	 * @return AliEn-style subject
	 */
	public static String transformDN(final String subject){
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
	 * @param dn subject, in slash notation
	 * @return all accounts, or <code>null</code> if none matches in fact
	 * @see #transformDN(String)
	 */
	public static Set<AliEnPrincipal> getAllByDN(final String dn){
		final Set<String> check = LDAPHelper.checkLdapInformation("subject="+dn, "ou=People,", "uid");
		
		if (check!=null && check.size()>0){
			final Set<AliEnPrincipal> ret = new LinkedHashSet<AliEnPrincipal>();
			
			for (final String username: check){
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
	public static X509Certificate convert(javax.security.cert.X509Certificate cert) {
	    try {
	        byte[] encoded = cert.getEncoded();
	        ByteArrayInputStream bis = new ByteArrayInputStream(encoded);
	        java.security.cert.CertificateFactory cf
	            = java.security.cert.CertificateFactory.getInstance("X.509");
	        return (java.security.cert.X509Certificate)cf.generateCertificate(bis);
	    } catch (java.security.cert.CertificateEncodingException e) {
	    	//ignore
	    } catch (javax.security.cert.CertificateEncodingException e) {
	    	//ignore

	    } catch (java.security.cert.CertificateException e) {
	    	//ignore

	    }
	    return null;
	}


	/**
	 * @param cert
	 * @return the other type of certificate
	 */
	public static javax.security.cert.X509Certificate convert(X509Certificate cert) {
	    try {
	        byte[] encoded = cert.getEncoded();
	        return javax.security.cert.X509Certificate.getInstance(encoded);
	    } catch (java.security.cert.CertificateEncodingException e) {
	    	//ignore

	    } catch (javax.security.cert.CertificateEncodingException e) {
	    	//ignore

	    } catch (javax.security.cert.CertificateException e) {
	    	//ignore

	    }
	    return null;
	}
	
}
