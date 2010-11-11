package alien.user;

import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.catalina.realm.RealmBase;

/**
 * overrides X509Certificate Authentication to check the users in LDAP
 * @author Alina Grigoras
 * @since 02-04-2007
 * */
public class LdapCertificateRealm extends RealmBase {
	private static final Logger	logger	= Logger.getLogger(LdapCertificateRealm.class.getCanonicalName());
	
	/**
	 * @param certChain Certificate chain
	 * @return AlicePrincipal which contains the LDAP username that has the given certificate associated
	 */
	@Override
	public Principal authenticate(final X509Certificate[] certChain) {
		final String sDN = certChain[0].getSubjectDN().getName();
		
		if (logger.isLoggable(Level.FINE))
			logger.fine("Request DN : "+sDN);
		
		return UserFactory.getByDN(UserFactory.transformDN(sDN)); 
	}

	/**
	 * @param principal - the principal which will be checked for the permissions
	 * @param role - the role
	 * @return true/false if the user is in role
	 */
	@Override
	public boolean hasRole(final Principal principal, final String role) {
		if (principal==null)
			return false;
		
		Set<String> sUsernames = null;
		
		if (principal instanceof AliEnPrincipal){
			sUsernames = ((AliEnPrincipal)principal).getNames();
		}
		else{
			sUsernames = new HashSet<String>(1);
			sUsernames.add(principal.getName());
		}
		
		if (logger.isLoggable(Level.FINE))
			logger.fine("hasRole('"+sUsernames+"', '"+role+"')");
		
		if (sUsernames==null || sUsernames.size()==0)
			return false;

		if ("users".equals(role))
		    return true;

		for (String sUsername: sUsernames){
			final Set<String> sRoles = LDAPHelper.checkLdapInformation("users="+sUsername, "ou=Roles,", "uid");

			if (logger.isLoggable(Level.FINER))
				logger.finer("Roles for '"+sUsername+"' : "+sRoles);
		
			if (sRoles.contains(role)){
				if (logger.isLoggable(Level.FINER))
					logger.finer("Returning true because this username has the desired role");
				
				return true;
			}
		}
		
		if (logger.isLoggable(Level.FINER))
			logger.finer("Returning false because no username had the desired role");
		
		return false;
	}

	@Override
	protected String getName() {
		return "LdapCertificateRealm";
	}


	@Override
	protected String getPassword(String arg0) {
		return null;
	}

	@Override
	protected Principal getPrincipal(String arg0) {
		return null;
	}

}
