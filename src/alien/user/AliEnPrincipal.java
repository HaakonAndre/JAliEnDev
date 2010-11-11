package alien.user;

import java.security.Principal;
import java.util.Set;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements java.security.Principal
 * 
 * @author Alina Grigoras
 * @since 02-04-2007
 */
public class AliEnPrincipal implements Principal {
	private static final Logger logger = Logger.getLogger(AliEnPrincipal.class.getCanonicalName());

	/**
	 * Set of account names from LDAP that have the given DN
	 */
	private final Set<String> sUsernames;

	/**
	 * Cache the roles
	 */
	private Set<String> roles = null;

	/**
	 * When were the roles generated
	 */
	private long lRolesChecked = 0;

	/**
	 * building a Principal for ALICE user
	 * 
	 * @param dn
	 */
	public AliEnPrincipal(final String dn) {
		final StringTokenizer st = new StringTokenizer(dn, ",");
		String sNewDn = "";

		while (st.hasMoreTokens()) {
			final String sToken = st.nextToken();

			sNewDn = sToken.trim() + (sNewDn.length() == 0 ? "" : "/") + sNewDn;
		}

		if (!sNewDn.startsWith("/"))
			sNewDn = "/" + sNewDn;

		this.sUsernames = LDAPHelper.checkLdapInformation("subject=" + sNewDn, "ou=People,", "uid");

		if (logger.isLoggable(Level.FINE))
			logger.fine("Usernames: '" + sUsernames + "' for DN='" + sNewDn + "'");
	}

	/**
	 * Get one of the accounts that match the given DN (first in the set that we
	 * got from LDAP).
	 * 
	 * @return one account name from LDAP that has the DN
	 */
	@Override
	public String getName() {
		return sUsernames != null && sUsernames.size() > 0 ? sUsernames.iterator().next() : null;
	}

	/**
	 * Get all the accounts that match the given DN
	 * 
	 * @return set of account names that have the DN
	 */
	public Set<String> getNames() {
		return sUsernames;
	}

	@Override
	public String toString() {
		return sUsernames.toString();
	}

	/**
	 * Get all the roles associated with this principal
	 * 
	 * @return all roles defined in LDAP
	 */
	public Set<String> getRoles() {
		if (roles != null && (System.currentTimeMillis() - lRolesChecked) < 1000 * 60 * 5)
			return roles;

		final Set<String> ret = new HashSet<String>();

		for (String sUsername : sUsernames) {
			final Set<String> sRoles = LDAPHelper.checkLdapInformation("users=" + sUsername, "ou=Roles,", "uid");

			if (sRoles != null)
				ret.addAll(sRoles);
		}

		roles = ret;
		lRolesChecked = System.currentTimeMillis();

		return roles;
	}

	/**
	 * Check if this principal has the given role
	 * 
	 * @param role role to check
	 * @return <code>true</code> if the user belongs to this group
	 */
	public boolean hasRole(final String role) {
		if (role == null || role.length() == 0)
			return false;

		if ("users".equals(role))
			return true;

		return getRoles().contains(role);
	}

	/**
	 * Check if this principal can become the given user/role
	 * 
	 * @param role the role to verify
	 * @return <code>true</code> if the role is one of this principal's accounts
	 *         or is any of the roles assigned to any of the accounts,
	 *         <code>false</code> otherwise
	 */
	public boolean canBecome(final String role) {
		if (role == null || role.length() == 0)
			return false;

		final Set<String> names = getNames();

		if (names == null || names.size() == 0)
			return false;

		if (names.contains(role))
			return true;

		if ("users".equals(role))
			return true;

		final Set<String> sRoles = getRoles();

		if (sRoles == null || sRoles.size() == 0)
			return false;

		if (sRoles.contains(role) || sRoles.contains("admin"))
			return true;

		return false;
	}
}
