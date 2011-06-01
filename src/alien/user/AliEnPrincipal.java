package alien.user;

import java.io.Serializable;
import java.security.Principal;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Implements java.security.Principal
 * 
 * @author Alina Grigoras
 * @since 02-04-2007
 */
public class AliEnPrincipal implements Principal, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5393260803758989309L;

	private final String username;
	
	/**
	 * Set of account names from LDAP that have the given DN
	 */
	private Set<String> sUsernames = null;

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
	 * @param username
	 */
	AliEnPrincipal(final String username) {
		this.username = username;
	}

	/**
	 * Get one of the accounts that match the given DN (first in the set that we
	 * got from LDAP).
	 * 
	 * @return one account name from LDAP that has the DN
	 */
	@Override
	public String getName() {
		return username;
	}
	
	/**
	 * If known, all usernames associated to a DN
	 * 
	 * @param names
	 */
	void setNames(final Set<String> names){
		sUsernames = new LinkedHashSet<String>(names);
		
		if (!sUsernames.contains(username))
			sUsernames.add(username);
	}

	/**
	 * Get all the accounts that match the given DN
	 * 
	 * @return set of account names that have the DN
	 */
	public Set<String> getNames() {
		if (sUsernames==null){
			sUsernames = new LinkedHashSet<String>();
			sUsernames.add(username);
		}
		
		return sUsernames;
	}

	@Override
	public String toString() {
		return getNames().toString();
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

		for (String sUsername : getNames()) {
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
