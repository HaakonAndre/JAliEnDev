package alien.user;

import java.io.Serializable;
import java.net.InetAddress;
import java.security.Principal;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import lazyj.StringFactory;

/**
 * Implements java.security.Principal
 * 
 * @author Alina Grigoras
 * @since 02-04-2007
 */
public class AliEnPrincipal implements Principal, Serializable {
	private final static String userRole = "users";

	private final static List<String> admins = Arrays.asList("admin");

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

	private transient InetAddress remoteEndpoint = null;

	/**
	 * building a Principal for ALICE user
	 * 
	 * @param username
	 */
	AliEnPrincipal(final String username) {
		this.username = StringFactory.get(username);
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
	void setNames(final Set<String> names) {
		sUsernames = new LinkedHashSet<>(names);

		if (!sUsernames.contains(username))
			sUsernames.add(StringFactory.get(username));
	}

	/**
	 * Get all the accounts that match the given DN
	 * 
	 * @return set of account names that have the DN
	 */
	public Set<String> getNames() {
		if (sUsernames == null) {
			sUsernames = new LinkedHashSet<>();
			sUsernames.add(username);
		}

		return sUsernames;
	}

	@Override
	public String toString() {
		return getNames().toString();
	}

	/**
	 * Check if two principals are the same user
	 * 
	 * @param user
	 *            to compare
	 * @return outcome of equals
	 */
	@Override
	public boolean equals(final Object user) {
		if (user == null)
			return false;

		if (this == user)
			return true;

		if (!(user instanceof AliEnPrincipal))
			return false;

		return getName().equals(((AliEnPrincipal) user).getName());
	}

	@Override
	public int hashCode() {
		return getName().hashCode();
	}

	/**
	 * Get all the roles associated with this principal
	 * 
	 * @return all roles defined in LDAP
	 */
	public Set<String> getRoles() {
		if (roles != null && (System.currentTimeMillis() - lRolesChecked) < 1000 * 60 * 5)
			return roles;

		final Set<String> ret = new LinkedHashSet<>();

		for (final String sUsername : getNames()) {
			ret.add(sUsername);

			final Set<String> sRoles = LDAPHelper.checkLdapInformation("users=" + sUsername, "ou=Roles,", "uid");

			if (sRoles != null)
				for (final String s : sRoles)
					ret.add(StringFactory.get(s));
		}

		roles = ret;
		lRolesChecked = System.currentTimeMillis();

		return roles;
	}

	/**
	 * Get default user role, normally the same name as the account
	 * 
	 * @return
	 */
	public String getDefaultRole() {
		final Set<String> allroles = getRoles();

		final String name = getName();

		if (allroles.size() == 0 || allroles.contains(getName()))
			return name;

		return allroles.iterator().next();
	}

	/**
	 * Check if this principal has the given role
	 * 
	 * @param role
	 *            role to check
	 * @return <code>true</code> if the user belongs to this group
	 */
	public boolean hasRole(final String role) {
		if (role == null || role.length() == 0)
			return false;

		if (userRole.equals(role) || getRoles().contains(role))
			return true;

		return false;
	}

	/**
	 * Check if this principal can become the given user/role
	 * 
	 * @param role
	 *            the role to verify
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

		if (userRole.equals(role))
			return true;

		final Set<String> sRoles = getRoles();

		if (sRoles == null || sRoles.size() == 0)
			return false;

		if (sRoles.contains(role) || sRoles.contains("admin"))
			return true;

		return false;
	}

	/**
	 * Return the default user role
	 * 
	 * @return user role
	 */
	public static String userRole() {
		return userRole;
	}

	/**
	 * Check if that role name authorizes admin privileges
	 * 
	 * @param role
	 * @return is admin privileged or not
	 */
	public static boolean roleIsAdmin(final String role) {
		return admins.contains(role);
	}

	/**
	 * @return the endpoint where this guy came from
	 */
	public InetAddress getRemoteEndpoint() {
		return remoteEndpoint;
	}

	/**
	 * Upon accepting a request, set this address to where the connection came
	 * from
	 * 
	 * @param remoteEndpoint
	 */
	public void setRemoteEndpoint(final InetAddress remoteEndpoint) {
		if (this.remoteEndpoint == null)
			this.remoteEndpoint = remoteEndpoint;
		else
			throw new IllegalAccessError("You are not allowed to overwrite this field!");
	}
}
