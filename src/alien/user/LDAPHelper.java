package alien.user;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import lazyj.cache.ExpirationCache;

/***
 * operations with LDAP informations
 *
 * @author Alina Grigoras
 * @since 02-04-2007
 */
public class LDAPHelper {
	private static final Logger logger = Logger.getLogger(LDAPHelper.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(LDAPHelper.class.getCanonicalName());

	/**
	 * For statistics, get the number of cached query results
	 *
	 * @return number of cached queries
	 */
	public static int getCacheSize() {
		return cache.size();
	}

	private static String ldapServers = ConfigUtils.getConfig().gets("ldap_server", "alice-ldap.cern.ch:8389");

	private static int ldapPort = ConfigUtils.getConfig().geti("ldap_port", 389);

	private static String ldapRoot = ConfigUtils.getConfig().gets("ldap_root", "o=alice,dc=cern,dc=ch");

	private static final ExpirationCache<String, TreeSet<String>> cache = new ExpirationCache<>(1000);

	private static ArrayList<String> ldapServerList = new ArrayList<>();

	private static final Map<String, String> defaultEnv = new HashMap<>();

	static {
		final StringTokenizer tok = new StringTokenizer(ldapServers, " \t\r\n,;");

		while (tok.hasMoreTokens()) {
			final String addr = tok.nextToken();

			ldapServerList.add(addr);
		}

		if (ldapServerList.size() > 1)
			Collections.shuffle(ldapServerList);

		defaultEnv.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
		defaultEnv.put("com.sun.jndi.ldap.read.timeout", "30000");
		defaultEnv.put("com.sun.jndi.ldap.connect.timeout", "10000");
		defaultEnv.put("com.sun.jndi.ldap.connect.pool.maxsize", "50");
		defaultEnv.put("com.sun.jndi.ldap.connect.pool.prefsize", "5");
		defaultEnv.put("com.sun.jndi.ldap.connect.pool.timeout", "120000");
	}

	/**
	 * @param sParam
	 *            - search query
	 * @param sRootExt
	 *            - subpath
	 * @param sKey
	 *            - key to extract
	 * @return Set of result from the query
	 */
	public static final Set<String> checkLdapInformation(final String sParam, final String sRootExt, final String sKey) {
		return checkLdapInformation(sParam, sRootExt, sKey, true);
	}

	/**
	 * @param sParam
	 *            - search query
	 * @param sRootExt
	 *            - subpath
	 * @param sKey
	 *            - key to extract
	 * @param recursive
	 * @return Set of result from the query
	 */
	public static final Set<String> checkLdapInformation(final String sParam, final String sRootExt, final String sKey, final boolean recursive) {
		final String sCacheKey = sParam + "\n" + sRootExt + "\n" + sKey;

		TreeSet<String> tsResult = cache.get(sCacheKey);

		if (tsResult != null) {
			if (monitor != null)
				monitor.incrementCacheHits("querycache");

			return tsResult;
		}

		if (monitor != null)
			monitor.incrementCacheMisses("querycache");

		final LinkedList<String> hosts = new LinkedList<>();

		for (String host : ldapServerList) {
			final int idx = host.indexOf(':');

			int thisLDAPPort = ldapPort;

			if (idx >= 0 && idx == host.lastIndexOf(':')) {
				thisLDAPPort = Integer.parseInt(host.substring(idx + 1));
				host = host.substring(0, idx);
			}

			try {
				final InetAddress[] addresses = InetAddress.getAllByName(host);

				if (addresses == null || addresses.length == 0)
					hosts.add(host + ":" + thisLDAPPort);
				else
					for (final InetAddress ia : addresses)
						if (ia instanceof Inet6Address)
							hosts.add(0, "[" + ia.getHostAddress() + "]:" + thisLDAPPort);
						else
							hosts.add(ia.getHostAddress() + ":" + thisLDAPPort);
			} catch (@SuppressWarnings("unused") final UnknownHostException uhe) {
				hosts.add(host + ":" + thisLDAPPort);
			}
		}

		if (hosts.size() > 1)
			Collections.shuffle(hosts);

		for (final String ldapServer : hosts) {
			tsResult = new TreeSet<>();

			try {
				final String dirRoot = sRootExt + ldapRoot;

				final Hashtable<String, String> env = new Hashtable<>();
				env.putAll(defaultEnv);
				env.put(Context.PROVIDER_URL, "ldap://" + ldapServer + "/" + dirRoot);

				final DirContext context = new InitialDirContext(env);

				try {
					final SearchControls ctrl = new SearchControls();
					ctrl.setSearchScope(recursive ? SearchControls.SUBTREE_SCOPE : SearchControls.ONELEVEL_SCOPE);

					final NamingEnumeration<SearchResult> enumeration = context.search("", sParam, ctrl);

					while (enumeration.hasMore()) {
						final SearchResult result = enumeration.next();

						final Attributes attribs = result.getAttributes();

						if (attribs == null)
							continue;

						final BasicAttribute ba = (BasicAttribute) attribs.get(sKey);

						if (ba == null)
							continue;

						final NamingEnumeration<?> values = ba.getAll();

						if (values == null)
							continue;

						while (values.hasMoreElements()) {
							final String s = values.nextElement().toString();
							tsResult.add(s);
						}

					}
				} finally {
					context.close();
				}

				cache.put(sCacheKey, tsResult, 1000 * 60 * 15);

				break;
			} catch (final NamingException ne) {
				if (logger.isLoggable(Level.FINE))
					logger.log(Level.WARNING, "Exception executing the LDAP query ('" + sParam + "', '" + sRootExt + "', '" + sKey + "')", ne);
				else
					logger.log(Level.WARNING, "Exception executing the LDAP query ('" + sParam + "', '" + sRootExt + "', '" + sKey + "'): " + ne + " (" + ne.getMessage() + ")");
			}
		}

		if (logger.isLoggable(Level.FINEST))
			logger.fine("Query was:\nparam: " + sParam + "\nroot extension: " + sRootExt + "\nkey: " + sKey + "\nresult:\n" + tsResult);

		return tsResult;
	}

	/**
	 * @param account
	 * @return the set of emails associated to the given account
	 */
	public static Set<String> getEmails(final String account) {
		if (account == null || account.length() == 0)
			return null;

		return LDAPHelper.checkLdapInformation("uid=" + account, "ou=People,", "email");
	}

	/**
	 * Debug method
	 *
	 * @param args
	 */
	public static void main(final String[] args) {
		System.out.println(checkLdapInformation("uid=gconesab", "ou=People,", "email"));

		System.out.println(checkLdapInformation("subject=/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=agrigora/CN=659434/CN=Alina Gabriela Grigoras", "ou=People,", "uid"));

		System.out.println(" 1 " + checkLdapInformation("users=peters", "ou=Roles,", "uid"));

		try {
			Thread.sleep(1000);
		} catch (@SuppressWarnings("unused") final Exception e) { /* nothing */
		}

		System.out.println(" 2 " + checkLdapInformation("users=peters", "ou=Roles,", "uid"));

		try {
			Thread.sleep(1000);
		} catch (@SuppressWarnings("unused") final Exception e) { /* nothing */
		}

		System.out.println(" 3 " + checkLdapInformation("users=peters", "ou=Roles,", "uid"));

		try {
			Thread.sleep(1000);
		} catch (@SuppressWarnings("unused") final Exception e) { /* nothing */
		}

		System.out.println(" 4 " + checkLdapInformation("users=peters", "ou=Roles,", "uid"));
	}
}
