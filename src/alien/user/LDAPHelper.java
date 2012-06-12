package alien.user;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
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

import lazyj.cache.ExpirationCache;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;

/***
 * operations with LDAP informations
 * @author Alina Grigoras
 * @since 02-04-2007
 * */
public class LDAPHelper {
	private static final Logger	logger	= Logger.getLogger(LDAPHelper.class.getCanonicalName());
	
	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(LDAPHelper.class.getCanonicalName());
	
	/**
	 * For statistics, get the number of cached query results
	 * 
	 * @return number of cached queries
	 */
	public static int getCacheSize(){
		return cache.size();
	}
	
	private static String ldapServers = ConfigUtils.getConfig().gets("ldap_server", "alice-ldap.cern.ch:8389");
	
	private static String ldapRoot = ConfigUtils.getConfig().gets("ldap_root", "o=alice,dc=cern,dc=ch");
	
	private static final ExpirationCache<String, TreeSet<String>> cache = new ExpirationCache<String, TreeSet<String>>(1000);
	
	private static ArrayList<String> ldapServerList = new ArrayList<String>();
	
	private static final Map<String, String> defaultEnv = new HashMap<String, String>();
	
	static{
		final StringTokenizer tok = new StringTokenizer(ldapServers, " \t\r\n,;");
		
		while (tok.hasMoreTokens())
			ldapServerList.add(tok.nextToken());
		
		defaultEnv.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
		defaultEnv.put("com.sun.jndi.ldap.read.timeout", "30000");
		defaultEnv.put("com.sun.jndi.ldap.connect.timeout", "10000");
		defaultEnv.put("com.sun.jndi.ldap.connect.pool.maxsize", "50");
		defaultEnv.put("com.sun.jndi.ldap.connect.pool.prefsize", "5");
		defaultEnv.put("com.sun.jndi.ldap.connect.pool.timeout", "120000");
	}
	
	/**
	 * @param sParam - search query
	 * @param sRootExt - subpath
	 * @param sKey - key to extract
	 * @return Set of result from the query
	 */
	public static final Set<String> checkLdapInformation(final String sParam, final String sRootExt, final String sKey){
		return checkLdapInformation(sParam, sRootExt, sKey, true);
	}

	/**
	 * @param sParam - search query
	 * @param sRootExt - subpath
	 * @param sKey - key to extract
	 * @param recursive 
	 * @return Set of result from the query
	 */
	public static final Set<String> checkLdapInformation(final String sParam, final String sRootExt, final String sKey, final boolean recursive){
		final String sCacheKey = sParam +"\n" + sRootExt + "\n" + sKey;
		
		TreeSet<String> tsResult = cache.get(sCacheKey);
		
		if (tsResult!=null){
			if (monitor!=null)
				monitor.incrementCacheHits("querycache");
			
			return tsResult;
		}

		if (monitor!=null)
			monitor.incrementCacheMisses("querycache");
		
		List<String> hosts = ldapServerList;
		
		if (hosts.size()>1){
			hosts = new ArrayList<String>(hosts);
			Collections.shuffle(hosts);
		}
		
		for (final String ldapServer: hosts){
			tsResult = new TreeSet<String>();
			
			try {
				final String dirRoot = sRootExt+ldapRoot;
	
				final Hashtable<String, String> env = new Hashtable<String, String>();
				env.putAll(defaultEnv);
				env.put(Context.PROVIDER_URL, "ldap://"+ldapServer+"/" + dirRoot);
	
				final DirContext context = new InitialDirContext(env);
	
				final SearchControls ctrl = new SearchControls();
				ctrl.setSearchScope(recursive ? SearchControls.SUBTREE_SCOPE : SearchControls.ONELEVEL_SCOPE);
	
				final NamingEnumeration<SearchResult> enumeration = context.search("", sParam, ctrl);
	
				while (enumeration.hasMore()) {
					final SearchResult result = enumeration.next();
	
					final Attributes attribs = result.getAttributes();
	
					if (attribs==null)
					    continue;
	
					final BasicAttribute ba = (BasicAttribute) attribs.get(sKey);
	
					if (ba==null)
					    continue;
	
					final NamingEnumeration<?> values = ba.getAll();
					
					if (values==null)
					    continue;
					
					while (values.hasMoreElements()){
						final String s = values.nextElement().toString();
						tsResult.add(s);
					}
	
				}
				
				cache.put(sCacheKey, tsResult, 1000*60*15);
				
				break;
			}
			catch (final NamingException ne) {    
				if (logger.isLoggable(Level.FINE))
				    logger.log(Level.WARNING, "Exception executing the LDAP query ('"+sParam+"', '"+sRootExt+"', '"+sKey+"')", ne);
				else
				    logger.log(Level.WARNING, "Exception executing the LDAP query ('"+sParam+"', '"+sRootExt+"', '"+sKey+"'): "+ne+" ("+ne.getMessage()+")");
			}
		}
		
		if (logger.isLoggable(Level.FINEST))
			logger.fine("Query was:\nparam: "+sParam+"\nroot extension: "+sRootExt+"\nkey: "+sKey+"\nresult:\n"+tsResult);

		return tsResult;
	}
	
	/**
	 * Debug method
	 * 
	 * @param args
	 */
	public static void main(final String[] args){
		System.out.println(checkLdapInformation(
		    "uid=gconesab",
		    "ou=People,",
		    "email"
		));
	
		System.out.println(checkLdapInformation(
				"subject=/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=agrigora/CN=659434/CN=Alina Gabriela Grigoras",
				"ou=People,",
				"uid"
		));
		
		System.out.println(" 1 "+ checkLdapInformation("users=peters", "ou=Roles,", "uid"));
		
		try{ Thread.sleep(1000); } catch (Exception e) { /* nothing */ }
		
		System.out.println(" 2 "+ checkLdapInformation("users=peters", "ou=Roles,", "uid"));
		
		try{ Thread.sleep(1000); } catch (Exception e) { /* nothing */ }
		
		System.out.println(" 3 "+ checkLdapInformation("users=peters", "ou=Roles,", "uid"));
		
		try{ Thread.sleep(1000); } catch (Exception e) { /* nothing */ }
		
		System.out.println(" 4 "+ checkLdapInformation("users=peters", "ou=Roles,", "uid"));	
	}
}
