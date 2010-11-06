package alien.se;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import alien.config.ConfigUtils;

import lazyj.DBFunctions;

/**
 * @author costing
 * @since Nov 4, 2010
 */
public class SE {
	
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(SE.class.getCanonicalName());

	/**
	 * SE name
	 */
	public String seName;
	
	/**
	 * SE number
	 */
	public int seNumber;
	
	/**
	 * QoS associated to this storage elements
	 */
	public Set<String> qos;
	
	/**
	 * IO daemons
	 */
	public String seioDaemons;
	
	/**
	 * SE storage path
	 */
	public String seStoragePath;
	
	/**
	 * SE used space
	 */
	public long seUsedSpace;
	
	/**
	 * Number of files
	 */
	public long seNumFiles;
	
	/**
	 * Minimum size
	 */
	public long seMinSize;
	
	/**
	 * SE type
	 */
	public String seType;
	
	/**
	 * Access restricted to this users
	 */
	public Set<String> exclusiveUsers;
	
	/**
	 * Exclusive write
	 */
	public String seExclusiveWrite;
	
	/**
	 * Exclusive read
	 */
	public String seExclusiveRead;
	
	/**
	 * @param db
	 */
	SE(final DBFunctions db){
		seName = db.gets("seName");
		
		seNumber = db.geti("seNumber");
		
		qos = parseArray(db.gets("seQoS"));
		
		seioDaemons = db.gets("seioDaemons");
		
		seStoragePath = db.gets("seStoragePath");
		
		seUsedSpace = db.getl("seUsedSpace");
		
		seNumFiles = db.getl("seNumFiles");
		
		seMinSize = db.getl("seMinSize");
		
		seType = db.gets("seType");
		
		exclusiveUsers = parseArray("exclusiveUsers");
		
		seExclusiveRead = db.gets("seExclusiveRead");
		
		seExclusiveWrite = db.gets("seExclusiveWrite");
	}
	
	/**
	 * @param s
	 * @return the set of elements
	 */
	public static Set<String> parseArray(final String s){
		if (s==null)
			return null;
		
		final Set<String> ret = new HashSet<String>();
		
		final StringTokenizer st = new StringTokenizer(s, ",");
		
		while (st.hasMoreTokens()){
			final String tok = st.nextToken().trim();
			
			if (tok.length()>0)
				ret.add(tok);
		}
		
		return Collections.unmodifiableSet(ret);
	}
}
