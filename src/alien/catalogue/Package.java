package alien.catalogue;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import lazyj.StringFactory;
import alien.config.ConfigUtils;


/**
 * @author ron
 * @since Nov 23, 2011
 */
public class Package implements Comparable<Package>, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1858434456566977987L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(Package.class.getCanonicalName());
	
	/**
	 * Whether or not this entry really exists in the catalogue
	 */
	public boolean exists = false;
		
	/**
	 * packageVersion
	 */
	public String packageVersion;
	
	/**
	 * packageName
	 */
	public String packageName;
	
	/**
	 * user
	 */
	public String user;	
	
	/**
	 * Platform - to - LFN mapping
	 */
	private final Map<String, String> platforms = new HashMap<String, String>();
		
	/**
	 * @param db
	 */
	public Package(final DBFunctions db){
		init(db);
		
		hashCodeValue = getFullName().hashCode();
	}
	
	private final int hashCodeValue;
	
	@Override
	public int hashCode() {
		return hashCodeValue;
	}
	
	private void init(final DBFunctions db){
		exists = true;
		
		packageVersion = StringFactory.get(db.gets("packageVersion"));

		packageName = StringFactory.get(db.gets("packageName"));

		user = StringFactory.get(db.gets("username"));
	}
	
	@Override
	public String toString() {
		return getFullName()+": "+platforms; 
	}
	
	
	/**
	 * @return the full package name
	 */
	public String getFullName(){
		return user+"@"+packageName+"::"+packageVersion;
	}

	/**
	 * @return the package version
	 */
	public String getVersion(){
		return packageVersion;
	}

	/**
	 * @return the package (short) name
	 */
	public String getName(){
		return packageName;
	}
	
	/**
	 * @return the user/owner of the package
	 */
	public String getUser(){
		return user;
	}

	/**
	 * @param platform 
	 * @return the LFN name of the package for the given platform
	 */
	public String getLFN(final String platform){
		return platforms.get(platform);
	}

	/**
	 * @return the available platforms
	 */
	public Set<String> getPlatforms(){
		return platforms.keySet();
	}
	
	/**
	 * @param platform
	 * @return <code>true</code> if this package is available for the given platform
	 */
	public boolean isAvailable(final String platform){
		return platforms.containsKey(platform);
	}
	
	/**
	 * Set the known package locations
	 * 
	 * @param platform
	 * @param lfn
	 */
	void setLFN(final String platform, final String lfn){
		platforms.put(platform, lfn);
	}
	
	@Override
	public boolean equals(final Object obj) {
		if (obj==null || !(obj instanceof Package))
			return false;

		if (this==obj)
			return true;

		final Package other = (Package) obj;		
		
		return compareTo(other)==0;
	}
	
	@Override
	public int compareTo(final Package arg0) {
		return getFullName().compareTo(arg0.getFullName());
	}
}
