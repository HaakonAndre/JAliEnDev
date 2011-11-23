package alien.catalogue;

import java.io.Serializable;
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
	 * fullPackageName
	 */
	public String fullPackageName;
	
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
	 * platform
	 */
	public String platform;
	
	/**
	 * lfn
	 */
	public String lfn;
	
	/**
	 * size
	 */
	public long size;
	
	
	/**
	 * @param db
	 */
	public Package(final DBFunctions db){
		init(db);
		
	}
	
	@Override
	public int hashCode() {
		// TODO
		return 0;
	}
	
	private void init(final DBFunctions db){
		
		exists = true;
		
		fullPackageName = StringFactory.get(db.gets("fullPackageName"));

		packageVersion = StringFactory.get(db.gets("packageVersion"));

		packageName = StringFactory.get(db.gets("packageName"));

		user = StringFactory.get(db.gets("username"));

		platform = StringFactory.get(db.gets("platform"));
		
		lfn = StringFactory.get(db.gets("lfn"));
		
		size = db.getl("size");
		
	}
	
	@Override
	public String toString() {
		return "Package fullPackageName\t: "+fullPackageName+"\n"+
				"Package packageVersion\t: "+packageVersion+"\n"+
				"Package packageName\t: "+packageName+"\n"+
				"Package username\t: "+user+"\n"+
				"Package platform\t: "+platform+"\n"+
				"Package lfn\t: "+lfn+"\n"+
				"Package size\t: "+size+"\n"
		       ;
	}
	
	
	/**
	 * @return the full package name
	 */
	public String getFullName(){
		return fullPackageName;
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
	 * @return the platform of the package
	 */
	public String getPlatform(){
		return platform;
	}

	/**
	 * @return the LFN name of the package
	 */
	public String getLFNName(){
		return lfn;
	}

	/**
	 * @return the size of the package
	 */
	public long getSize(){
		return size;
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
	public int compareTo(Package arg0) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	
	
}
