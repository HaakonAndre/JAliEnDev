package alien.catalogue;

import lazyj.DBFunctions;
import lazyj.ExtProperties;
import alien.config.ConfigUtils;

/**
 * One row from alice_users.HOSTS
 * 
 * @author costing
 * @since Nov 4, 2010
 */
public class Host {
	/**
	 * host index
	 */
	public int hostIndex;
	
	/**
	 * machine name:port 
	 */
	public String address;
	
	/**
	 * database name
	 */
	public String db;
	
	/**
	 * driver 
	 */
	public String driver;
	
	/**
	 * organization
	 */
	public String organization;
	
	private ExtProperties dbProperties = null;
	
	/**
	 * Initialize from one row extracted from alice_users.HOSTS
	 * 
	 * @param db
	 * @see CatalogueUtils#getHost(int)
	 */
	Host(final DBFunctions db){
		hostIndex = db.geti("hostIndex");
		address = db.gets("address");
		this.db = db.gets("db");
		driver = db.gets("driver").toLowerCase();
		organization = db.gets("organization");
		
		final ExtProperties parent = ConfigUtils.getDBConfiguration().get("alice_users");
		
		if (parent!=null)
			dbProperties = new ExtProperties(parent.getProperties());
		else
			dbProperties = new ExtProperties();
		
		dbProperties.set("database", this.db);
		dbProperties.set("host", address.substring(0, address.indexOf(':')));
		dbProperties.set("port", address.substring(address.indexOf(':')+1));
		
		if (driver.indexOf("mysql")>=0)
			dbProperties.set("driver", "com.mysql.jdbc.Driver");
		else
		if (driver.indexOf("postgres")>=0)
			dbProperties.set("driver", "org.postgresql.Driver");
	}

	/**
	 * Get a database connection to this host
	 * 
	 * @return a database connection to this host
	 */
	public DBFunctions getDB(){
		return new DBFunctions(dbProperties);
	}	
}