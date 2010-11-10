/**
 * 
 */
package alien.config;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import lazyj.ExtProperties;
import lazyj.cache.ExpirationCache;

/**
 * @author costing
 * @since Nov 3, 2010
 */
public class ConfigUtils {
	private static ExpirationCache<String, String> seenLoggers = new ExpirationCache<String, String>();
	
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(ConfigUtils.class.getCanonicalName());
	
	private static final Map<String, ExtProperties> dbConfigFiles;
	
	private static final Map<String, ExtProperties> otherConfigFiles;
	
	private static final String CONFIG_FOLDER;
	
	private static LoggingConfigurator logging = null;
	
	static {
		CONFIG_FOLDER = System.getProperty("AliEnConfig", "config");
		
		final File f = new File(CONFIG_FOLDER);

		final HashMap<String, ExtProperties> dbconfig = new HashMap<String, ExtProperties>();
		
		final HashMap<String, ExtProperties> otherconfig = new HashMap<String, ExtProperties>();
		
		if (f.exists() && f.isDirectory() && f.canRead()){
			final File[] list = f.listFiles();
			
			if (list!=null){
				for (final File sub: list){
					if (sub.isFile() && sub.canRead() && sub.getName().endsWith(".properties")){
						String sName = sub.getName();
						sName = sName.substring(0, sName.lastIndexOf('.'));
						
						final ExtProperties prop = new ExtProperties(CONFIG_FOLDER, sName);
						
						if (sName.equals("logging")){
							logging = new LoggingConfigurator(prop);
						}
						else
						if (prop.gets("driver").length()>0){
							dbconfig.put(sName, prop);
						}
						else
							otherconfig.put(sName, prop);
					}
				}
			}
		}
		
		dbConfigFiles = Collections.unmodifiableMap(dbconfig);
		
		otherConfigFiles = Collections.unmodifiableMap(otherconfig);
		
		if (logging == null){
			final ExtProperties prop = new ExtProperties();
			
	        prop.set("handlers", "java.util.logging.ConsoleHandler");                                                                                                                                     
	        prop.set("java.util.logging.ConsoleHandler.level", "FINEST");                                                                                                                                 
	        prop.set(".level", "INFO");                                                                                                                                                                   
	        prop.set("java.util.logging.ConsoleHandler.formatter", "java.util.logging.SimpleFormatter");
	        
	        logging = new LoggingConfigurator(prop);
		}
	}
	
	/**
	 * Get all database-related configuration files
	 * 
	 * @return db configurations
	 */
	public static final Map<String, ExtProperties> getDBConfiguration(){
		return dbConfigFiles;
	}
	
	/**
	 * Get a DB connection to a specific database key. The code relies on the <i>AlienConfig</i> system property to point to
	 * a base directory where files named <code>key</code>.properties can be found. If a file for this key can be found it is
	 * returned to the caller, otherwise a <code>null</code> value is returned. 
	 * 
	 * @param key database class, something like &quot;catalogue_admin&quot;
	 * @return the database connection, or <code>null</code> if it is not available.
	 */
	public static final DBFunctions getDB(final String key){
		final ExtProperties p = dbConfigFiles.get(key);
		
		if (p==null)
			return null;
		
		return new DBFunctions(p);
	}
	
	/**
	 * Get the contents of the configuration file indicated by the key
	 * 
	 * @param key
	 * @return configuration contents
	 */
	public static final ExtProperties getConfiguration(final String key){
		return otherConfigFiles.get(key);
	}
	
	/**
	 * Set the Java logging properties and subscribe to changes on the configuration files
	 * 
	 * @author costing
	 * @since Nov 3, 2010
	 */
	static class LoggingConfigurator implements Observer {
		private final ExtProperties prop;
		
		/**
		 * Set the logging configuration
		 * 
		 * @param p
		 */
		LoggingConfigurator(final ExtProperties p) {
			prop = p;
			
			prop.addObserver(this);
			
			update(null, null);
		}
		
		@Override
		public void update(final Observable o, final Object arg) {
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			
			try {                                                                                                                                                                                                  
	            prop.getProperties().store(baos, "AliEn Loggging Properties");                                                                                                                                                       
	        }
			catch ( Throwable t ) {                                                                                                                                                                              
	            System.err.println("Cannot store default props");                                                                                                                                                  
	            t.printStackTrace();                                                                                                                                                                               
	        }
			
	        try { 
	        	baos.flush(); 
	        }
	        catch ( Exception ex ){
	        	// ignore
	        }
	        
	        final byte[] buff = baos.toByteArray();
	        
	        final ByteArrayInputStream bais = new ByteArrayInputStream(buff);
	        
	        try {                                                                                                                                                                                                  
	            LogManager.getLogManager().readConfiguration(bais);                                                                                                                                                
	        }
	        catch ( Throwable t ) {                                                                                                                                                                              
	            System.err.println("Cannot load default props into LogManager");                                                                                                                                   
	            t.printStackTrace();                                                                                                                                                                               
	        }
		}
	}
	
	/**
	 * Get the logger for this component
	 * 
	 * @param component
	 * @return the logger
	 */
	public static Logger getLogger(final String component){
		final Logger l = Logger.getLogger(component);
		
		final String s = seenLoggers.get(component);
		
		if (s==null && logging!=null){
			seenLoggers.put(component, component, 60*1000);
		
			logging.update(null, null);
		}
		
		return l;
	}
}
