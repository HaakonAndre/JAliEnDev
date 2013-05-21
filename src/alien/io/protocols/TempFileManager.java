/**
 * 
 */
package alien.io.protocols;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import lazyj.LRUMap;
import alien.catalogue.GUID;
import alien.config.ConfigUtils;
import alien.io.IOUtils;

/**
 * @author costing
 * @since Nov 14, 2011
 */
public class TempFileManager extends LRUMap<GUID, File>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6481164994092554757L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(TempFileManager.class.getCanonicalName());
	
	private final long totalSizeLimit;
	
	private long currentSize = 0;
	
	private final boolean delete;
	
	private TempFileManager(final int entriesLimit, final long sizeLimit, final boolean delete) {
		super(entriesLimit);

		this.totalSizeLimit = sizeLimit;
		
		this.delete = delete;
	}

	private static final TempFileManager tempInstance = new TempFileManager(ConfigUtils.getConfig().geti("alien.io.protocols.TempFileManager.temp.entries", 50), ConfigUtils.getConfig().getl("alien.io.protocols.TempFileManager.temp.size", 10*1024*1024*1024L), true);
	private static final TempFileManager persistentInstance = new TempFileManager(ConfigUtils.getConfig().geti("alien.io.protocols.TempFileManager.persistent.entries", 100), 0, false);
	
	private static List<File> lockedLocalFiles = new LinkedList<File>();
	
	/* (non-Javadoc)
	 * @see lazyj.LRUMap#removeEldestEntry(java.util.Map.Entry)
	 */
	@Override
	protected boolean removeEldestEntry(final java.util.Map.Entry<GUID, File> eldest) {
		final boolean wasLocked = isLocked(eldest.getValue());
		
		final boolean remove = !eldest.getValue().exists() || (!wasLocked && (super.removeEldestEntry(eldest) || (totalSizeLimit>0 && currentSize > totalSizeLimit)));
	
		if (logger.isLoggable(Level.FINEST)){
			logger.log(Level.FINEST, "Decision on ('"+eldest.getValue().getAbsolutePath()+"'): "+remove+", count: "+size()+" / "+getLimit()+", size: "+currentSize+" / "+totalSizeLimit+", locked: "+wasLocked);
		}
		
		if (remove){
			currentSize -= eldest.getKey().size;
			
			if (delete){
				if (eldest.getValue().exists()){
					if (!eldest.getValue().delete()){
						logger.log(Level.WARNING, "Could not delete temporary file "+eldest.getValue());
					}
				}
				else{
					logger.log(Level.FINE, "Somebody has already deleted "+eldest.getValue()+" while its lock status was: "+wasLocked);
				}
					
				release(eldest.getValue());
			}
		}
		
		return remove;
	}
	
	/* (non-Javadoc)
	 * @see java.util.HashMap#put(java.lang.Object, java.lang.Object)
	 */
	@Override
	public File put(final GUID key, final File value) {
		if (delete)
			value.deleteOnExit();
		
		currentSize += key.size;
		
		return super.put(key, value);
	}
	
	/**
	 * @param key
	 * @return the cached file
	 */
	public static File getAny(final GUID key){
		File f;
		
		synchronized (tempInstance){
			f = tempInstance.get(key);
		}
		
		if (f!=null && f.exists() && f.isFile() && f.canRead()){
			lock(f);
			
			return f;
		}
		
		synchronized (persistentInstance) {
			f = persistentInstance.get(key);
		}
		
		try {
			if (f!=null && f.exists() && f.isFile() && f.canRead() && f.length()==key.size && IOUtils.getMD5(f).equalsIgnoreCase(key.md5))
				return f;
		}
		catch (IOException e) {
			return null;
		}
		
		return null;
	}
	
	/**
	 * @param key
	 * @param localFile
	 */
	public static void putTemp(final GUID key, final File localFile){
		synchronized (tempInstance){
			tempInstance.put(key, localFile);
		}
		
		lock(localFile);
	}
	
	/**
	 * Re-pin a temporary file on disk until its processing is over.
	 * 
	 * @param f
	 * @see #release(File)
	 */
	private static void lock(final File f){
		synchronized (lockedLocalFiles){			
			lockedLocalFiles.add(f);
		}
		
		if (logger.isLoggable(Level.FINEST)){
			try{
				throw new IOException();
			}
			catch (IOException ioe){
				logger.log(Level.FINEST, f.getAbsolutePath()+" locked by", ioe);
			}
		}
	}
	
	/**
	 * For temporary downloaded files, call this when you are done working with the local file to allow the garbage collector to reclaim the space when needed.
	 * 
	 * @param f
	 */
	public static void release(final File f){
		synchronized (lockedLocalFiles){
			lockedLocalFiles.remove(f);
		}
	}
	
	/**
	 * @param f
	 * @return <code>true</code> if the file is locked
	 */
	public static boolean isLocked(final File f){
		synchronized (lockedLocalFiles) {
			return lockedLocalFiles.contains(f);
		}
	}
	
	/**
	 * @return currently locked files, for debugging purposes
	 */
	public static List<File> getLockedFiles(){
		synchronized (lockedLocalFiles){
			return new ArrayList<File>(lockedLocalFiles);
		}
	}
	
	/**
	 * @param key
	 * @param localFile
	 */
	public static void putPersistent(final GUID key, final File localFile){
		synchronized (persistentInstance){
			persistentInstance.put(key, localFile);
		}
	}
}
