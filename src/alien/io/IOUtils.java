package alien.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

import lazyj.Utils;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.io.protocols.Protocol;
import alien.io.protocols.TempFileManager;
import alien.se.SEUtils;
import alien.shell.commands.JAliEnCOMMander;
import alien.shell.commands.JAliEnCommandcp;
import alien.shell.commands.PlainWriter;
import alien.shell.commands.UIPrintWriter;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;

/**
 * Helper functions for IO
 * 
 * @author costing
 */
public class IOUtils {
	
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(IOUtils.class.getCanonicalName());

	/**
	 * @param f
	 * @return the MD5 checksum of the entire file
	 * @throws IOException
	 */
	public static String getMD5(final File f) throws IOException{
		if (f==null || !f.isFile() || !f.canRead())
			throw new IOException("Cannot read from this file: "+f);
		
		DigestInputStream dis = null;
		
		try{
			final MessageDigest md = MessageDigest.getInstance("MD5");
			dis = new DigestInputStream(new FileInputStream(f), md);
			
			final byte[] buff = new byte[10240];
			
			int cnt;
			
			do{
				cnt = dis.read(buff);
			}
			while (cnt==buff.length);
			
			final byte[] digest = md.digest();
			
			return String.format("%032x", new BigInteger(1, digest));
		}
		catch (IOException ioe){
			throw ioe;
		}
		catch (Exception e){
			// ignore
		}
		finally{
			if (dis!=null){
				try{
					dis.close();
				}
				catch (IOException ioe){
					// ignore
				}
			}
		}
		
		return null;
	}

	/**
	 * Download the file in a temporary location
	 * 
	 * @param guid
	 * @return the temporary file name. You should handle the deletion of this temporary file!
	 * @see TempFileManager#release(File)
	 */
	public static File get(final GUID guid){
		return get(guid, null);
	}
	
	/**
	 * Download the file in a specified location
	 * 
	 * @param guid
	 * @param localFile path where the file should be downloaded. Can be <code>null</code> in which case a temporary location will be used
	 * @return the downloaded file, or <code>null</code> if the file could not be retrieved
	 */
	public static File get(final GUID guid, final File localFile){
		final File cachedContent = TempFileManager.getAny(guid);
		
		if (cachedContent!=null){
			if (localFile==null)
				return cachedContent;

			try{
				if (!Utils.copyFile(cachedContent.getAbsolutePath(), localFile.getAbsolutePath())){
					if (logger.isLoggable(Level.WARNING))
						logger.log(Level.WARNING, "Cannot copy "+cachedContent.getAbsolutePath()+" to "+localFile.getAbsolutePath());
					
					return null;
				}
			}
			finally{
				TempFileManager.release(cachedContent);
			}
			
			TempFileManager.putPersistent(guid, localFile);
			
			return localFile;
		}
		
		final Set<PFN> pfns = guid.getPFNs();
		
		if (pfns==null || pfns.size()==0)
			return null;
		
		final Set<PFN> realPFNsSet = new HashSet<PFN>();
		
		boolean zipArchive = false;
		
		for (final PFN pfn: pfns){
			if (pfn.pfn.startsWith("guid:/") && pfn.pfn.indexOf("?ZIP=")>=0)
				zipArchive = true;
			
			final Set<PFN> realPfnsTemp = pfn.getRealPFNs();
			
			if (realPfnsTemp==null || realPfnsTemp.size()==0)
				continue;
			
			for (final PFN realPFN: realPfnsTemp)
				realPFNsSet.add(realPFN);
		}
		
		final String site = ConfigUtils.getConfig().gets("alice_close_site", "CERN").trim();
		
		final List<PFN> sortedRealPFNs = SEUtils.sortBySite(realPFNsSet, site, false, false);
		
		File f = null;
			
		for (final PFN realPfn: sortedRealPFNs){
			if (realPfn.ticket==null){
				System.err.println("Missing ticket for "+realPfn.pfn);
				continue;	// no access to this guy
			}
				
			final List<Protocol> protocols = Transfer.getAccessProtocols(realPfn);
			
			if (protocols==null || protocols.size()==0)
				continue;
			
			for (final Protocol protocol: protocols){
				try{
					System.err.println("Trying protocol "+protocol+" for getting "+realPfn+" to "+(zipArchive ? null : localFile));
					
					f = protocol.get(realPfn, zipArchive ? null : localFile);
				
					if (f!=null)
						break;
				}
				catch (IOException e){
					System.err.println(e.getMessage());
				}
			}
		}
		
		if (f==null || !zipArchive)
			return f;
		
		try{
			for (final PFN p: pfns){
				if (p.pfn.startsWith("guid:/") && p.pfn.indexOf("?ZIP=")>=0){
					// this was actually an archive
					
					final String archiveFileName = p.pfn.substring(p.pfn.lastIndexOf('=')+1);
					
					try{
						final ZipInputStream zi = new ZipInputStream(new FileInputStream(f));
					
						ZipEntry zipentry = zi.getNextEntry();
						
						File target = null;
						
			            while (zipentry != null){
			            	if (zipentry.getName().equals(archiveFileName)){
			            		if (localFile!=null){
			            			target = localFile;
			            		}
			            		else{
			            			target = File.createTempFile(guid.guid+"#"+archiveFileName+".", null, getTemporaryDirectory());
			            		}
			            		
			            		final FileOutputStream fos = new FileOutputStream(target);
			            		
			            		final byte[] buf = new byte[8192];
			            		
			            		int n;
			            		
			            		while ((n = zi.read(buf, 0, buf.length)) > -1)
			                        fos.write(buf, 0, n);
			            		
			            		fos.close();
			            		zi.closeEntry();
			            		break;
			            	}
			            	
			            	zipentry = zi.getNextEntry();
			            }
			            
			            zi.close();
			            
			            if (target!=null){
			            	if (localFile==null)
			            		TempFileManager.putTemp(guid, target);
			            	else
			            		TempFileManager.putPersistent(guid, localFile);
			            }
			            
			            return target;
					}
					catch (ZipException e){
						logger.log(Level.WARNING, "ZipException parsing the content of "+f.getAbsolutePath(), e);
					}
					catch (IOException e){
						logger.log(Level.WARNING, "IOException extracting "+archiveFileName+" from "+f.getAbsolutePath()+" to parse as ZIP", e);
					}
					
					return null;
				}
			}
		}
		finally{
			TempFileManager.release(f);
		}
		
		return null;
	}
	
	/**
	 * @param guid
	 * @return the contents of the file, or <code>null</code> if there was a problem getting it
	 */
	public static String getContents(final GUID guid) {
		final String reason = AuthorizationFactory.fillAccess(guid, AccessType.READ);
		
		if (reason!=null){
			logger.log(Level.WARNING, "Access denied: "+reason);
			
			return null;
		}
		
		final File f = get(guid);
		
		if (f!=null){
			try{
				return Utils.readFile(f.getCanonicalPath());
			}
			catch (final IOException ioe){
				// ignore, shouldn't be ...
			}
			finally{
				TempFileManager.release(f);
			}
		}
		
		return null;
	}
	
	/**
	 * @param lfn
	 * @return the contents of the file, or <code>null</code> if there was a problem getting it
	 */
	public static String getContents(final LFN lfn){
		if (lfn==null)
			return null;
		
		final GUID g = GUIDUtils.getGUID(lfn);
		
		if (g==null)
			return null;
		
		return getContents(g);
	}
	
	/**
	 * @param lfn
	 * @return the contents of the file, or <code>null</code> if there was a problem getting it
	 */
	public static String getContents(final String lfn){
		return getContents(LFNUtils.getLFN(lfn));
	}

	/**
	 * @param lfn relative paths are allowed
	 * @param owner
	 * @return <code>true</code> if the indicated LFN doesn't exist (any more) in the catalogue and can be created again
	 */
	public static boolean backupFile(final String lfn, final AliEnPrincipal owner){
    	final String absolutePath = FileSystemUtils.getAbsolutePath(owner.getName(), null, lfn);
    	
    	final LFN l = LFNUtils.getLFN(absolutePath, true);
    
    	if (!l.exists){
    		return true;
    	}
    	
    	final LFN backupLFN = LFNUtils.getLFN(absolutePath+"~", true);
    	
    	if (backupLFN.exists && AuthorizationChecker.canWrite(backupLFN.getParentDir(), owner)){
    		if (!backupLFN.delete(true, false))
    			return false;
    	}
    	
    	return LFNUtils.mvLFN(owner, l, absolutePath+"~")!=null;
	}
	
	/**
	 * Upload a local file to the Grid
	 * 
	 * @param localFile
	 * @param toLFN
	 * @param owner
	 * @throws IOException
	 */
	public static void upload(final File localFile, final String toLFN, final AliEnPrincipal owner) throws IOException{
		upload(localFile, toLFN, owner, 2, null);
	}
	
	/**
	 * Upload a local file to the Grid
	 * 
	 * @param localFile
	 * @param toLFN
	 * @param owner
	 * @param replicaCount
	 * @throws IOException
	 */
	public static void upload(final File localFile, final String toLFN, final AliEnPrincipal owner, final int replicaCount) throws IOException{
		upload(localFile, toLFN, owner, replicaCount, null);
	}
	
	/**
	 * Upload a local file to the Grid
	 * 
	 * @param localFile
	 * @param toLFN
	 * @param owner
	 * @param replicaCount
	 * @param progressReport
	 * @throws IOException
	 */
	public static void upload(final File localFile, final String toLFN, final AliEnPrincipal owner, final int replicaCount, final OutputStream progressReport) throws IOException{
    	final String absolutePath = FileSystemUtils.getAbsolutePath(owner.getName(), null, toLFN);
    	
    	final LFN l = LFNUtils.getLFN(absolutePath, true);
    	
    	if (l.exists){
    		throw new IOException("LFN already exists: "+toLFN);
    	}
    	
    	final ArrayList<String> cpArgs = new ArrayList<String>();
    	cpArgs.add("file:"+localFile.getAbsolutePath());
    	cpArgs.add(absolutePath);
    	cpArgs.add("-S");
    	cpArgs.add("disk:"+replicaCount);

    	final UIPrintWriter out = progressReport!=null ? new PlainWriter(progressReport) : null;
    	
    	final JAliEnCOMMander cmd = new JAliEnCOMMander(owner, owner.getName(), null,  ConfigUtils.getConfig().gets("alice_close_site", "CERN").trim(), out);
    	
    	final JAliEnCommandcp cp = new JAliEnCommandcp(cmd, out, cpArgs);
    	
    	cp.copyLocalToGrid(localFile, absolutePath);
	}
	
	/**
	 * @return the temporary directory where downloaded files are put by default
	 */
	public static final File getTemporaryDirectory(){
		final String sDir = ConfigUtils.getConfig().gets("alien.io.IOUtils.tempDownloadDir");
		
		if (sDir==null || sDir.length()==0)
			return null;
		
		final File f = new File(sDir);
		
		if (f.exists() && f.isDirectory() && f.canWrite())
			return f;
		
		return null;
	}
}
