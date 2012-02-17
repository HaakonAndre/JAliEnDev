package alien.io;

import java.io.File;
import java.io.FileInputStream;
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
		final Set<PFN> pfns = guid.getPFNs();
		
		if (pfns==null || pfns.size()==0)
			return null;
		
		final Set<PFN> realPFNsSet = new HashSet<PFN>();
		
		for (final PFN pfn: pfns){
			final Set<PFN> realPfnsTemp = pfn.getRealPFNs();
			
			if (realPfnsTemp==null || realPfnsTemp.size()==0)
				continue;
			
			for (PFN realPFN: realPfnsTemp)
				realPFNsSet.add(realPFN);
		}
		
		final String site = ConfigUtils.getConfig().gets("alice_close_site", "CERN").trim();
		
		final List<PFN> sortedRealPFNs = SEUtils.sortBySite(realPFNsSet, site, false);
			
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
					final File f = protocol.get(realPfn, localFile);
				
					return f;
				}
				catch (IOException e){
					// ignore
				}
			}
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
			catch (IOException ioe){
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
    		if (!backupLFN.delete())
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
}
