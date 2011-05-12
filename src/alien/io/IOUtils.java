package alien.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.List;
import java.util.Set;

import lazyj.Utils;
import alien.catalogue.GUID;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.io.protocols.Protocol;

/**
 * Helper functions for IO
 * 
 * @author costing
 */
public class IOUtils {

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
		final Set<PFN> pfns = guid.getPFNs();
		
		if (pfns==null || pfns.size()==0)
			return null;
		
		for (final PFN pfn: pfns){
			Set<PFN> realPfns = pfn.getRealPFNs();
			
			if (realPfns==null || realPfns.size()==0)
				continue;
			
			for (final PFN realPfn: realPfns){
				final List<Protocol> protocols = Transfer.getAccessProtocols(realPfn);
			
				if (protocols==null || protocols.size()==0)
					continue;
			
				// request access to this file
				final String reason = AuthorizationFactory.fillAccess(pfn, AccessType.READ);
			
				if (reason!=null){
					// we don't have access to this file
					continue;
				}
			
				for (final Protocol protocol: protocols){
					try{
						final File f = protocol.get(pfn, null);
					
						return f;
					}
					catch (IOException e){
						// ignore
					}
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
		final File f = get(guid);
		
		if (f!=null){
			try{
				return Utils.readFile(f.getCanonicalPath());
			}
			catch (IOException ioe){
				// ignore, shouldn't be ...
			}
			finally{
				f.delete();
			}
		}
		
		return null;
	}
	
}
