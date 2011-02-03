package alien.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.DigestInputStream;
import java.security.MessageDigest;

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
		
		try{
			final MessageDigest md = MessageDigest.getInstance("MD5");
			final DigestInputStream dis = new DigestInputStream(new FileInputStream(f), md);
			
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
		
		return null;
	}
	
}
