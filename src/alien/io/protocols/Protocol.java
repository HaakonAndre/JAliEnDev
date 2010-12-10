/**
 * 
 */
package alien.io.protocols;

import java.io.File;
import java.io.IOException;

import alien.catalogue.PFN;

/**
 * @author costing
 * @since Dec 8, 2010
 */
public abstract class Protocol {
	
	/**
	 * Package protected 
	 */
	Protocol(){
		// package protected
	}
	
	/**
	 * @param pfn pfn to delete
	 * @return <code>true</code> if the file was deleted, <code>false</code> if the file doesn't exist
	 * @throws IOException in case of access problems
	 */
	public boolean delete(final PFN pfn) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Download the file locally
	 * 
	 * @param pfn location
	 * @param localFile local file. Can be <code>null</code> to generate a temporary file name. If specified, it must point to a 
	 * 	file name that doesn't exist yet but can be created by this user. 
	 * @return local file, either the same that was passed or a temporary file name
	 * @throws IOException in case of problems
	 */
	public File get(final PFN pfn, final File localFile) throws IOException{
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Upload the local file
	 * 
	 * @param pfn target PFN
	 * @param localFile local file name (which must exist of course)
	 * @return storage reply envelope
	 * @throws IOException in case of problems
	 */
	public String put(final PFN pfn, final File localFile) throws IOException{
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Direct transfer between the two involved storage elements
	 * 
	 * @param source
	 * @param target
	 * @return storage reply envelope
	 * @throws IOException
	 */
	public String transfer(final PFN source, final PFN target) throws IOException{
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Check the consistency of the downloaded file
	 * 
	 * @param f
	 * @param pfn 
	 * @return <code>true</code> if the file matches catalogue information, <code>false</code> otherwise
	 */
	public static boolean checkDownloadedFile(final File f, final PFN pfn){
		if (f==null || !f.exists() || !f.isFile())
			return false;
		
		if (f.length() != pfn.getGuid().size)
			return false;
		
		return true;
	}
	
}
