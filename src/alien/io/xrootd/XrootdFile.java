package alien.io.xrootd;

import java.util.Date;
import java.util.StringTokenizer;

import lazyj.Format;

/**
 * @author costing
 *
 */
public class XrootdFile implements Comparable<XrootdFile>{
	/**
	 * entry permissions
	 */
	public final String perms;
	
	/**
	 * size
	 */
	public final long size;
	
	/**
	 * change time
	 */
	public final Date date;
	
	/**
	 * full path
	 */
	public final String path;
	
	/**
	 * parse the output of "ls", "dirlist" or "dirlistrec" and extract the tokens
	 * 
	 * @param line
	 * @throws IllegalArgumentException 
	 */
	public XrootdFile(final String line) throws IllegalArgumentException{
		StringTokenizer st = new StringTokenizer(line);
		
		if (st.countTokens()!=5)
			throw new IllegalArgumentException("Not in the correct format : "+line);
		
		perms = st.nextToken();
		
		size = Long.parseLong(st.nextToken());
		
		date = Format.parseDate(st.nextToken()+" "+st.nextToken());
		
		path = st.nextToken();
	}
	
	/**
	 * @return true if dir
	 */
	public boolean isDirectory(){
		return perms.startsWith("d");
	}
	
	/**
	 * @return true if file
	 */
	public boolean isFile(){
		return perms.startsWith("-");
	}
	
	/**
	 * @return the last token of the path
	 */
	public String getName(){
		int idx = path.lastIndexOf('/');
		
		if (idx>=0)
			return path.substring(idx+1);
		
		return path;
	}

	@Override
	public int compareTo(final XrootdFile o) {
		final int diff = perms.compareTo(o.perms);
		
		if (diff!=0)
			return diff;
		
		return path.compareTo(o.path);
	}
	
	@Override
	public int hashCode() {
		return path.hashCode();
	}
	
	@Override
	public String toString() {
		return path;
	}
}
