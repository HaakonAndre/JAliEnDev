package alien.catalogue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashSet;

import lazyj.Format;
import lazyj.Utils;
import alien.io.IOUtils;

/**
 * XML collection wrapper
 * 
 * @author costing
 * @since 2012-02-15
 */
public class XmlCollection extends LinkedHashSet<LFN>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 3567611755762002384L;

	/**
	 * Empty collection to start with
	 */
	public XmlCollection(){
		// empty collection
	}
	
	/**
	 * Read the content of a local XML file
	 * 
	 * @param localFile
	 * @throws IOException 
	 */
	public XmlCollection(final File localFile) throws IOException{
		this(Utils.readFile(localFile.getAbsolutePath()));
	}
	
	/**
	 * Parse this XML
	 * 
	 * @param content
	 * @throws IOException 
	 */
	public XmlCollection(final String content) throws IOException {
		parseXML(content);
	}
	
	/**
	 * read the contents of a LFN in the catalogue
	 * 
	 * @param lfn
	 * @throws IOException
	 */
	public XmlCollection(final LFN lfn) throws IOException {
		if (lfn==null || !lfn.isFile())
			throw new IOException("LFN is not readable");
		
		parseXML(IOUtils.getContents(lfn));
	}
	
	/**
	 * read the contents of a GUID in the catalogue
	 * 
	 * @param guid
	 * @throws IOException
	 */
	public XmlCollection(final GUID guid) throws IOException {
		if (guid==null)
			throw new IOException("GUID is not readable");

		parseXML(IOUtils.getContents(guid));
	}
	
	private void parseXML(final String content) throws IOException {
		final BufferedReader br = new BufferedReader(new StringReader(content));
		
		String sLine;
		
		while ( (sLine=br.readLine()) != null ){
			sLine = sLine.trim();
			
			if (!sLine.startsWith("<file name=\""))
				continue;
			
			final int idx = sLine.indexOf("\" lfn=\"/");
			
			if (idx<0)
				continue;
			
			try{
				final String fileName = sLine.substring(idx+7, sLine.indexOf('"', idx+8));

				add(LFNUtils.getLFN(fileName));
			}
			catch (final Throwable t){
				throw new IOException("Exception parsing XML", t);
			}
		}
	}
	
	private String collectionName;
	
	/**
	 * Set collection name
	 * 
	 * @param newName
	 */
	public void setName(final String newName){
		collectionName = newName;
	}
	
	/**
	 * @return the collection name
	 */
	public String getName(){
		return collectionName;
	}
	
	private String owner;
	
	/**
	 * Set ownership
	 * 
	 * @param newOwner
	 */
	public void setOwner(final String newOwner){
		owner = newOwner;
	}
	
	/**
	 * @return currently set owner
	 */
	public String getOwner(){
		return owner;
	}
	
	private String command;
	
	/**
	 * command that has generated this collection
	 * 
	 * @param newCommand
	 */
	public void setCommand(final String newCommand){
		command = newCommand;
	}
	
	/**
	 * @return the command that has generated this collection
	 */
	public String getCommand(){
		return command;
	}
	
	private static final SimpleDateFormat ALIEN_TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	
	private static String formatTimestamp(final Date d){
		if (d==null)
			return "";
		
		synchronized (ALIEN_TIME_FORMAT){
			return ALIEN_TIME_FORMAT.format(d);
		}
	}
	
	private static String getXMLPortion(final LFN l){
		return "      <file name=\""+Format.escHtml(l.getFileName())+"\" "+
	       "aclId=\""+(l.aclId>0 ? String.valueOf(l.aclId) : "") +"\" "+
		   "broken=\""+(l.broken ? 1 : 0)+"\" "+
	       "ctime=\""+formatTimestamp(l.ctime)+"\" "+
		   "dir=\""+l.dir+"\" "+
	       "entryId=\""+l.entryId+"\" "+
		   "expiretime=\""+formatTimestamp(l.expiretime)+"\" "+
	       "gowner=\""+Format.escHtml(l.gowner)+"\" "+
		   "guid=\""+l.guid.toString()+"\" "+
	       "guidtime=\"\" "+
		   "jobid=\""+(l.jobid>0 ? String.valueOf(l.jobid) : "")+"\" "+
	       "lfn=\""+l.getCanonicalName()+"\" "+
		   "md5=\""+Format.escHtml(l.md5)+"\" "+
	       "owner=\""+Format.escHtml(l.owner)+"\" "+
		   "perm=\""+Format.escHtml(l.perm)+"\" "+
	       "replicated=\""+(l.replicated ? 1 : 0)+"\" "+
		   "size=\""+l.size+"\" "+
	       "turl=\"alien://"+Format.escHtml(l.getCanonicalName())+"\" "+
		   "type=\""+l.type+"\" />";
	}
	
	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("<?xml version=\"1.0\"?>").append('\n');
		sb.append("<alien>\n");
		sb.append("  <collection name=\""+Format.escHtml(collectionName!=null && collectionName.length()>0 ? collectionName : "tempCollection")+"\">\n");
		
		int iCount = 0;
		
		for (final LFN l : this){
			final String sXML = getXMLPortion(l);
				
			if (sXML!=null){
				iCount ++;
				
				sb.append("    <event name=\""+iCount+"\">\n");
				sb.append(sXML).append('\n');
				sb.append("    </event>\n");
			}
		}
		
		final long lNow = System.currentTimeMillis();
		
	    sb.append("    <info command=\""+Format.escHtml(command!=null ? command : "alien.catalogue.XmlCollection")+"\" creator=\""+Format.escHtml(owner!=null ? owner : "JAliEn-Central")+"\" date=\"").append(new Date(lNow)).append("\" timestamp=\"").append(lNow).append("\" />\n");
	    sb.append("  </collection>\n");                                                                                                                                                  
	    sb.append("</alien>");       
	        
		return sb.toString();
	}
}
