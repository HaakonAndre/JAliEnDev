package alien.catalogue.access;

import java.io.Serializable;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lazyj.Format;

import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.se.SE;
import alien.se.SEUtils;


/**
 * @author ron
 *
 */
public class XrootDEnvelope implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1024787790575833398L;

	/**
	 * Format
	 */
	public static final String hashord = "turl-access-lfn-guid-se-size-md5";
	
	/**
	 * the access ticket this envelope belongs to
	 */
	public AccessType type = null;
	
	/**
	 * pfn of the file on the SE (proto:://hostdns:port//storagepath)
	 */
	public final PFN pfn;
	
	/**
	 * A LFN that is pointing to this envelope's GUID/PFN us as a guid:// archive link
	 */
	private LFN archiveAnchor;
	
	
	/**
	 * Signed envelope
	 */
	protected String signedEnvelope; 
	
	/**
	 * Encrypted envelope
	 */
	protected String encryptedEnvelope;

	/**
	 * @param type
	 * @param pfn 
	 */
	public XrootDEnvelope(final AccessType type, final PFN pfn){
		this.type = type;
		this.pfn = pfn;
	}

	/**
	 * Set the LFN that is pointing to this envelope's GUID/PFN us as a guid:// archive link
     */	
	public void setArchiveAnchor(LFN anchor){
		archiveAnchor = anchor;
	}

	/**
	 * @return envelope xml
	 */
	public String getUnEncryptedEnvelope() {

		final String access = type.toString().replace("write", "write-once");
		
		final String[] pfnsplit = pfn.getPFN().split("//");
		
		final GUID guid = pfn.getGuid();
		
		final Set<LFN> lfns = guid.getLFNs();
		
		final SE se = SEUtils.getSE(pfn.seNumber);
		
		String ret = "<authz>\n  <file>\n"
		+ "    <access>"+ access+"</access>\n";
		String turl = pfn.getPFN();
		if(archiveAnchor!=null)
			turl  += "#" + archiveAnchor.getFileName();
		ret += "    <turl>"+ Format.escHtml(turl)+ "</turl>\n";
		if(archiveAnchor!=null)
			ret += "    <lfn>"+Format.escHtml(archiveAnchor.getCanonicalName())+"</lfn>\n";
		else if (lfns!=null && lfns.size()>0)
			ret += "    <lfn>"+Format.escHtml(lfns.iterator().next().getCanonicalName())+"</lfn>\n";
		else
			ret += "    <lfn>/NOLFN</lfn>\n";
		
		ret += "    <size>"+guid.size+"</size>" + "\n"
		+ "    <pfn>"+Format.escHtml("/" + pfnsplit[2])+"</pfn>\n"
		+ "    <se>"+Format.escHtml(se.getName())+"</se>\n"
		+ "    <guid>"+Format.escHtml(guid.getName())+"</guid>\n"
		+ "    <md5>"+Format.escHtml(guid.md5)+"</md5>\n"
		+ "  </file>\n</authz>\n";
		
		return ret;
	}

	/**
	 * Splitter of PFNs
	 */
	public static final Pattern PFN_EXTRACT = Pattern.compile("^\\w+://([\\w-]+(\\.[\\w-]+)*(:\\d+))?/(.*)$");
	
	/**
	 * @return URL of the storage. This is passed as argument to xrdcp and in most cases it is the PFN but for 
	 * 			DCACHE it is a special path ...
	 */
	public String getTransactionURL() {
		final SE se = SEUtils.getSE(pfn.seNumber);
		
		if (se == null)
			return null;
		
		if (se.seName.indexOf("DCACHE") > 0){
			final GUID guid = pfn.getGuid();
			
			final Set<LFN> lfns = guid.getLFNs();
			
			if (lfns!=null && lfns.size()>0)
				return se.seioDaemons + "/" + lfns.iterator().next().getCanonicalName();
			
			return se.seioDaemons + "//NOLFN";
		}
		
		final Matcher m = PFN_EXTRACT.matcher(pfn.pfn);
		
		if (m.matches()){
			return se.seioDaemons + "/" + m.group(4);
		}

		return pfn.pfn;
	}

	/**
	 * @return url envelope
	 */
	public String getUnsignedEnvelope() {
				
		final GUID guid = pfn.getGuid();

		
		final Set<LFN> lfns = guid.getLFNs();
		
		final SE se = SEUtils.getSE(pfn.seNumber);
		
		String ret = "turl=" + pfn.getPFN();
		if(archiveAnchor!=null)
			ret = ret + "#" + archiveAnchor.getFileName();
		
		System.out.println("Decorating: " + pfn.getPFN());
		if(archiveAnchor!=null)
		System.out.println("Archive Link: " + archiveAnchor.getFileName());
		
		
		ret +=  "&access=" + type.toString();
		
		if(archiveAnchor!=null)
			ret += "&lfn=" + archiveAnchor.getCanonicalName();
		else if (lfns!=null && lfns.size()>0)
			ret += "&lfn=" + lfns.iterator().next().getCanonicalName();
		else
			ret += "&lfn=/NOLFN";
			
		if(archiveAnchor==null){
		ret += "&guid=" + guid.getName() ;
		} else {
			ret += "&zguid=" + guid.getName() ;
			ret += "&guid=" + archiveAnchor.guid;

		}
		ret += "&se=" + se.getName() +
		"&size=" + guid.size + "&md5="+ guid.md5;
		
		return ret;
	}
	
	/**
	 * @param signedEnvelope
	 */
	public void setSignedEnvelope(String signedEnvelope){
		this.signedEnvelope = signedEnvelope;
	}
	
	/**
	 * @return the signed envelope
	 */
	public String getSignedEnvelope(){
		return signedEnvelope;
	}
	
	/**
	 * @param encryptedEnvelope
	 */
	public void setEncryptedEnvelope(String encryptedEnvelope){
		this.encryptedEnvelope = encryptedEnvelope;
	}
	
	/**
	 * @return encrypted envelope
	 */
	public String getEncryptedEnvelope(){
		return encryptedEnvelope;
	}
}
