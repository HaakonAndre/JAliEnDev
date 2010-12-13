package alien.catalogue.access;

import java.io.Serializable;
import java.util.Set;

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
	 * @return envelope xml
	 */
	public String getUnEncryptedEnvelope() {

		final String access = type.toString().replace("write", "write-once");
		
		final String[] pfnsplit = pfn.getPFN().split("//");
		
		final GUID guid = pfn.getGuid();
		
		final Set<LFN> lfns = guid.getLFNs();
		
		final SE se = SEUtils.getSE(pfn.seNumber);
		
		String ret = "<authz>\n  <file>\n"
		+ "    <access>"+ access+"</access>\n"
		+ "    <turl>"+ pfn.getPFN()+ "</turl>\n";
		
		if (lfns!=null && lfns.size()>0)
			ret += "    <lfn>"+lfns.iterator().next().getCanonicalName()+"</lfn>\n";
		
		ret += "    <size>"+Long.toString(guid.size)+"</size>" + "\n"
		+ "    <pfn>"+pfnsplit[2]+"</pfn>\n"
		+ "    <se>"+se.getName()+"</se>\n"
		+ "    <guid>"+guid.getName()+"</guid>\n"
		+ "    <md5>"+guid.md5+"</md5>\n"
		+ "  </file>\n</authz>\n";
		
		return ret;
	}
	

	/**
	 * @return url envelope
	 */
	public String getUnsignedEnvelope() {
		
		final GUID guid = pfn.getGuid();
		
		final Set<LFN> lfns = guid.getLFNs();
		
		final SE se = SEUtils.getSE(pfn.seNumber);
		
		String ret = "turl=" + pfn.getPFN() + "&access=" + type.toString();
		
		if (lfns!=null && lfns.size()>0)
			ret += "&lfn=" + lfns.iterator().next().getCanonicalName();
	
		ret += "&guid=" + guid.getName() +
		"&se=" + se.getName() +
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
