package alien.catalogue.access;

import java.io.Serializable;
import java.security.GeneralSecurityException;

import alien.catalogue.CatalogEntity;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.se.SE;
import alien.se.SEUtils;
import alien.tsealedEnvelope.EncryptedAuthzToken;


/**
 * @author ron
 *
 */
public class XrootDEnvelope  implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1024787790575833398L;

	public static final String hashord = "turl-access-lfn-guid-size-md5";
	
	/**
	 * the access ticket this envelope belongs to
	 */
	public AccessTicket ticket = null;
	
	/**
	 * pfn of the file on the SE (proto:://hostdns:port//storagepath)
	 */
	public final PFN pfn;
	

	/**
	 * name of the regarding SE
	 */
	public final SE se;
	
	/**
	 * Signed envelope
	 */
	protected String signedEnvelope; 
	
	/**
	 * Encrypted envelope
	 */
	protected String encryptedEnvelope;

	/**
	 * triggers additional encrypted envelope creation
	 */
	public boolean createEcryptedEnvelope;
	
	
	/**
	 * @param type
	 * @param signedEnvelope 
	 * @param encryptedEnvelope 
	 */
	public XrootDEnvelope(final AccessTicket ticket, PFN pfn, SE se){
	
		
		this.ticket = ticket;
		this.se = se;
		this.pfn = pfn;
		createEcryptedEnvelope = se.needsEncryptedEnvelope;
	}


	public String getUnEncryptedEnvelope() {

		String access = ticket.getAccessType().toString().replace("write", "write-once");
		
		String[] pfnsplit = pfn.toString().split("//");
		
		return "<authz>\n  <file>\n"
		+ "    <access>"+ access+"</access>\n"
		+ "    <turl>"+ pfn.toString()+ "</turl>\n"
		+ "    <lfn>"+ticket.getLFN().toString()+"</lfn>\n"
		+ "    <size>"+Long.toString(ticket.getLFN().size)+"</size>" + "\n"
		+ "    <pfn>"+pfnsplit[2]+"</pfn>\n"
		+ "    <se>"+se.toString()+"</se>\n"
		+ "    <guid>"+ticket.getGUID().toString()+"</guid>\n"
		+ "    <md5>"+ticket.getLFN().md5.toString()+"</md5>\n"
		+ "  </file>\n</authz>\n";
	}
	

	public String getUnsignedEnvelope() {
		
		return "turl=" + pfn.toString() 
		+ "&access=" + ticket.getAccessType().toString() +
		"&lfn=" + ticket.getLFN().toString() +"&guid=" + ticket.getGUID().toString() +
		"&size=" + Long.toString(ticket.getLFN().size) + "&md5="+ ticket.getLFN().md5.toString();
	}

	public void setSignedEnvelope(String signedEnvelope){
		this.signedEnvelope = signedEnvelope;
	}
	public String getSignedEnvelope(){
		return signedEnvelope;
	}
	
	public void setEncryptedEnvelope(String encryptedEnvelope){
		this.encryptedEnvelope = encryptedEnvelope;
	}
	public String getEncryptedEnvelope(){
		return encryptedEnvelope;
	}
}
