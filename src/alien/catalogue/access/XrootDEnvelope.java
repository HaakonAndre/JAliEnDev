package alien.catalogue.access;

import java.io.Serializable;

import alien.catalogue.PFN;
import alien.se.SE;


/**
 * @author ron
 *
 */
public class XrootDEnvelope  implements Serializable {

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
	 * @param ticket 
	 * @param pfn 
	 * @param se 
	 */
	public XrootDEnvelope(final AccessTicket ticket, PFN pfn, SE se){
	
		
		this.ticket = ticket;
		this.se = se;
		this.pfn = pfn;
		createEcryptedEnvelope = se.needsEncryptedEnvelope;
	}


	/**
	 * @return envelope xml
	 */
	public String getUnEncryptedEnvelope() {

		String access = ticket.getAccessType().toString().replace("write", "write-once");
		
		String[] pfnsplit = pfn.toString().split("//");
		
		return "<authz>\n  <file>\n"
		+ "    <access>"+ access+"</access>\n"
		+ "    <turl>"+ pfn.getPFN()+ "</turl>\n"
		+ "    <lfn>"+ticket.getLFN().getName()+"</lfn>\n"
		+ "    <size>"+Long.toString(ticket.getLFN().size)+"</size>" + "\n"
		+ "    <pfn>"+pfnsplit[2]+"</pfn>\n"
		+ "    <se>"+se.getName()+"</se>\n"
		+ "    <guid>"+ticket.getGUID().getName()+"</guid>\n"
		+ "    <md5>"+ticket.getLFN().md5+"</md5>\n"
		+ "  </file>\n</authz>\n";
	}
	

	/**
	 * @return url envelope
	 */
	public String getUnsignedEnvelope() {
		
		return "turl=" + pfn.getPFN()
		+ "&access=" + ticket.getAccessType().toString() +
		"&lfn=" + ticket.getLFN().getName() +"&guid=" + ticket.getGUID().getName() +
		"&se=" + se.getName() +
		"&size=" + Long.toString(ticket.getLFN().size) + "&md5="+ ticket.getLFN().md5;
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
