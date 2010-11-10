package alien.catalogue.access;

import alien.catalogue.PFN;
import alien.se.SE;
import alien.se.SEUtils;

/**
 * @author Steffen
 */
public class XrootDEnvelope {

	private String plainEnvelopeTicket;
	private String encryptedEnvelope;
	private String signedEnvelope;

	private CatalogueAccess access;
	private SE se;
	private PFN pfn;

	void setCatalogueAccess(final CatalogueAccess access) {
		this.access = access;
	}

	XrootDEnvelope(final CatalogueAccess access, final PFN pfn) {
		this.access = access;
		this.pfn = pfn;
		decorate();
	}

	void decorate() {
		se = SEUtils.getSE(pfn.seNumber);
	}
	
	

	private void initializeEncryptedTicket() {
		plainEnvelopeTicket = "<authz>\n  <file>\n" + "    <access>"
				+ access.access + "</access>\n" + "    <turl>" + se.seioDaemons
				+ "/" + se.seStoragePath + access.guid.toString() + "</turl>\n"
				+ "    <lfn>" + access.lfn.toString() + "</lfn>\n"
				+ "    <size>" + access.lfn.size + "</size>\n" + "    <pfn>"
				+ se.seStoragePath + access.guid.toString() + "</pfn>\n"
				+ "    <se>" + se.seName + "</se>\n" + "    <guid"
				+ access.guid.toString() + "</guid>\n" + "    <md5>"
				+ access.lfn.md5 + "</md5>\n" + "  </file>\n</authz>\n";
	}

	// void setTicket(String ticket){
	// this.ticket = ticket;
	// }
	String getTicket() {
		if (plainEnvelopeTicket==null)
			initializeEncryptedTicket();
		
		return plainEnvelopeTicket;
	}

	// void setEncryptedEnvelope(String encryptedEnvelope){
	// this.encryptedEnvelope = encryptedEnvelope;
	// }
	String getEncryptedEnvelope() {
		return encryptedEnvelope;
	}

	// void setSignedEnvelope(String signedEnvelope){
	// this.signedEnvelope = signedEnvelope;
	// }
	String getSignedEnvelope() {
		return signedEnvelope;
	}
}
