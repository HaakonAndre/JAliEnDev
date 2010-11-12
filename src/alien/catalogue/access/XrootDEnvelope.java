package alien.catalogue.access;

import java.util.HashMap;
import java.util.Map;

import alien.catalogue.PFN;
import alien.se.SE;
import alien.se.SEUtils;

/**
 * @author Steffen
 */
public class XrootDEnvelope {

	// analog to the following access types, we define levels:
	//	public static int INVALID = 0;
	//	public static int READ = 1;
	//	public static int WRITE = 2;
	//	public static int DELETE = 3;
	private static String[] levels = {"invalid","read","write-once","delete"};
	
	private String plainEnvelopeTicket;
	private String encryptedEnvelope;
	private String signedEnvelope;

	private String envAccess;
	private String envLFN;
	private String envGUID;
	private String envPFN;
	private String envSize;
	private String envMD5;
	private String envTURL;
	private String envSE;

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
		se = SEUtils.getSE(Integer.valueOf(pfn.seNumber));
	}

	private void initializeEncryptedTicket() {

		envAccess = levels[access.access];
		envTURL = se.seioDaemons + se.seStoragePath + access.guid.toString();
		envPFN = se.seStoragePath + access.guid.toString();
		envLFN = access.lfn.toString();
		envSize = Long.toString(access.lfn.size);
		envGUID = access.guid.toString();
		envSE = se.seName;
		envGUID = access.guid.toString();
		envMD5 = access.lfn.md5;

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
		if (plainEnvelopeTicket == null)
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
	
	public Map<String, String> getPerlEnvelopeTicket(){
		
		Map<String, String> envelope = new HashMap<String, String>();
		envelope.put("access",envAccess);
		envelope.put("lfn", envLFN);
		envelope.put("guid",envGUID );
		envelope.put("pfn", envPFN);
		envelope.put("turl",envTURL );
		envelope.put("size",envSize );
		envelope.put("md5", envMD5);
		envelope.put("signedenvelope", signedEnvelope);
		envelope.put("envelope", encryptedEnvelope);

		return envelope;
	}
}
