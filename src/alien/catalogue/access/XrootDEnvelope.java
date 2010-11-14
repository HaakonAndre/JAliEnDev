package alien.catalogue.access;

import java.util.HashMap;
import java.util.Map;

import alien.catalogue.PFN;
import alien.se.SE;
import alien.se.SEUtils;

/**
 * @author ron
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
	private String newPFN;
	

	void setCatalogueAccess(final CatalogueAccess access) {
		this.access = access;
	}

	public XrootDEnvelope(final CatalogueAccess access, final PFN pfn) {
		this.access = access;
		this.pfn = pfn;
		se = SEUtils.getSE(Integer.valueOf(pfn.seNumber));
	}
	
	public XrootDEnvelope(final CatalogueAccess access, final SE se, final String newPFN) {
		this.access = access;
		this.newPFN = newPFN;
		this.se = se;
	}

	private void initializeEncryptedTicket() {

		envAccess = levels[access.access];
		envTURL = se.seioDaemons + se.seStoragePath + access.getGUID().toString();
		envPFN = se.seStoragePath + access.getGUID().toString();
		envLFN = access.getLFN().toString();
		envSize = Long.toString(access.getLFN().size);
		envGUID = access.getGUID().toString();
		envSE = se.seName;
		envGUID = access.getGUID().toString();
		envMD5 = access.getLFN().md5;

		plainEnvelopeTicket = "<authz>\n  <file>\n" + "    <access>"
				+ access.access + "</access>\n" + "    <turl>" + se.seioDaemons
				+ "/" + se.seStoragePath + access.getGUID().toString() + "</turl>\n"
				+ "    <lfn>" + access.getLFN().toString() + "</lfn>\n"
				+ "    <size>" + access.getLFN().size + "</size>\n" + "    <pfn>"
				+ se.seStoragePath + access.getGUID().toString() + "</pfn>\n"
				+ "    <se>" + se.seName + "</se>\n" + "    <guid"
				+ access.getGUID().toString() + "</guid>\n" + "    <md5>"
				+ access.getLFN().md5 + "</md5>\n" + "  </file>\n</authz>\n";
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
