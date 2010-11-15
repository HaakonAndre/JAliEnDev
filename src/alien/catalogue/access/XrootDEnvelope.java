package alien.catalogue.access;

import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;

import alien.catalogue.PFN;
import alien.se.SE;
import alien.se.SEUtils;
import alien.tsealedEnvelope.EncryptedAuthzToken;

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
		initializeEncryptedTicket();
	}
	
	public XrootDEnvelope(final CatalogueAccess access, final SE se, final String newPFN) {
		this.access = access;
		this.newPFN = newPFN;
		this.se = se;
		initializeEncryptedTicket();
	}

	private void initializeEncryptedTicket() {

		envAccess = levels[access.access];
		envTURL = se.seioDaemons.toString() + "/" + se.seStoragePath.toString() + access.getGUID().getName();
		envPFN = se.seStoragePath.toString() + access.getGUID().getName();
		envLFN = access.getLFN().getName();
		envSize = Long.toString(access.getLFN().size);
		envGUID = access.getGUID().getName();
		envSE = se.seName;
		envGUID = access.getGUID().getName();
		envMD5 = access.getLFN().md5.toString();

		plainEnvelopeTicket = "<authz>\n  <file>\n" + "    <access>"
				+ envAccess + "</access>\n" + "    <turl>" + envTURL + "</turl>\n"
				+ "    <lfn>" + envLFN + "</lfn>\n"
				+ "    <size>" + envSize + "</size>\n" + "    <pfn>"
				+ envPFN + "</pfn>\n"
				+ "    <se>" + se.seName + "</se>\n" + "    <guid"
				+ envGUID + "</guid>\n" + "    <md5>"
				+ envMD5 + "</md5>\n" + "  </file>\n</authz>\n";
		System.out.println("we have a ticket:");
		System.out.println(plainEnvelopeTicket);
	}

	// void setTicket(String ticket){
	// this.ticket = ticket;
	// }
	String getTicket() {
		if (plainEnvelopeTicket == null)
			initializeEncryptedTicket();

		return plainEnvelopeTicket;
	}

	
	public void decorateEnvelope(EncryptedAuthzToken authz) throws GeneralSecurityException{
		encryptedEnvelope = authz.encrypt(plainEnvelopeTicket);
		System.out.println("we have an encrypted ticket:");
		System.out.println(encryptedEnvelope);
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
