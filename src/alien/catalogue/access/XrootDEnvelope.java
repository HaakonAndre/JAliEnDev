package alien.catalogue.access;

import java.io.Serializable;
import java.security.GeneralSecurityException;

import alien.catalogue.PFN;
import alien.se.SE;
import alien.se.SEUtils;
import alien.tsealedEnvelope.EncryptedAuthzToken;

/**
 * @author ron
 */
public class XrootDEnvelope implements Serializable {

	// analog to the following access types, we define levels:
	// public static int INVALID = 0;
	// public static int READ = 1;
	// public static int WRITE = 2;
	// public static int DELETE = 3;
	private static String[] levels = { "invalid", "read", "write-once",
			"delete" };

	private String plainEnvelopeTicket;
	private String encryptedEnvelope;
	
	public static final String hashord = "turl-access-lfn-guid-size-md5";
	
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

	public XrootDEnvelope(final CatalogueAccess access, final SE se,
			final String newPFN) {
		this.access = access;
		this.newPFN = newPFN;
		this.se = se;
		initializeEncryptedTicket();
	}

	private void initializeEncryptedTicket() {

		envAccess = levels[access.access];
		envTURL = se.seioDaemons.toString() + "/" + se.seStoragePath.toString()
				+ access.getGUID().getName();
		envPFN = se.seStoragePath.toString() + access.getGUID().getName();
		envLFN = access.getLFN().getName();
		envSize = Long.toString(access.getLFN().size);
		envGUID = access.getGUID().getName();
		envSE = se.seName;

		envMD5 = "md5field";
		// envMD5 = access.getLFN().md5.toString();



		
		
		System.out.println("we have a ticket:");
		System.out.println(plainEnvelopeTicket);
	}

	String getTicket() {
		if (plainEnvelopeTicket == null)
			initializeEncryptedTicket();

		return plainEnvelopeTicket;
	}

	public void decorateEnvelope(EncryptedAuthzToken authz)
			throws GeneralSecurityException {
		encryptedEnvelope = authz.encrypt(plainEnvelopeTicket);
		//System.out.println("we have an encrypted ticket:");
		//System.out.println(encryptedEnvelope);
	}

	private void setSignature(String signature){
		signedEnvelope = signature;
	}

	public String getUnsignedEnvelope() {
		
		return "turl=" + se.seioDaemons.toString() + "/" + se.seStoragePath.toString() +
		access.getGUID().getName() + "&access=" + levels[access.access] +
		"&lfn=" + access.getLFN().getName() +"&guid=" + access.getGUID().getName() +
		"&size=" + Long.toString(access.getLFN().size) + "&md5="+ "md5field";
		// envMD5 = access.getLFN().md5.toString();
		

	}

	private String getSignedEnvelope() {
		return signedEnvelope;
	}

	public String getEncryptedEnvelope() {
		return encryptedEnvelope;
	}
}
