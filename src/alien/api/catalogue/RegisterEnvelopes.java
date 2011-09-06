package alien.api.catalogue;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.List;

import alien.api.Request;
import alien.catalogue.BookingTable;
import alien.catalogue.PFN;
import alien.catalogue.access.XrootDEnvelope;
import alien.catalogue.access.XrootDEnvelopeReply;
import alien.io.xrootd.envelopes.XrootDEnvelopeSigner;
import alien.user.AliEnPrincipal;

/**
 * 
 * @author ron
 * @since Jun 05, 2011
 */
public class RegisterEnvelopes extends Request {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8844570313869928918L;
	private AliEnPrincipal user = null;
	private List<String> signedEnvelopes = null;
	private List<PFN> pfns = null;

	private String encryptedEnvelope = null;
	private int size = 0;
	@SuppressWarnings("unused")
	private String lfn = null;
	@SuppressWarnings("unused")
	private String perm = null;
	@SuppressWarnings("unused")
	private String expire = null;
	@SuppressWarnings("unused")
	private String pfn = null;
	@SuppressWarnings("unused")
	private String se = null;
	@SuppressWarnings("unused")
	private String GUID = null;
	private String md5 = null;

	/**
	 * Register PFNs with envelopes
	 * 
	 * @param user
	 * @param signedEnvelopes
	 */
	public RegisterEnvelopes(AliEnPrincipal user, List<String> signedEnvelopes) {
		this.user = user;
		this.signedEnvelopes = signedEnvelopes;
	}

	/**
	 * Register PFNs with envelopes
	 * 
	 * @param user
	 * @param encryptedEnvelope 
	 * @param size 
	 * @param lfn 
	 * @param perm 
	 * @param expire 
	 * @param pfn 
	 * @param se 
	 * @param GUID 
	 * @param md5 
	 */
	public RegisterEnvelopes(AliEnPrincipal user, String encryptedEnvelope,
			int size, String lfn, String perm, String expire, String pfn,
			String se, String GUID, String md5) {
		this.user = user;
		this.encryptedEnvelope = encryptedEnvelope;
	}

	@Override
	public void run() {

		if (signedEnvelopes != null) {
			pfns = new ArrayList<PFN>(signedEnvelopes.size());

			for (String env : signedEnvelopes) {

				System.out.println("We received an envelope for registration: "
						+ env);

				try {

					if (XrootDEnvelopeSigner.verifyEnvelope(env, true)) {
						XrootDEnvelope xenv = new XrootDEnvelope(env);
						System.out.println("Self Signature VERIFIED! : "
								+ xenv.pfn.pfn);
						if (BookingTable.commit(user,
								BookingTable.getBookedPFN(xenv.pfn.pfn))) {
							System.out.println("Successfully moved "
									+ xenv.pfn.pfn + " to the Catalogue");

							pfns.add(xenv.pfn);
						}

					} else if (XrootDEnvelopeSigner.verifyEnvelope(env, false)) {
						XrootDEnvelopeReply xenv = new XrootDEnvelopeReply(env);
						System.out.println("SE Signature VERIFIED! : "
								+ xenv.pfn.pfn);
						if (BookingTable.commit(user,
								BookingTable.getBookedPFN(xenv.pfn.pfn))) {
							System.out.println("Successfully moved "
									+ xenv.pfn.pfn + " to the Catalogue");
							pfns.add(xenv.pfn);
						}

					} else {
						System.out.println("COULD NOT VERIFY ANY SIGNATURE!");
					}

				} catch (SignatureException e) {
					System.err
							.println("Sorry ... Could not sign the envelope!");
				} catch (InvalidKeyException e) {
					System.err
							.println("Sorry ... Could not sign the envelope!");
				} catch (NoSuchAlgorithmException e) {
					System.err
							.println("Sorry ... Could not sign the envelope!");
				} catch (IOException e) {
					System.err.println("Sorry ... Error getting the PFN!");
				}
			}
		} else if (encryptedEnvelope != null) {

			try {
				XrootDEnvelope xenv = XrootDEnvelopeSigner
						.decryptEnvelope(encryptedEnvelope);
				if (xenv != null) {
					PFN pfn = BookingTable.getBookedPFN(xenv.pfn.pfn);

					if (pfn != null) {

						if (size != 0)
							pfn.getGuid().size = size;
						if (md5 != null && md5 != "" && md5 != "0")
							pfn.getGuid().md5 = md5;

						if (BookingTable.commit(user, pfn)) {
							System.out.println("Successfully moved "
									+ xenv.pfn.pfn + " to the Catalogue");
							pfns.add(xenv.pfn);
						}
					}
				}
			} catch (Exception e) {
				System.err.println("Sorry ... Error registering the PFN!");
			}
		}
	}

	/**
	 * @return PFNs to write on
	 */
	public List<PFN> getPFNs() {
		return pfns;
	}

	@Override
	public String toString() {
		return "Asked to register: " + signedEnvelopes.toString() + " ("
				+ "), reply is: " + this.pfns;
	}
}
