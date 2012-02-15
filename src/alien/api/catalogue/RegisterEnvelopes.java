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
	private List<String> signedEnvelopes = null;
	private List<PFN> pfns = null;

	private String encryptedEnvelope = null;
	private int size = 0;
	private String md5 = null;

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

	/**
	 * Register PFNs with envelopes
	 * 
	 * @param user
	 * @param role
	 * @param signedEnvelopes
	 */
	public RegisterEnvelopes(final AliEnPrincipal user, final String role, List<String> signedEnvelopes) {
		setRequestUser(user);
		setRoleRequest(role);
		this.signedEnvelopes = signedEnvelopes;
	}

	/**
	 * Register PFNs with envelopes
	 * 
	 * @param user
	 * @param role
	 * 
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
	public RegisterEnvelopes(final AliEnPrincipal user, final String role, String encryptedEnvelope, int size, String md5, String lfn, String perm, String expire, String pfn, String se, String GUID) {
		setRequestUser(user);
		setRoleRequest(role);
		this.encryptedEnvelope = encryptedEnvelope;
		this.size = size;
		this.md5 = md5;
	}

	@Override
	public void run() {
		authorizeUserAndRole();

		if (signedEnvelopes != null) {
			pfns = new ArrayList<PFN>(signedEnvelopes.size());

			for (final String env : signedEnvelopes) {
				try {
					if (XrootDEnvelopeSigner.verifyEnvelope(env, true)) {
						XrootDEnvelope xenv = new XrootDEnvelope(env);
						
						System.out.println("Self Signature VERIFIED! : " + xenv.pfn.pfn);
						
						if (BookingTable.commit(getEffectiveRequester(), BookingTable.getBookedPFN(xenv.pfn.pfn))) {
							System.out.println("Successfully moved " + xenv.pfn.pfn + " to the Catalogue");

							pfns.add(xenv.pfn);
						}
						else{
							System.out.println("Could not commit self-signed "+xenv.pfn.pfn+" to the Catalogue");
						}
					}
					else
						if (XrootDEnvelopeSigner.verifyEnvelope(env, false)) {
							XrootDEnvelopeReply xenv = new XrootDEnvelopeReply(env);
							
							System.out.println("SE Signature VERIFIED! : " + xenv.pfn.pfn);
							
							if (BookingTable.commit(getEffectiveRequester(), BookingTable.getBookedPFN(xenv.pfn.pfn))) {
								System.out.println("Successfully moved " + xenv.pfn.pfn + " to the Catalogue");
							
								pfns.add(xenv.pfn);
							}
							else{
								System.out.println("Could not commit "+xenv.pfn.pfn+" to the Catalogue");
							}
						}
						else {
							System.out.println("COULD NOT VERIFY ANY SIGNATURE!");
						}

				}
				catch (SignatureException e) {
					System.err.println("Sorry ... Could not sign the envelope!");
				}
				catch (InvalidKeyException e) {
					System.err.println("Sorry ... Could not sign the envelope!");
				}
				catch (NoSuchAlgorithmException e) {
					System.err.println("Sorry ... Could not sign the envelope!");
				}
				catch (IOException e) {
					System.err.println("Sorry ... Error getting the PFN!");
				}
			}
		}
		else
			if (encryptedEnvelope != null) {
				pfns = new ArrayList<PFN>(1);
				XrootDEnvelope xenv = null;
				try {
					xenv = XrootDEnvelopeSigner.decryptEnvelope(encryptedEnvelope);
				}
				catch (Exception e) {
					System.err.println("Sorry ... Error decrypting envelope: " + e);
				}

				if (xenv != null) {
					PFN bookedpfn = null;

					try {
						bookedpfn = BookingTable.getBookedPFN(xenv.pfn.pfn);
					}
					catch (Exception e) {
						System.err.println("Sorry ... Error getting the PFN: " + e);
					}

					if (bookedpfn != null) {

						if (size != 0)
							bookedpfn.getGuid().size = size;
						if (md5 != null && md5 != "" && md5 != "0")
							bookedpfn.getGuid().md5 = md5;

						try {
							if (BookingTable.commit(getEffectiveRequester(), bookedpfn)) {

								System.out.println("Successfully moved " + xenv.pfn.pfn + " to the Catalogue");

								pfns.add(bookedpfn);
							}
							else {
								System.err.println("Unable to register " + xenv.pfn.pfn + " in the Catalogue");
							}
						}
						catch (Exception e) {
							e.printStackTrace();
							System.err.println("Sorry ... Error registering the PFN: " + e);
						}
					}

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
		return "Asked to register: " + signedEnvelopes.toString() + " (" + "), reply is: " + this.pfns;
	}
}
