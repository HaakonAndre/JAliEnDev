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
	private List<String> envelopes = null;

	private List<PFN> pfns = null;

	/**
	 * Register PFNs with envelopes
	 * 
	 * @param user
	 * @param envelopes 
	 */
	public RegisterEnvelopes(AliEnPrincipal user,
			List<String> envelopes) {
		this.user = user;
		this.envelopes = envelopes;
	}

	@Override
	public void run() {

		if (envelopes != null) {
			pfns = new ArrayList<PFN>(envelopes.size());

			for (String env : envelopes) {

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
			return "Asked to register: " + envelopes.toString() + " (" + "), reply is: "
					+ this.pfns;
	}
}
