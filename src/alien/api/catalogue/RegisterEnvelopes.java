package alien.api.catalogue;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.Request;
import alien.catalogue.BookingTable;
import alien.catalogue.PFN;
import alien.catalogue.access.XrootDEnvelope;
import alien.catalogue.access.XrootDEnvelopeReply;
import alien.config.ConfigUtils;
import alien.io.xrootd.envelopes.XrootDEnvelopeSigner;
import alien.user.AliEnPrincipal;

/**
 * 
 * @author ron
 * @since Jun 05, 2011
 */
public class RegisterEnvelopes extends Request {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(RegisterEnvelopes.class.getCanonicalName());

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
	private final String lfn = null;
	@SuppressWarnings("unused")
	private final String perm = null;
	@SuppressWarnings("unused")
	private final String expire = null;
	@SuppressWarnings("unused")
	private final String pfn = null;
	@SuppressWarnings("unused")
	private final String se = null;
	@SuppressWarnings("unused")
	private final String GUID = null;

	/**
	 * Register PFNs with envelopes
	 * 
	 * @param user
	 * @param role
	 * @param signedEnvelopes
	 */
	public RegisterEnvelopes(final AliEnPrincipal user, final String role, final List<String> signedEnvelopes) {
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
	public RegisterEnvelopes(final AliEnPrincipal user, final String role, final String encryptedEnvelope, final int size, final String md5, final String lfn, final String perm, final String expire,
			final String pfn, final String se, final String GUID) {
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
			pfns = new ArrayList<>(signedEnvelopes.size());

			for (final String env : signedEnvelopes)
				try {
					if (XrootDEnvelopeSigner.verifyEnvelope(env, true)) {
						final XrootDEnvelope xenv = new XrootDEnvelope(env);

						if (logger.isLoggable(Level.FINER))
							logger.log(Level.FINER, "Self Signature VERIFIED! : " + xenv.pfn.pfn);

						if (BookingTable.commit(getEffectiveRequester(), BookingTable.getBookedPFN(xenv.pfn.pfn))) {
							if (logger.isLoggable(Level.FINE))
								logger.log(Level.FINE, "Successfully moved " + xenv.pfn.pfn + " to the Catalogue");

							pfns.add(xenv.pfn);
						} else
							logger.log(Level.WARNING, "Could not commit self-signed " + xenv.pfn.pfn + " to the Catalogue");
					} else if (XrootDEnvelopeSigner.verifyEnvelope(env, false)) {
						final XrootDEnvelopeReply xenv = new XrootDEnvelopeReply(env);

						if (logger.isLoggable(Level.FINER))
							logger.log(Level.FINER, "SE Signature VERIFIED! : " + xenv.pfn.pfn);

						if (BookingTable.commit(getEffectiveRequester(), BookingTable.getBookedPFN(xenv.pfn.pfn))) {
							if (logger.isLoggable(Level.FINE))
								logger.log(Level.FINE, "Successfully moved " + xenv.pfn.pfn + " to the Catalogue");

							pfns.add(xenv.pfn);
						} else
							logger.log(Level.WARNING, "Could not commit " + xenv.pfn.pfn + " to the Catalogue");
					} else
						logger.log(Level.WARNING, "COULD NOT VERIFY ANY SIGNATURE!");

				} catch (final SignatureException e) {
					logger.log(Level.WARNING, "Wrong signature", e);
				} catch (final InvalidKeyException e) {
					logger.log(Level.WARNING, "Invalid key", e);
				} catch (final NoSuchAlgorithmException e) {
					logger.log(Level.WARNING, "No such algorithm", e);
				} catch (final IOException e) {
					logger.log(Level.WARNING, "IO Exception", e);
				}
		} else if (encryptedEnvelope != null) {
			pfns = new ArrayList<>(1);
			XrootDEnvelope xenv = null;
			try {
				xenv = XrootDEnvelopeSigner.decryptEnvelope(encryptedEnvelope);
			} catch (final Exception e) {
				logger.log(Level.WARNING, "Error decrypting envelope", e);
				return;
			}

			if (xenv != null) {
				PFN bookedpfn = null;

				try {
					bookedpfn = BookingTable.getBookedPFN(xenv.pfn.pfn);
				} catch (final Exception e) {
					logger.log(Level.WARNING, "Error getting the PFN: ", e);
					return;
				}

				if (bookedpfn != null) {
					if (size != 0)
						bookedpfn.getGuid().size = size;

					if (md5 != null && md5.length() > 0 && !md5.equals("0"))
						bookedpfn.getGuid().md5 = md5;

					try {
						if (BookingTable.commit(getEffectiveRequester(), bookedpfn)) {
							if (logger.isLoggable(Level.FINE))
								logger.log(Level.FINE, "Successfully moved " + xenv.pfn.pfn + " to the Catalogue");

							pfns.add(bookedpfn);
						} else
							logger.log(Level.WARNING, "Unable to register " + xenv.pfn.pfn + " in the Catalogue");
					} catch (final Exception e) {
						logger.log(Level.WARNING, "Error registering pfn", e);
					}
				} else
					logger.log(Level.WARNING, "Could not find this booked pfn: " + xenv.pfn.pfn);
			} else
				logger.log(Level.WARNING, "Null decrypted envelope");
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
