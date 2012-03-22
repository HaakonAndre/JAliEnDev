package alien.api.catalogue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.Request;
import alien.catalogue.BookingTable;
import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.se.SE;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;
import alien.user.LDAPHelper;

/**
 * 
 * @author ron
 * @since Jun 03, 2011
 */
public class PFNforWrite extends Request {
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(PFNforWrite.class.getCanonicalName());

	private static final long serialVersionUID = 6219657670649893255L;

	private String site = null;
	private LFN lfn = null;
	private GUID guid = null;
	private List<String> ses = null;
	private List<String> exses = null;
	
	private HashMap<String,Integer> qos = null;

	private List<PFN> pfns = null;

	/**
	 * Get PFNs to write
	 * 
	 * @param user
	 * @param role
	 * @param site
	 * @param lfn
	 * @param guid
	 * @param ses
	 * @param exses
	 * @param qos
	 */
	public PFNforWrite(final AliEnPrincipal user, final String role, final String site, final LFN lfn, final GUID guid, final List<String> ses, final List<String> exses, final HashMap<String,Integer> qos) {
		setRequestUser(user);
		setRoleRequest(role);
		this.site = site;
		this.lfn = lfn;
		this.guid = guid;
		this.ses = ses;
		this.exses = exses;
		this.qos = qos;
		
		if (logger.isLoggable(Level.FINE)){
			logger.log(Level.FINE, "got pos: "+ses);
			logger.log(Level.FINE, "got neg: " + exses);
			logger.log(Level.FINE, "got qos: " + qos);
		}
	}

	@Override
	public void run() {
		authorizeUserAndRole();
		
		if (logger.isLoggable(Level.FINE)){
			logger.log(Level.FINE, "REQUEST IS:" + this);
			
			logger.log(Level.FINE, "got pos: "+ses);
			logger.log(Level.FINE, "got neg: " + exses);
			logger.log(Level.FINE, "got qos: " + qos);
		
			logger.log(Level.FINE,"Request details : ----------------------\n" + guid
				+ "\n ---------------------- \n " + lfn
				+ " \n ---------------------- \n" + getEffectiveRequester());
		}
		
		if (((this.ses == null) || this.ses.size() == 0)
				&& (this.qos == null || (this.qos.keySet().size() < 1))) {
			final Set<String> defaultQos = LDAPHelper.checkLdapInformation(
					"(objectClass=AliEnVOConfig)", "ou=Config,",
					"sedefaultQosandCount");

			if (defaultQos.isEmpty())
				logger.log(Level.WARNING,
						"No specification of storages and no default LDAP entry found.");

			String defQos = defaultQos.iterator().next();

			if (this.qos == null)
				this.qos = new HashMap<String, Integer>(1);

			int count = 1;

			try {
				count = Integer.parseInt(defQos.substring(defQos.indexOf('=') + 1));
			} catch (NumberFormatException e) {
				// ignore
			}

			this.qos.put(defQos.substring(0, defQos.indexOf('=')), Integer.valueOf(count));
		}

		List<SE> SEs = SEUtils.getBestSEsOnSpecs(this.site, this.ses, this.exses, this.qos, true);

		if (SEs == null || SEs.size() < 1) {
			this.pfns = new ArrayList<PFN>(0);
			logger.log(Level.WARNING,"Couldn't discover any SEs for this request (site:"+this.site+", ses:"+this.ses+", exses:"+this.exses+", qos:"+this.qos+")");
			return;
		}

		this.pfns = new ArrayList<PFN>(SEs.size());

		for (final SE se : SEs) {
			if (!se.canWrite(getEffectiveRequester())) {
				logger.log(
						Level.INFO,
						getEffectiveRequester()
								+ " is not allowed to write to the explicitly requested SE "
								+ se.seName);
				continue;
			}
			try {
				this.pfns.add(BookingTable.bookForWriting(getEffectiveRequester(),
						this.lfn, this.guid, null, 0, se));
			} catch (Exception e) {
				logger.log(Level.WARNING,
						"Error for the request on " + se.getName()
								+ ", message", e.fillInStackTrace());
			}
		}

		logger.log(Level.FINE, "Returning: " + this.toString());
	}

	/**
	 * @return PFNs to write on
	 */
	public List<PFN> getPFNs() {
		return this.pfns;
	}

	@Override
	public String toString() {
			return "Asked for write: " + this.lfn + " (" + this.site + "," + this.qos + "," + this.ses + "," + this.exses + "), reply is: " + this.pfns;

	}
}
