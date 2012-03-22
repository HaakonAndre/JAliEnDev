package alien.se;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import lazyj.StringFactory;
import alien.catalogue.GUID;
import alien.config.ConfigUtils;
import alien.user.AliEnPrincipal;
import alien.user.LDAPHelper;

/**
 * @author costing
 * @since Nov 4, 2010
 */
public class SE implements Serializable, Comparable<SE> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5338699957055031926L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(SE.class.getCanonicalName());

	/**
	 * SE name
	 */
	public String seName;

	/**
	 * SE number
	 */
	public int seNumber;

	/**
	 * SE version number, if < 219, then triggers encrypted xrootd envelope
	 * creation over boolean needsEncryptedEnvelope
	 */
	public int seVersion;

	/**
	 * QoS associated to this storage elements
	 */
	public Set<String> qos;

	/**
	 * IO daemons
	 */
	public String seioDaemons;

	/**
	 * SE storage path
	 */
	public String seStoragePath;

	/**
	 * SE used space
	 */
	public long seUsedSpace;

	/**
	 * Number of files
	 */
	public long seNumFiles;

	/**
	 * Minimum size
	 */
	public long seMinSize;

	/**
	 * SE type
	 */
	public String seType;

	/**
	 * Access restricted to this users
	 */
	public Set<String> exclusiveUsers;

	/**
	 * Exclusive write
	 */
	public Set<String> seExclusiveWrite;

	/**
	 * Exclusive read
	 */
	public Set<String> seExclusiveRead;

	/**
	 * triggered by the seVersion if < 200
	 */
	public boolean needsEncryptedEnvelope;

	/**
	 * Demote write factor
	 */
	public double demoteWrite;
	
	/**
	 * Demote read factor
	 */
	public double demoteRead;
	
	/**
	 * Size, as declared in LDAP
	 */
	public long size;
	
	/**
	 * @param db
	 */
	SE(final DBFunctions db) {
		seName = StringFactory.get(db.gets("seName").toUpperCase());

		seNumber = db.geti("seNumber");

		qos = parseArray(db.gets("seQoS"));

		seVersion = db.geti("seVersion");

		needsEncryptedEnvelope = (seVersion < 200);
		// TODO: remove this, when the version in the DB is working and not
		// anymore overwritten to null
		if ("alice::cern::setest".equals(seName.toLowerCase()))
			needsEncryptedEnvelope = false;

		seioDaemons = StringFactory.get(db.gets("seioDaemons"));

		seStoragePath = StringFactory.get(db.gets("seStoragePath"));

		seUsedSpace = db.getl("seUsedSpace");

		seNumFiles = db.getl("seNumFiles");

		seMinSize = db.getl("seMinSize");

		seType = StringFactory.get(db.gets("seType"));

		exclusiveUsers = parseArray(db.gets("exclusiveUsers"));

		seExclusiveRead = parseArray(db.gets("seExclusiveRead"));

		seExclusiveWrite = parseArray(db.gets("seExclusiveWrite"));
		
		demoteWrite = db.getd("sedemotewrite");
		
		demoteRead = db.getd("sedemoteread");
		
		size = getSize();
	}

	@Override
	public String toString() {
		return "SE: seName: " + seName + "\n" + "seNumber\t: " + seNumber
				+ "\n" + "seVersion\t: " + seVersion + "\n" + "qos\t: " + qos
				+ "\n" + "seioDaemons\t: " + seioDaemons + "\n"
				+ "seStoragePath\t: " + seStoragePath + "\n"
				+ "seUsedSpace\t: " + seUsedSpace + "\n" + "seNumFiles\t: "
				+ seNumFiles + "\n" + "seMinSize\t: " + seMinSize + "\n"
				+ "seType\t: " + seType + "\n" + "exclusiveUsers\t: "
				+ exclusiveUsers + "\n" + "seExclusiveRead\t: "
				+ seExclusiveRead + "\n" + "seExclusiveWrite\t: "
				+ seExclusiveWrite;
	}

	/**
	 * @return SE name
	 */
	public String getName() {
		return seName;
	}
	
	/**
	 * @param qosRequest
	 * @return if this SE server the requested QoS type
	 */
	public boolean isQosType(String qosRequest){
		return qos.contains(qosRequest);
	}

	private static final NumberFormat twoDigits = new DecimalFormat("00");
	private static final NumberFormat fiveDigits = new DecimalFormat("00000");

	/**
	 * @return the protocol part
	 */
	public String generateProtocol() {
		if (seioDaemons == null || seioDaemons.length() == 0)
			return null;

		String ret = seioDaemons;

		if (!ret.endsWith("/") || seStoragePath == null || !seStoragePath.startsWith("/"))
			ret += "/";

		if ((seStoragePath != null) && !seStoragePath.equals("/"))
			ret += seStoragePath;
		
		return ret;
	}

	/**
	 * @param guid
	 * @return the PFN for this storage
	 */
	public String generatePFN(final GUID guid) {
		String ret = generateProtocol();

		if (ret == null)
			return ret;

		ret += "/" + twoDigits.format(guid.getCHash()) + "/"
				+ fiveDigits.format(guid.getHash()) + "/"
				+ guid.guid.toString();
		return StringFactory.get(ret);
	}

	/**
	 * @param s
	 * @return the set of elements
	 */
	public static Set<String> parseArray(final String s) {
		if (s == null)
			return null;

		final Set<String> ret = new HashSet<String>();

		final StringTokenizer st = new StringTokenizer(s, ",");

		while (st.hasMoreTokens()) {
			final String tok = StringFactory.get(st.nextToken().trim());

			if (tok.length() > 0)
				ret.add(tok);
		}

		return Collections.unmodifiableSet(ret);
	}

	@Override
	public int compareTo(final SE o) {
		return seName.compareToIgnoreCase(o.seName);
	}

	@Override
	public boolean equals(final Object obj) {
		if (!(obj instanceof SE))
			return false;

		return compareTo((SE) obj) == 0;
	}

	@Override
	public int hashCode() {
		return seName.toUpperCase().hashCode();
	}

	/**
	 * Check if the user is allowed to read files from this storage element
	 * 
	 * @param user
	 * @return <code>true</code> if allowed
	 */
	public boolean canRead(final AliEnPrincipal user) {
		if (seExclusiveRead.size() == 0)
			return true;

		return seExclusiveRead.contains(user.getName());
	}

	/**
	 * Check if the user is allowed to write files in this storage element
	 * 
	 * @param user
	 * @return <code>true</code> if allowed
	 */
	public boolean canWrite(final AliEnPrincipal user) {
		if (seExclusiveWrite.size() == 0)
			return true;

		final boolean allowed = seExclusiveWrite.contains(user.getName());

		return allowed;
	}
	
	/**
	 * @return Storage Element declared size, in KB, or <code>-1</code> if the SE is not defined
	 */
	private final long getSize(){
		final int idx = seName.indexOf("::");
		
		if (idx<0)
			return 0;
		
		final int idx2 = seName.lastIndexOf("::");
		
		if (idx2<=idx)
			return 0;
		
		final String site = seName.substring(idx+2, idx2);
		final String name = seName.substring(idx2+2);
		
		Set<String> ldapinfo = LDAPHelper.checkLdapInformation("name="+name, "ou=SE,ou=Services,ou="+site+",ou=Sites,", "savedir");
		
		if (ldapinfo==null || ldapinfo.size()==0){
			ldapinfo = LDAPHelper.checkLdapInformation("name="+name, "ou=SE,ou=Services,ou="+site+",ou=Sites,", "name");
			
			if (ldapinfo==null || ldapinfo.size()==0)
				return -1;
			
			return 0;
		}
		
		long ret = 0;
		
		for (final String s: ldapinfo){
			final StringTokenizer st = new StringTokenizer(s, ",");
			
			while (st.hasMoreTokens()){
				try{
					ret += Long.parseLong(st.nextToken());
				}
				catch (NumberFormatException nfe){
					// ignore
				}
			}
		}
		
		return ret;
	}
}
