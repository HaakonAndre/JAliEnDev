package alien.ui.api;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.se.SE;
import alien.ui.Dispatcher;
import alien.user.AliEnPrincipal;

/**
 * 
 * @author ron
 * @since Jun 03, 2011
 */
public class CatalogueApiUtils {

	/**
	 * Get LFN from String, only if it exists
	 * @param slfn name of the LFN
	 * @return the LFN object
	 */
	public static LFN getLFN(String slfn) {
		return getLFN(slfn, false);
	}

	/**
	 * Get LFN from String
	 * @param slfn name of the LFN
	 * @param evenIfDoesNotExist 
	 * @return the LFN object
	 */
	public static LFN getLFN(String slfn, boolean evenIfDoesNotExist) {

		try {
			LFNfromString rlfn = (LFNfromString) Dispatcher.execute(new LFNfromString(slfn, evenIfDoesNotExist),true);

			return rlfn.getLFN();
		} catch (IOException e) {
			System.out.println("Could not get LFN: " + slfn);
			e.printStackTrace();
		}
		return null;

	}

	/**
	 * Get GUID from String
	 * @param sguid GUID as String
	 * @return the GUID object
	 */
	public static GUID getGUID(String sguid) {
		return getGUID(sguid, false);
	}

	/**
	 * Get GUID from String
	 * @param sguid GUID as String
	 * @param evenIfDoesNotExist 
	 * @return the GUID object
	 */
	public static GUID getGUID(String sguid, boolean evenIfDoesNotExist) {

		try {
			GUIDfromString rguid = (GUIDfromString) Dispatcher.execute(new GUIDfromString(sguid,
							evenIfDoesNotExist),true);

			return rguid.getGUID();
		} catch (IOException e) {
			System.out.println("Could not get GUID: " + sguid);
		}
		return null;

	}

	/**
	 * Get PFNs from GUID as String
	 * @param sguid GUID as String
	 * @return the PFNs
	 */
	public static Set<PFN> getPFNs(String sguid) {

		try {
			PFNfromString rpfns = (PFNfromString) Dispatcher.execute(new PFNfromString(sguid),true);

			return rpfns.getPFNs();
		} catch (IOException e) {
			System.out.println("Could not get GUID: " + sguid);
		}
		return null;

	}
	
	
	/**
	 * Get PFNs for reading by LFN
	 * @param user asking for access
	 * @param site site the user is matched to
	 * @param lfn LFN of the entry as String 
	 * @param ses SEs to priorize to read from 
	 * @param exses SEs to depriorize to read from 
	 * @return PFNs, filled with read envelopes and credentials if necessary and authorized
	 */
	public static List<PFN> getPFNsToRead(AliEnPrincipal user, String site, LFN lfn,
			List<String> ses, List<String> exses) {
		try {
			PFNforReadOrDel readFile = (PFNforReadOrDel) Dispatcher.execute(new PFNforReadOrDel(user, site, AccessType.READ,
							lfn, ses, exses),true);
			return readFile.getPFNs();
		} catch (IOException e) {
			System.out.println("Could not get PFN for: " + lfn);
		}
		return null;
	}

	/**
	 * Get PFNs for reading by GUID
	 * @param user asking for access
	 * @param site site the user is matched to
	 * @param guid GUID of the entry as String 
	 * @param ses SEs to priorize to read from 
	 * @param exses SEs to depriorize to read from 
	 * @return PFNs, filled with read envelopes and credentials if necessary and authorized
	 */
	public static List<PFN> getPFNsToRead(AliEnPrincipal user, String site, GUID guid,
			List<String> ses, List<String> exses) {
		try {
			PFNforReadOrDel readFile = (PFNforReadOrDel) Dispatcher.execute(new PFNforReadOrDel(user, site, AccessType.READ,
							guid, ses, exses),true);
			return readFile.getPFNs();
		} catch (IOException e) {
			System.out.println("Could not get PFN for: " + guid);
		}
		return null;
	}
	
	/**
	 * Get PFNs for writing by LFN
	 * @param user asking for access
	 * @param site site the user is matched to
	 * @param lfn LFN of the entry as String 
	 * @param ses SEs to priorize to read from 
	 * @param exses SEs to depriorize to read from 
	 * @param qosType QoS type to ask for
	 * @param qosCount QoS count of the type to ask for
	 * @return PFNs, filled with write envelopes and credentials if necessary and authorized
	 */
	public static List<PFN> getPFNsToWrite(AliEnPrincipal user, String site, LFN lfn,
			List<String> ses, List<String> exses, String qosType, int qosCount) {
		try {
			PFNforWrite writeFile = (PFNforWrite) Dispatcher.execute(new PFNforWrite(user, site, lfn, ses, exses,
							qosType, qosCount),true);
			return writeFile.getPFNs();
		} catch (IOException e) {
			System.out.println("Could not get PFN for: " + lfn);
		}
		return null;
	}

	/**
	 * Get PFNs for writing by GUID
	 * @param user asking for access
	 * @param site site the user is matched to
	 * @param guid GUID of the entry as String 
	 * @param ses SEs to priorize to read from 
	 * @param exses SEs to depriorize to read from 
	 * @param qosType QoS type to ask for
	 * @param qosCount QoS count of the type to ask for
	 * @return PFNs, filled with write envelopes and credentials if necessary and authorized
	 */
	public static List<PFN> getPFNsToWrite(AliEnPrincipal user, String site, GUID guid,
			List<String> ses, List<String> exses, String qosType, int qosCount) {
		try {
			PFNforWrite writeFile = (PFNforWrite) Dispatcher.execute(new PFNforWrite(user, site, guid, ses, exses,
							qosType, qosCount),true);
			return writeFile.getPFNs();
		} catch (IOException e) {
			System.out.println("Could not get PFN for: " + guid);
		}
		return null;
	}
	
	
	/**
	 * Register PFNs with enveloeps
	 * @param user asking for access
	 * @param envelopes
	 * @return PFNs that were successfully registered
	 */
	public static List<PFN> registerEnvelopes(AliEnPrincipal user, List<String> envelopes) {
		try {
			RegisterEnvelopes register = (RegisterEnvelopes) Dispatcher.execute(new RegisterEnvelopes(user, envelopes),true);
			return register.getPFNs();
		} catch (IOException e) {
			System.out.println("Could not get PFNs for: " + envelopes.toString());
		}
		return null;
	}
	
	
	/**
	 * Get an SE by its name
	 * @param se name of the SE
	 * @return SE object
	 */
	public static SE getSE(String se) {

		try {
			SEfromString rse = (SEfromString) Dispatcher.execute(new SEfromString(se),true);

			return rse.getSE();
		} catch (IOException e) {
			System.out.println("Could not get SE: " + se);
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Get an SE by its number
	 * @param seno number of the SE
	 * @return SE object
	 */
	public static SE getSE(int seno) {

		try {
			SEfromString rse = (SEfromString) Dispatcher.execute(new SEfromString(seno),true);

			return rse.getSE();
		} catch (IOException e) {
			System.out.println("Could not get SE: " + seno);
			e.printStackTrace();
		}
		return null;
	}

}
