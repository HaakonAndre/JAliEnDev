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
 * Get the GUID object for String
 * 
 * @author ron
 * @since Jun 03, 2011
 */
public class CatalogueApiUtils {

	public static LFN getLFN(String slfn) {
		return getLFN(slfn, false);
	}

	public static LFN getLFN(String slfn, boolean evenIfDoesNotExist) {

		try {
			LFNfromString rlfn = (LFNfromString) Dispatcher.execute(new LFNfromString(slfn, evenIfDoesNotExist));

			return rlfn.getLFN();
		} catch (IOException e) {
			System.out.println("Could not get LFN: " + slfn);
			e.printStackTrace();
		}
		return null;

	}

	public static GUID getGUID(String sguid) {
		return getGUID(sguid, false);
	}

	public static GUID getGUID(String sguid, boolean evenIfDoesNotExist) {

		try {
			GUIDfromString rguid = (GUIDfromString) Dispatcher.execute(new GUIDfromString(sguid,
							evenIfDoesNotExist));

			return rguid.getGUID();
		} catch (IOException e) {
			System.out.println("Could not get GUID: " + sguid);
		}
		return null;

	}

	public static Set<PFN> getPFNs(String sguid) {

		try {
			PFNfromString rpfns = (PFNfromString) Dispatcher.execute(new PFNfromString(sguid));

			return rpfns.getPFNs();
		} catch (IOException e) {
			System.out.println("Could not get GUID: " + sguid);
		}
		return null;

	}
	
	
	public static List<PFN> getPFNsToRead(AliEnPrincipal user, String site, LFN lfn,
			List<String> ses, List<String> exses) {
		try {
			PFNforReadOrDel readFile = (PFNforReadOrDel) Dispatcher.execute(new PFNforReadOrDel(user, site, AccessType.READ,
							lfn, ses, exses));
			return readFile.getPFNs();
		} catch (IOException e) {
			System.out.println("Could not get PFN for: " + lfn);
		}
		return null;
	}

	public static List<PFN> getPFNsToRead(AliEnPrincipal user, String site, GUID guid,
			List<String> ses, List<String> exses) {
		try {
			PFNforReadOrDel readFile = (PFNforReadOrDel) Dispatcher.execute(new PFNforReadOrDel(user, site, AccessType.READ,
							guid, ses, exses));
			return readFile.getPFNs();
		} catch (IOException e) {
			System.out.println("Could not get PFN for: " + guid);
		}
		return null;
	}
	
	public static List<PFN> getPFNsToWrite(AliEnPrincipal user, String site, LFN lfn,
			List<String> ses, List<String> exses, String qosType, int qosCount) {
		try {
			PFNforWrite writeFile = (PFNforWrite) Dispatcher.execute(new PFNforWrite(user, site, lfn, ses, exses,
							qosType, qosCount));
			return writeFile.getPFNs();
		} catch (IOException e) {
			System.out.println("Could not get PFN for: " + lfn);
		}
		return null;
	}

	public static List<PFN> getPFNsToWrite(AliEnPrincipal user, String site, GUID guid,
			List<String> ses, List<String> exses, String qosType, int qosCount) {
		try {
			PFNforWrite writeFile = (PFNforWrite) Dispatcher.execute(new PFNforWrite(user, site, guid, ses, exses,
							qosType, qosCount));
			return writeFile.getPFNs();
		} catch (IOException e) {
			System.out.println("Could not get PFN for: " + guid);
		}
		return null;
	}
	
	public static SE getSE(String se) {

		try {
			SEfromString rse = (SEfromString) Dispatcher.execute(new SEfromString(se));

			return rse.getSE();
		} catch (IOException e) {
			System.out.println("Could not get SE: " + se);
			e.printStackTrace();
		}
		return null;
	}
	
	
	public static SE getSE(int seno) {

		try {
			SEfromString rse = (SEfromString) Dispatcher.execute(new SEfromString(seno));

			return rse.getSE();
		} catch (IOException e) {
			System.out.println("Could not get SE: " + seno);
			e.printStackTrace();
		}
		return null;
	}
	
	
	

}
