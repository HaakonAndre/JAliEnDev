package alien.api.catalogue;

import java.util.List;
import java.util.Set;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.se.SE;
import alien.shell.commands.JAliEnCOMMander;

/**
 * 
 * @author ron
 * @since Jun 03, 2011
 */
public class CatalogueApiUtils {
	
	private final JAliEnCOMMander commander;
	
	
	/**
	 * @param commander
	 */
	public CatalogueApiUtils(JAliEnCOMMander commander){
		this.commander = commander;
	}

	/**
	 * Get LFN from String, only if it exists
	 * 
	 * @param slfn
	 *            name of the LFN
	 * @return the LFN object
	 */
	public LFN getLFN(String slfn) {
		return getLFN(slfn, false);
	}

	/**
	 * Get LFNs from String as a directory listing, only if it exists
	 * 
	 * @param slfn
	 *            name of the LFN
	 * @return the LFN objects
	 */
	public List<LFN> getLFNs(String slfn) {
		try {
			return Dispatcher.execute(new LFNListingfromString(commander.getUser(), commander.getRole(), slfn), true).getLFNs();
		}
		catch (ServerException e) {
			System.out.println("Could not get LFN: " + slfn);
			e.getCause().printStackTrace();
		}
		
		return null;
	}

	/**
	 * Get LFN from String
	 * 
	 * @param slfn
	 *            name of the LFN
	 * @param evenIfDoesNotExist
	 * @return the LFN object
	 */
	public LFN getLFN(String slfn, boolean evenIfDoesNotExist) {

		try {
			return Dispatcher.execute(new LFNfromString(commander.getUser(), commander.getRole(), slfn, evenIfDoesNotExist), true).getLFN();
		}
		catch (ServerException e) {
			System.out.println("Could not get LFN: " + slfn);
			e.getCause().printStackTrace();
		}
		
		return null;
	}
	

	/**
	 * Remove a LFN in the Catalogue
	 * @param path
	 * @return state of the LFN's deletion
	 *         <code>null</code>
	 */
	public boolean removeLFN(String path) {
		try {
			return Dispatcher.execute(new RemoveLFNfromString(commander.getUser(), commander.getRole(), path), true).wasRemoved();
		}
		catch (ServerException e) {
			System.out.println("Could not remove the LFN: " + path);
			e.getCause().printStackTrace();
		}
		
		return false;
	}
	

	/**
	 * Get GUID from String
	 * 
	 * @param sguid
	 *            GUID as String
	 * @return the GUID object
	 */
	public GUID getGUID(String sguid) {
		return getGUID(sguid, false);
	}

	/**
	 * Get GUID from String
	 * 
	 * @param sguid
	 *            GUID as String
	 * @param evenIfDoesNotExist
	 * @return the GUID object
	 */
	public GUID getGUID(String sguid, boolean evenIfDoesNotExist) {
		try {
			return Dispatcher.execute(new GUIDfromString(commander.getUser(), commander.getRole(), sguid, evenIfDoesNotExist), true).getGUID();
		}
		catch (ServerException e) {
			System.out.println("Could not get GUID: " + sguid);
			e.getCause().printStackTrace();
		}
		
		return null;
	}

	/**
	 * Get PFNs from GUID as String
	 * 
	 * @param sguid
	 *            GUID as String
	 * @return the PFNs
	 */
	public Set<PFN> getPFNs(String sguid) {
		try {
			return Dispatcher.execute(new PFNfromString(commander.getUser(), commander.getRole(), sguid), true).getPFNs();
		}
		catch (ServerException e) {
			System.out.println("Could not get GUID: " + sguid);
			e.getCause().printStackTrace();
		}
		
		return null;
	}

	/**
	 * Get PFNs for reading by LFN
	 * 
	 * @param site
	 *            site the user is matched to
	 * @param lfn
	 *            LFN of the entry as String
	 * @param ses
	 *            SEs to prioritize to read from
	 * @param exses
	 *            SEs to deprioritize to read from
	 * @return PFNs, filled with read envelopes and credentials if necessary and
	 *         authorized
	 */
	public List<PFN> getPFNsToRead(String site, LFN lfn, List<String> ses, List<String> exses) {
		try {
			return Dispatcher.execute(new PFNforReadOrDel(commander.getUser(), commander.getRole(), site, AccessType.READ, lfn, ses, exses), true).getPFNs();
		}
		catch (ServerException e) {
			System.out.println("Could not get PFN for: " + lfn);
			e.getCause().printStackTrace();
		}
		
		return null;
	}

	/**
	 * Get PFNs for reading by GUID
	 * 
	 * @param site
	 *            site the user is matched to
	 * @param guid
	 *            GUID of the entry as String
	 * @param ses
	 *            SEs to priorize to read from
	 * @param exses
	 *            SEs to depriorize to read from
	 * @return PFNs, filled with read envelopes and credentials if necessary and
	 *         authorized
	 */
	public List<PFN> getPFNsToRead(String site, GUID guid, List<String> ses, List<String> exses) {
		try {
			return Dispatcher.execute(new PFNforReadOrDel(commander.getUser(), commander.getRole(), site, AccessType.READ, guid, ses, exses), true).getPFNs();
		} catch (ServerException e) {
			System.out.println("Could not get PFN for: " + guid);
			e.getCause().printStackTrace();
		}
		return null;
	}

	/**
	 * Get PFNs for writing by LFN
	 * 
	 * @param site
	 *            site the user is matched to
	 * @param lfn
	 *            LFN of the entry as String
	 * @param guid 
	 * @param ses
	 *            SEs to priorize to read from
	 * @param exses
	 *            SEs to depriorize to read from
	 * @param qosType
	 *            QoS type to ask for
	 * @param qosCount
	 *            QoS count of the type to ask for
	 * @return PFNs, filled with write envelopes and credentials if necessary
	 *         and authorized
	 */
	public List<PFN> getPFNsToWrite(String site, LFN lfn, GUID guid, List<String> ses, List<String> exses, String qosType, int qosCount) {
		try {
			return Dispatcher.execute(new PFNforWrite(commander.getUser(), commander.getRole(), site, lfn, guid, ses, exses, qosType, qosCount), true).getPFNs();
		}
		catch (ServerException e) {
			System.out.println("Could not get PFN for: " + lfn);
			e.getCause().printStackTrace();
		}
		
		return null;
	}

	/**
	 * Get PFNs for writing by GUID
	 * 
	 * @param site
	 *            site the user is matched to
	 * @param guid
	 *            GUID of the entry as String
	 * @param ses
	 *            SEs to prioritize to read from
	 * @param exses
	 *            SEs to deprioritize to read from
	 * @param qosType
	 *            QoS type to ask for
	 * @param qosCount
	 *            QoS count of the type to ask for
	 * @return PFNs, filled with write envelopes and credentials if necessary
	 *         and authorized
	 */
	public List<PFN> getPFNsToWrite(String site, GUID guid, List<String> ses, List<String> exses, String qosType,
		int qosCount) {
		try {
			return Dispatcher.execute(new PFNforWrite(commander.getUser(), commander.getRole(), site, guid, ses, exses, qosType, qosCount),true).getPFNs();
		}
		catch (ServerException e) {
			System.out.println("Could not get PFN for: " + guid);
			e.getCause().printStackTrace();
		}
		return null;
	}

	/**
	 * Register PFNs with envelopes
	 * 
	 * @param envelopes
	 * @return PFNs that were successfully registered
	 */
	public List<PFN> registerEnvelopes(final List<String> envelopes) {
		try {
			return Dispatcher.execute(new RegisterEnvelopes(commander.getUser(), commander.getRole(), envelopes), true).getPFNs();
		}
		catch (ServerException e) {
			System.out.println("Could not get PFNs for: "+ envelopes.toString());
			e.getCause().printStackTrace();
		}
		
		return null;
	}

	/**
	 * Register PFNs with enveloeps
	 * 
	 * @param encryptedEnvelope
	 * @param size 
	 * @param md5 
	 * @param lfn 
	 * @param perm 
	 * @param expire 
	 * @param pfn 
	 * @param se 
	 * @param guid 
	 * @return PFNs that were successfully registered
	 */
	public List<PFN> registerEncryptedEnvelope(String encryptedEnvelope, int size, String md5, String lfn, String perm, String expire, String pfn, String se, String guid) {
		try {
			return Dispatcher.execute(new RegisterEnvelopes(commander.getUser(), commander.getRole(), encryptedEnvelope, size, md5, lfn, perm, expire, pfn, se, guid), true).getPFNs();
		}
		catch (ServerException e) {
			System.out.println("Could not get PFNs for: " + encryptedEnvelope);
			e.getCause().printStackTrace();
		}
		
		return null;
	}

	/**
	 * Create a directory in the Catalogue
	 * 
	 * @param path
	 * @return LFN of the created directory, if successful, else
	 *         <code>null</code>
	 */
	public LFN createCatalogueDirectory(String path) {
		return createCatalogueDirectory(path, false);
	}

	/**
	 * Create a directory in the Catalogue
	 * 
	 * @param path
	 * @param createNonExistentParents
	 * @return LFN of the created directory, if successful, else
	 *         <code>null</code>
	 */
	public LFN createCatalogueDirectory(String path, boolean createNonExistentParents) {
		try {
			return Dispatcher.execute(new CreateCatDirfromString(commander.getUser(), commander.getRole(), path,createNonExistentParents), true).getDir();
		}
		catch (ServerException e) {
			System.out.println("Could not create the CatDir: " + path);
			e.getCause().printStackTrace();
		}
		
		return null;
	}

	/**
	 * Remove a directory in the Catalogue
	 * @param path
	 * @return state of directory's deletion
	 *         <code>null</code>
	 */
	public boolean removeCatalogueDirectory(final String path) {
		try {
			return Dispatcher.execute(new RemoveCatDirfromString(commander.getUser(), commander.getRole(), path), true).wasRemoved();
		} catch (ServerException e) {
			System.out.println("Could not remove the CatDir: " + path);
			e.getCause().printStackTrace();
		}
		
		return false;
	}
	
	
	/**
	 * find bases on pattern
	 * 
	 * @param path
	 * @param pattern
	 * @param flags
	 * @return result LFNs
	 */
	public List<LFN> find(String path, String pattern, int flags) {
		try {
			return Dispatcher.execute(new FindfromString(commander.getUser(), commander.getRole(), path, pattern, flags), true).getLFNs();
		}
		catch (ServerException e) {
			System.out.println("Unable to execute find: path (" + path + "), pattern (" + pattern + "), flags (" + flags + ")");
			e.getCause().printStackTrace();
		}
		
		return null;
	}

	/**
	 * Get an SE by its name
	 * 
	 * @param se
	 *            name of the SE
	 * @return SE object
	 */
	public SE getSE(String se) {
		try {
			return Dispatcher.execute(new SEfromString(commander.getUser(), commander.getRole(), se), true).getSE();
		}
		catch (ServerException e) {
			System.out.println("Could not get SE: " + se);
			e.getCause().printStackTrace();
		}
		
		return null;
	}

	/**
	 * Get an SE by its number
	 * 
	 * @param seno
	 *            number of the SE
	 * @return SE object
	 */
	public SE getSE(int seno) {
		try {
			return Dispatcher.execute(new SEfromString(commander.getUser(), commander.getRole(), seno), true).getSE();
		}
		catch (ServerException e) {
			System.out.println("Could not get SE: " + seno);
			e.getCause().printStackTrace();
		}
		
		return null;
	}

}
