package alien.api.catalogue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.Package;
import alien.catalogue.access.AccessType;
import alien.config.ConfigUtils;
import alien.io.TransferDetails;
import alien.se.SE;
import alien.shell.commands.JAliEnCOMMander;

/**
 *
 * @author ron
 * @since Jun 03, 2011
 */
public class CatalogueApiUtils {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(CatalogueApiUtils.class.getCanonicalName());

	private final JAliEnCOMMander commander;

	/**
	 * @param commander
	 */
	public CatalogueApiUtils(final JAliEnCOMMander commander) {
		this.commander = commander;
	}

	/**
	 * Get LFN from String, only if it exists
	 *
	 * @param slfn
	 *            name of the LFN
	 * @return the LFN object
	 */
	public LFN getLFN(final String slfn) {
		return getLFN(slfn, false);
	}

	/**
	 * Get LFN from String
	 *
	 * @param slfn
	 * @param evenIfDoesntExist
	 * @return the LFN object, that might exist or not (if <code>evenIfDoesntExist = true</code>)
	 */
	public LFN getLFN(final String slfn, final boolean evenIfDoesntExist) {
		final Collection<LFN> ret = getLFNs(Arrays.asList(slfn), false, evenIfDoesntExist);
		return ret != null && ret.size() > 0 ? ret.iterator().next() : null;
	}

	/**
	 * Get LFNs from String as a directory listing, only if it exists
	 *
	 * @param slfn
	 *            name of the LFN
	 * @return the LFN objects
	 */
	public List<LFN> getLFNs(final String slfn) {
		try {
			return Dispatcher.execute(new LFNListingfromString(commander.getUser(), slfn)).getLFNs();
		} catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get LFN: " + slfn);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Get LFN from String
	 *
	 * @param slfn
	 *            name of the LFN
	 * @param ignoreFolders
	 * @param evenIfDoesntExist
	 * @return the LFN object
	 */
	public List<LFN> getLFNs(final Collection<String> slfn, final boolean ignoreFolders, final boolean evenIfDoesntExist) {
		try {
			return Dispatcher.execute(new LFNfromString(commander.getUser(), ignoreFolders, evenIfDoesntExist, slfn)).getLFNs();
		} catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get LFN: " + slfn);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Remove a LFN in the Catalogue
	 *
	 * @param path
	 * @return state of the LFN's deletion <code>null</code>
	 */
	public boolean removeLFN(final String path) {
		try {
			return Dispatcher.execute(new RemoveLFNfromString(commander.getUser(), path, false)).wasRemoved();
		} catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not remove the LFN: " + path);
			e.getCause().printStackTrace();
		}

		return false;
	}

	/**
	 * Move a LFN in the Catalogue
	 *
	 * @param path
	 * @param newpath
	 * @return state of the LFN's deletion <code>null</code>
	 */
	public LFN moveLFN(final String path, final String newpath) {
		try {
			return Dispatcher.execute(new MoveLFNfromString(commander.getUser(), path, newpath)).newLFN();
		} catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not move the LFN-->newLFN: " + path + "-->" + newpath);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Get GUID from String
	 *
	 * @param sguid
	 *            GUID as String
	 * @return the GUID object
	 */
	public GUID getGUID(final String sguid) {
		return getGUID(sguid, false, false);
	}

	/**
	 * Get GUID from String
	 *
	 * @param sguid
	 *            GUID as String
	 * @param evenIfDoesNotExist
	 * @param resolveLFNs
	 *            populate the LFN cache of the GUID object
	 * @return the GUID object
	 */
	public GUID getGUID(final String sguid, final boolean evenIfDoesNotExist, final boolean resolveLFNs) {
		try {
			return Dispatcher.execute(new GUIDfromString(commander.getUser(), sguid, evenIfDoesNotExist, resolveLFNs)).getGUID();
		} catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get GUID: " + sguid);
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
	public Set<PFN> getPFNs(final String sguid) {
		try {
			return Dispatcher.execute(new PFNfromString(commander.getUser(), sguid)).getPFNs();
		} catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get GUID: " + sguid);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Get PFNs for reading by LFN
	 *
	 * @param lfn
	 *            LFN of the entry as String
	 * @param ses
	 *            SEs to prioritize to read from
	 * @param exses
	 *            SEs to deprioritize to read from
	 * @return PFNs, filled with read envelopes and credentials if necessary and authorized
	 */
	public List<PFN> getPFNsToRead(final LFN lfn, final List<String> ses, final List<String> exses) {
		try {

			return Dispatcher.execute(new PFNforReadOrDel(commander.getUser(), commander.getSite(), AccessType.READ, lfn, ses, exses)).getPFNs();
		} catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get PFN for: " + lfn);
			e.getCause().printStackTrace();

		}
		return null;
	}

	/**
	 * Get PFNs for writing by LFN
	 *
	 * @param lfn
	 *            LFN of the entry as String
	 * @param guid
	 * @param ses
	 *            SEs to prioritize to read from
	 * @param exses
	 *            SEs to de-prioritize
	 * @param qos
	 *            QoS types and counts to ask for
	 * @return PFNs, filled with write envelopes and credentials if necessary and authorized
	 */
	public List<PFN> getPFNsToWrite(final LFN lfn, final GUID guid, final List<String> ses, final List<String> exses, final HashMap<String, Integer> qos) {
		try {
			return Dispatcher.execute(new PFNforWrite(commander.getUser(), commander.getSite(), lfn, guid, ses, exses, qos)).getPFNs();
		} catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get PFN for: " + lfn);
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
			final List<String> encryptedEnvelopes = new LinkedList<>();
			final List<String> signedEnvelopes = new LinkedList<>();

			for (final String envelope : envelopes)
				if (envelope.contains("&signature="))
					signedEnvelopes.add(envelope);
				else
					encryptedEnvelopes.add(envelope);

			final List<PFN> ret = new LinkedList<>();

			if (signedEnvelopes.size() > 0) {
				final List<PFN> signedPFNs = Dispatcher.execute(new RegisterEnvelopes(commander.getUser(), envelopes)).getPFNs();

				if (signedPFNs != null && signedPFNs.size() > 0)
					ret.addAll(signedPFNs);
			}

			for (final String envelope : encryptedEnvelopes) {
				final List<PFN> encryptedPFNs = Dispatcher.execute(new RegisterEnvelopes(commander.getUser(), envelope, 0, null)).getPFNs();

				if (encryptedPFNs != null && encryptedPFNs.size() > 0)
					ret.addAll(encryptedPFNs);
			}

			return ret;
		} catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get PFNs for: " + envelopes.toString());
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
	 * @return PFNs that were successfully registered
	 */
	public List<PFN> registerEncryptedEnvelope(final String encryptedEnvelope, final int size, final String md5) {
		try {
			return Dispatcher.execute(new RegisterEnvelopes(commander.getUser(), encryptedEnvelope, size, md5)).getPFNs();
		} catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get PFNs for: " + encryptedEnvelope);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Create a directory in the Catalogue
	 *
	 * @param path
	 * @return LFN of the created directory, if successful, else <code>null</code>
	 */
	public LFN createCatalogueDirectory(final String path) {
		return createCatalogueDirectory(path, false);
	}

	/**
	 *
	 * @param path
	 * @return LFN of the created file, if successful, else <code>null</code>
	 */
	public LFN touchLFN(final String path) {
		try {
			return Dispatcher.execute(new TouchLFNfromString(commander.getUser(), path)).getLFN();
		} catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not create the file: " + path);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Create a directory in the Catalogue
	 *
	 * @param path
	 * @param createNonExistentParents
	 * @return LFN of the created directory, if successful, else <code>null</code>
	 */
	public LFN createCatalogueDirectory(final String path, final boolean createNonExistentParents) {
		try {
			return Dispatcher.execute(new CreateCatDirfromString(commander.getUser(), path, createNonExistentParents)).getDir();
		} catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not create the CatDir: " + path);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Remove a directory in the Catalogue
	 *
	 * @param path
	 * @return state of directory's deletion <code>null</code>
	 */
	public boolean removeCatalogueDirectory(final String path) {
		try {
			return Dispatcher.execute(new RemoveCatDirfromString(commander.getUser(), path)).wasRemoved();
		} catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not remove the CatDir: " + path);
			e.getCause().printStackTrace();
		}

		return false;
	}

	/**
	 * Find an LFN based on pattern
	 *
	 * @param path
	 * @param pattern
	 * @param flags
	 * @return result LFNs
	 */
	public Collection<LFN> find(final String path, final String pattern, final int flags) {
		return find(path, pattern, flags, "");
	}

	/**
	 * Find an LFN based on pattern and save to XmlCollection
	 * 
	 * @param path
	 * @param pattern
	 * @param flags
	 * @param xmlCollectionName
	 * @return result LFNs
	 */
	public Collection<LFN> find(final String path, final String pattern, final int flags, final String xmlCollectionName) {
		return this.find(path, pattern, flags, xmlCollectionName, Long.valueOf(0));
	}

	/**
	 * Find an LFN based on pattern and save to XmlCollection
	 * 
	 * @param path
	 * @param pattern
	 * @param flags
	 * @param xmlCollectionName
	 * @param queueid
	 * @return result LFNs
	 */
	public Collection<LFN> find(final String path, final String pattern, final int flags, final String xmlCollectionName, Long queueid) {
		try {
			return Dispatcher.execute(new FindfromString(commander.getUser(), path, pattern, flags, xmlCollectionName, queueid)).getLFNs();
		} catch (final ServerException e) {
			logger.log(Level.WARNING, "Unable to execute find: path (" + path + "), pattern (" + pattern + "), flags (" + flags + ")");
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
	public SE getSE(final String se) {
		try {
			return Dispatcher.execute(new SEfromString(commander.getUser(), se)).getSE();
		} catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get SE: " + se);
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
	public SE getSE(final int seno) {
		try {
			return Dispatcher.execute(new SEfromString(commander.getUser(), seno)).getSE();
		} catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get SE: " + seno);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Get Packages for a certain platform
	 *
	 * @param platform
	 * @return the Packages
	 */
	public List<Package> getPackages(final String platform) {
		try {
			return Dispatcher.execute(new PackagesfromString(commander.getUser(), platform)).getPackages();
		} catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get Packages for: " + platform);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * @param lfn_name
	 * @param username_to_chown
	 * @param groupname_to_chown
	 * @param recursive
	 * @return command result for each lfn
	 */
	public HashMap<String, Boolean> chownLFN(final String lfn_name, final String username_to_chown, final String groupname_to_chown, final boolean recursive) {
		if (lfn_name == null || lfn_name.length() == 0)
			return null;

		final LFN lfn = this.getLFN(lfn_name);

		if (lfn == null)
			return null;
		try {
			final ChownLFN cl = Dispatcher.execute(new ChownLFN(commander.getUser(), lfn_name, username_to_chown, groupname_to_chown, recursive));
			if (cl != null)
				return cl.getResults();
		} catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not chown " + lfn_name + " for " + username_to_chown);
			e.getCause().printStackTrace();
		}
		return null;
	}

	/**
	 * @param lfn_name
	 * @param ses
	 * @param exses
	 * @param qos
	 * @param useLFNasGuid
	 * @param attempts
	 * @return command result for each lfn
	 */
	public HashMap<String, Integer> mirrorLFN(final String lfn_name, final List<String> ses, final List<String> exses, final HashMap<String, Integer> qos, final boolean useLFNasGuid,
			final Integer attempts) {

		if (lfn_name == null || lfn_name.length() == 0)
			throw new IllegalArgumentException("Empty LFN name");

		try {
			final MirrorLFN ml = Dispatcher.execute(new MirrorLFN(commander.getUser(), lfn_name, ses, exses, qos, useLFNasGuid, attempts));
			return ml.getResultHashMap();
		} catch (final SecurityException e) {
			logger.log(Level.WARNING, e.getMessage());
		} catch (final ServerException e) {
			logger.log(Level.WARNING, e.getMessage());
			e.getCause().printStackTrace();
		}
		return null;
	}

	/**
	 * @param site
	 * @param write
	 * @param lfn
	 * @return SE distance list
	 */
	public List<HashMap<SE, Double>> listSEDistance(final String site, final boolean write, final String lfn) {
		ListSEDistance lsd;
		try {
			lsd = Dispatcher.execute(new ListSEDistance(commander.getUser(), site, write, lfn));
			return (lsd != null ? lsd.getSEDistances() : null);
		} catch (final ServerException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * @param status
	 * @param toSE
	 * @param user
	 * @param id
	 * @param count
	 * @param desc
	 * @return transfer details
	 */
	public List<TransferDetails> listTransfer(final String status, final String toSE, final String user, final Integer id, final int count, final boolean desc) {

		ListTransfer lt;
		try {
			lt = Dispatcher.execute(new ListTransfer(commander.getUser(), status, toSE, user, id, count, desc));
			return (lt != null ? lt.getTransfers() : null);
		} catch (final ServerException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * @param file
	 * @param isGuid
	 * @param se
	 * @return exit code
	 */
	public int deleteMirror(final String file, final boolean isGuid, final String se) {
		try {
			final DeleteMirror dm = Dispatcher.execute(new DeleteMirror(commander.getUser(), file, isGuid, se));
			return dm.getResult();
		} catch (final ServerException e) {
			e.printStackTrace();
			return -100;
		}
	}
}
