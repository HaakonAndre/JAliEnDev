package alien.services;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import lia.Monitor.modules.XrootdServerMonitor;
import alien.catalogue.PFN;
import alien.catalogue.access.CatalogueAccess;
import alien.catalogue.access.XrootDEnvelope;
import alien.se.SE;
import alien.se.SEUtils;

public class CatalogueAccessEnvelopeDecorator {

	public static void loadXrootDEnvelopesForCatalogueAccessV218(
			CatalogueAccess ca, String sitename, String qos, int qosCount,
			Set<String> staticSEs, Set<String> exxSes) {

		if (ca.getAccess() == CatalogueAccess.READ
				|| ca.getAccess() == CatalogueAccess.DELETE) {

			ca.loadPFNS();
			Hashtable<SE, PFN> whereis = getSEsForPFNs(ca.getPFNS());

			Hashtable<SE, PFN> existingSEs = returnExisting(whereis, staticSEs);

			if (existingSEs.isEmpty()) {

				Hashtable<SE, PFN> closestExistingSEs = 
						sortSEsBySiteAndexcludeSpecifiedSEs(whereis, exxSes, sitename);
				getEnvelopesforSEList(ca, closestExistingSEs);

			} else {
				getEnvelopesforSEList(ca, existingSEs);
			}

		} else if (ca.getAccess() == CatalogueAccess.WRITE) {

			List<SE> statics = (List<SE>) SEUtils.getSEs(staticSEs);

			List<SE> dynamics = SEUtils.getQOSNClosestSEsNotExSEs(sitename,
					qos, qosCount, exxSes);

			getEnvelopesforSEList(ca, statics);
			getEnvelopesforSEList(ca, dynamics);
		}

	}

	private static void getEnvelopesforSEList(CatalogueAccess ca, Hashtable<SE, PFN> thereis) {
		for (SE se : thereis.keySet()) {
			ca.addEnvelope(new XrootDEnvelope(ca, thereis.get(se)));
		}
	}

	
	private static void getEnvelopesforSEList(CatalogueAccess ca, List<SE> ses) {
		for (SE se : ses) {
			
			ca.addEnvelope(new XrootDEnvelope(ca, se, se.seioDaemons + se.seStoragePath + ca.getGUID().toString()));
		}
	}
	
	
	

	private static Hashtable<SE, PFN> sortSEsBySiteAndexcludeSpecifiedSEs(
			Hashtable<SE, PFN> whereis, Set<String> exses, String sitename) {
		
		final List<SE> existing = new ArrayList<SE>();
		
		Hashtable<SE, PFN> thereis = new Hashtable<SE, PFN>();

		for (String sename : exses) {
			SE se = SEUtils.getSE(sename);
			if (!whereis.containsKey(se.seNumber))
				existing.add(se);
		}

		final List<SE> sorted = SEUtils.sortSEsBySite(existing, sitename);
		for (SE se : sorted) {
			thereis.put(se, whereis.get(se));
		}

		return thereis;

	}

	private static Hashtable<SE, PFN> returnExisting(
			Hashtable<SE, PFN> whereis, Set<String> ses) {

		Hashtable<SE, PFN> thereis = new Hashtable<SE, PFN>();

		for (String sename : ses) {
			SE se = SEUtils.getSE(sename);
			if (whereis.containsKey(se.seNumber))
				thereis.put(se, whereis.get(se));
		}

		return thereis;

	}

	private static Hashtable<SE, PFN> getSEsForPFNs(Set<PFN> pfns) {
		Hashtable<SE, PFN> whereis = new Hashtable<SE, PFN>();

		for (PFN pfn : pfns)
			whereis.put(SEUtils.getSE(pfn.seNumber), pfn);

		return whereis;

	}
}
