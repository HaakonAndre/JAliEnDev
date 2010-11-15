package alien.services;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import alien.catalogue.PFN;
import alien.catalogue.access.CatalogueAccess;
import alien.catalogue.access.XrootDEnvelope;
import alien.se.SE;
import alien.se.SEUtils;

/**
 * @author Steffen
 * @since Nov 14, 2010
 */
public class CatalogueAccessEnvelopeDecorator {

	/**
	 * @param ca
	 * @param sitename
	 * @param qos
	 * @param qosCount
	 * @param staticSEs
	 * @param exxSes
	 */
	public static void loadXrootDEnvelopesForCatalogueAccessV218(
			CatalogueAccess ca, String sitename, String qos, int qosCount,
			Set<SE> staticSEs, Set<SE> exxSes) {

		if (ca.getAccess() == CatalogueAccess.READ
				|| ca.getAccess() == CatalogueAccess.DELETE) {

			
			ca.loadPFNS();
			
			final Set<PFN> whereis = ca.getPFNS();

			// keep all replicas, even if the SE is reported as broken (though it will go to the end of list)
			final List<PFN> sorted = SEUtils.sortBySite(whereis, sitename, false);
			
			// remove the replicas from these SEs, if specified
			if (exxSes!=null && exxSes.size()>0){
				final Iterator<PFN> it = sorted.iterator();
				
				while (it.hasNext()){
					final PFN pfn = it.next();
					
					final SE se = SEUtils.getSE(pfn.seNumber);
					
					if (exxSes.contains(se))
						it.remove();
				}
			}
			
//			getEnvelopesforPFNList(ca, sorted);
			if(sorted.iterator().hasNext())
				ca.addEnvelope(new XrootDEnvelope(ca,  sorted.iterator().next()));
		}
		else if (ca.getAccess() == CatalogueAccess.WRITE) {

			final List<SE> ses = new LinkedList<SE>();
			
			if (staticSEs != null)
				ses.addAll(staticSEs);
			
			filterSEs(ses, qos, exxSes);
			
			if (ses.size()>=qosCount){
				final List<SE> dynamics = SEUtils.getClosestSEs(sitename);
				
				filterSEs(dynamics, qos, exxSes);

				final Iterator<SE> it = dynamics.iterator();
				
				while (ses.size() < qosCount && it.hasNext())
					ses.add(it.next());
			}
			
			getEnvelopesforSEList(ca, ses);
		}
	}
	
	private static final void filterSEs(final List<SE> ses, final String qos, final Set<SE> exxSes){
		final Iterator<SE> it = ses.iterator();
		
		while (it.hasNext()){
			final SE se = it.next();
			
			if (!se.qos.contains(qos) || exxSes.contains(se))
				it.remove();
		}
	}
	
	private static void getEnvelopesforPFNList(final CatalogueAccess ca, final Collection<PFN> pfns) {
		for (final PFN pfn: pfns) {
			ca.addEnvelope(new XrootDEnvelope(ca, pfn));
		}
	}

	
	private static void getEnvelopesforSEList(final CatalogueAccess ca, final Collection<SE> ses) {
		for (final SE se : ses) {
			ca.addEnvelope(new XrootDEnvelope(ca, se, se.seioDaemons + se.seStoragePath + ca.getGUID().toString()));
		}
	}
}
