package utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.io.protocols.XRDStatus;
import alien.io.protocols.Xrootd;

/**
 * @author costing
 *
 */
public class XRDChecker {

	/**
	 * @param guid
	 * @return the status for each PFN of this LFN (the real ones)
	 */
	public static final Map<PFN, XRDStatus> check(final GUID guid){
		if (guid==null)
			return null;
		
		final Set<GUID> realGUIDs = guid.getRealGUIDs();
		
		final Map<PFN, XRDStatus> ret = new HashMap<PFN, XRDStatus>();
		
		final Xrootd xrootd = new Xrootd();
		
		for (final GUID realId: realGUIDs){
			final Set<PFN> pfns = realId.getPFNs();
			
			if (pfns==null)
				continue;
			
			for (final PFN pfn: pfns){
				final String reason = AuthorizationFactory.fillAccess(pfn, AccessType.READ);
				
				if (reason!=null){
					ret.put(pfn, new XRDStatus(false, reason));
					continue;
				}
				
				try{
					final String output = xrootd.xrdstat(pfn, false);
					
					ret.put(pfn, new XRDStatus(true, output));
				}
				catch (IOException ioe){
					ret.put(pfn, new XRDStatus(false, ioe.getMessage()));
					
					ioe.printStackTrace();
				}
			}
		}
		
		return ret;
	}
	
	/**
	 * @param lfn
	 * @return the status for each PFN of this LFN (the real ones)
	 */
	public static final Map<PFN, XRDStatus> check(final LFN lfn){
		if (lfn==null)
			return null;
		
		final GUID guid = GUIDUtils.getGUID(lfn.guid);
		
		if (guid==null)
			return null;
		
		return check(guid);
	}
	
}
