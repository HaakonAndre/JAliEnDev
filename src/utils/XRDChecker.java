package utils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.io.IOUtils;
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
	
	/**
	 * @param pfn
	 * @return the check status
	 */
	public static final XRDStatus checkByDownloading(final PFN pfn){
		final Xrootd xrootd = new Xrootd();
		
		xrootd.setTimeout(60);
		
		File f = null;
		
		final GUID guid = pfn.getGuid();
		
		try{
			f = File.createTempFile("xrdstatus-", "-download.tmp");
		
			f.delete();
			
			final long lStart = System.currentTimeMillis();
			
			System.err.println("Getting this file "+pfn.pfn);
			
			xrootd.get(pfn, f);
			
			System.err.println("Got the file in "+(System.currentTimeMillis() - lStart)/1000+" seconds");
			
			if (f.length() != guid.size){
				return new XRDStatus(false, "Size is different: catalog="+guid.size+", downloaded size: "+f.length());
			}
			
			final String fileMD5 = IOUtils.getMD5(f);
			
			if (!fileMD5.equalsIgnoreCase(guid.md5)){
				return new XRDStatus(false, "MD5 is different: catalog="+guid.md5+", downloaded file="+fileMD5);
			}
		}
		catch (IOException ioe){
			return new XRDStatus(false, ioe.getMessage());
		}
		finally{
			if (f!=null)
				f.delete();
		}
		
		return new XRDStatus(true, null);
	}
	
	/**
	 * Check all replicas of an LFN, first just remotely querying the status then fully downloading each
	 * replica and computing the md5sum.
	 * 
	 * @param lfn
	 * @return the status of all replicas
	 */
	public static final Map<PFN, XRDStatus> fullCheckLFN(final String lfn){
		final Map<PFN, XRDStatus> check = XRDChecker.check(LFNUtils.getLFN(lfn));

		if (check==null || check.size()==0)
			return check;
		
		final Iterator<Map.Entry<PFN, XRDStatus>> it = check.entrySet().iterator();
		
		while (it.hasNext()){
			final Map.Entry<PFN, XRDStatus> entry = it.next();
			
			final PFN pfn = entry.getKey();
			
			final XRDStatus status = entry.getValue();
			
			if (status.commandOK){
				// really ?
				final XRDStatus downloadStatus = checkByDownloading(pfn);
				
				if (!downloadStatus.commandOK)
					entry.setValue(downloadStatus);
			}
		}
		
		return check;
	}
	
}
