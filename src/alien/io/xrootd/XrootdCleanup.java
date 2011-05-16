package alien.io.xrootd;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import lazyj.Format;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.PFN;
import alien.se.SE;
import alien.se.SEUtils;

/**
 * @author costing
 *
 */
public class XrootdCleanup {
	private final SE se;
	
	private final String server;
	
	private long sizeRemoved = 0;
	private long sizeKept = 0;
	private long filesRemoved = 0;
	private long filesKept = 0;
	private long dirsSeen = 0;
	
	/**
	 * Check all GUID files in this storage by listing recursively its contents.
	 * 
	 * @param sSE
	 */
	public XrootdCleanup(final String sSE){
		se = SEUtils.getSE(sSE);
		
		if (se==null){
			server = null;
			
			System.err.println("No such SE "+sSE);
			
			return;
		}		
		
		String sBase = se.seioDaemons;
		
		if (sBase.startsWith("root://"))
			sBase = sBase.substring(7);
		
		server = sBase;
		
		storageCleanup("/");
	}
	
	private void storageCleanup(final String path){
		System.err.println("storageCleanup: "+path);
		
		dirsSeen++;
		
		try{
			final XrootdListing listing = new XrootdListing(server, path);
			
			for (XrootdFile file: listing.getFiles()){
				fileCheck(file);
			}
			
			for (XrootdFile dir: listing.getDirs()){
				if (dir.getName().matches("^\\d+$"))
					storageCleanup(dir.path);
			}
		}
		catch (IOException ioe){
			System.err.println(ioe.getMessage());
		}
	}
	
	private boolean removeFile(final XrootdFile file){
		System.err.println("RM "+file);
		
		return true;
	}
	
	private void fileCheck(final XrootdFile file) {
		try{
			final UUID uuid = UUID.fromString(file.getName());
			
			final GUID guid = GUIDUtils.getGUID(uuid);
			
			boolean remove = false;
			
			if (guid==null){
				remove = true;
			}
			else{
				final Set<PFN> pfns = guid.getPFNs();
				
				if (pfns==null || pfns.size()==0)
					remove = true;
				else{
					boolean found = false;
					
					for (final PFN pfn: pfns){
						if (pfn.seNumber == se.seNumber){
							found = true;
							break;
						}
					}
					
					remove = !found;
				}
			}
			
			if (remove && removeFile(file)){
				sizeRemoved += file.size;
				filesRemoved ++;
			}
			else{
				sizeKept += file.size;
				filesKept ++;
			}
		}
		catch (Exception e){
			// ignore
		}
	}
	
	@Override
	public String toString() {
		return "Removed "+filesRemoved+" files ("+Format.size(sizeRemoved)+"), kept "+filesKept+" ("+Format.size(sizeKept)+"), "+dirsSeen+" directories";
	}
	
}
