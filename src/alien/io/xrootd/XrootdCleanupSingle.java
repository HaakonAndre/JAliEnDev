package alien.io.xrootd;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.DBFunctions;
import lazyj.Format;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.se.SE;
import alien.se.SEUtils;

/**
 * Single threaded listing of Xrootd server content and injecting in the deletion queue the files that should not be there
 * 
 * @author costing
 *
 */
public class XrootdCleanupSingle {
	/**
	 * Storage element we are working on
	 */
	final SE se;
	
	private final String server;

	private final AtomicLong sizeRemoved = new AtomicLong();
	private final AtomicLong sizeKept = new AtomicLong();
	private final AtomicLong sizeFailed = new AtomicLong();
	private final AtomicLong filesRemoved = new AtomicLong();
	private final AtomicLong filesKept = new AtomicLong();
	private final AtomicLong filesFailed = new AtomicLong();
	private final AtomicLong dirsSeen = new AtomicLong();
	
	/**
	 * How many items are currently in progress
	 */
	final AtomicInteger inProgress = new AtomicInteger(0);
	
	/**
	 * how many files were processed so far
	 */
	final AtomicInteger processed = new AtomicInteger(0);
	
	/**
	 * Check all GUID files in this storage by listing recursively its contents.
	 * 
	 * @param sSE
	 */
	public XrootdCleanupSingle(final String sSE){
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
	}

	/**
	 * @param path
	 */
	void storageCleanup(final String path){
		System.err.println("storageCleanup: "+path);
		
		dirsSeen.incrementAndGet();
		
		try{
			final boolean setSE = se.getName().toLowerCase().contains("dcache");  
			
			final XrootdListing listing = new XrootdListing(server, path, setSE ? se : null) ;
						
			for (final XrootdFile file: listing.getFiles()){
				fileCheck(file);
			}
			
			for (final XrootdFile dir: listing.getDirs()){
				if (dir.path.matches("^/\\d{2}(/\\d{5})?$")){
					storageCleanup(dir.path);
				}
			}
		}
		catch (IOException ioe){
			System.err.println(ioe.getMessage());
			ioe.printStackTrace();
		}
	}
	
	private void fileCheck(final XrootdFile file) {
		try{
			if (System.currentTimeMillis() - file.date.getTime() < 1000*60*60*24){
				// ignore very recent files
				return;
			}
			
			final UUID uuid;
			
			try{
				uuid = UUID.fromString(file.getName());
			}
			catch (Exception e){
				// not an alien file name, ignore
				return;
			}
			
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
						if (se.equals(pfn.getSE())){
							found = true;
							break;
						}
					}
					
					remove = !found;
				}
			}
			
			if (remove){
				if (removeFile(file)){
					sizeRemoved.addAndGet(file.size);
					filesRemoved.incrementAndGet();
				}
				else{
					sizeFailed.addAndGet(file.size);
					filesFailed.incrementAndGet();
				}
			}
			else{
				sizeKept.addAndGet(file.size);
				filesKept.incrementAndGet();
			}
		}
		catch (Exception e){
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		
		processed.incrementAndGet();
	}
	
	// B6B6EF58-4000-11E0-9CE5-001F29EB8B98
	private static final Pattern UUID_PATTERN = Pattern.compile(".*([0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}).*");
	
	private boolean removeFile(final XrootdFile file){
		final Matcher m = UUID_PATTERN.matcher(file.getName());

		final UUID uuid;

		if (m.matches())
			uuid = UUID.fromString(m.group(1));
		else
			return false;

		System.err.println("RM "+uuid+" FROM "+se.seName+", "+file.size+" ("+Format.size(file.size)+"), "+file.date);
		
		final DBFunctions db = ConfigUtils.getDB("alice_users");
		
		db.query("INSERT IGNORE INTO orphan_pfns (guid,se,size) VALUES (string2binary(?), ?, ?);", false, uuid.toString(), Integer.valueOf(se.seNumber), Long.valueOf(file.size));
		
		db.close();
		
		return true;
	}
	
	@Override
	public String toString() {
		return "Removed "+filesRemoved+" files ("+Format.size(sizeRemoved.longValue())+"), "+
			"failed to remove "+filesFailed+" ("+Format.size(sizeFailed.longValue())+"), "+
			"kept "+filesKept+" ("+Format.size(sizeKept.longValue())+"), "+
			dirsSeen+" directories from "+se.seName;
	}
	
	/**
	 * @param args the only argument taken by this class is the name of the storage to be cleaned
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		final OptionParser parser = new OptionParser();
		
		parser.accepts("?", "Print this help");
		parser.accepts("a", "Run on all known SEs");
		
		final OptionSet options = parser.parse(args);
		
		if ((options.nonOptionArguments().size()==0 && !options.has("a")) || options.has("?")){
			parser.printHelpOn(System.out);
			return;
		}
		
		Collection<String> ses;
		
		if (options.has("a")){
			ses = new LinkedList<String>();
			
			for (final SE se: SEUtils.getSEs(null)){
				ses.add(se.getName());
			}
		}
		else{
			ses = options.nonOptionArguments();
		}
		
		final long lStart = System.currentTimeMillis();
		
		for (final String se: ses){
			new Thread(){
				@Override
				public void run() {
					setName(se);
					
					final XrootdCleanupSingle cleanup = new XrootdCleanupSingle(se);
					cleanup.storageCleanup("/");
					System.err.println(cleanup+", took "+Format.toInterval(System.currentTimeMillis() - lStart));					
				}
			}.start();
		}
	}
}
