package alien;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.logging.Logger;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.DBFunctions;
import lazyj.Format;
import lazyj.Utils;
import utils.XRDChecker;
import alien.catalogue.BookingTable;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.io.Transfer;
import alien.io.TransferBroker;
import alien.io.protocols.XRDStatus;
import alien.io.xrootd.XrootdCleanup;
import alien.io.xrootd.XrootdFile;
import alien.io.xrootd.XrootdListing;
import alien.se.SE;
import alien.se.SEUtils;
import alien.user.UserFactory;

/**
 * Testing stuff
 * 
 * @author costing
 *
 */
public class Testing {
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(Testing.class.getCanonicalName());
		
	private static final void performTransfer(final int transferId){
		
		final DBFunctions db = ConfigUtils.getDB("transfers");
		
		db.query("select lfn,destination from TRANSFERS_DIRECT where transferId="+transferId+";");
		
		String sLFN = db.gets(1);
		String targetSE = db.gets(2);
		
		final LFN lfn = LFNUtils.getLFN(sLFN);
		
		if (!lfn.exists){
			System.err.println("LFN doesn't exist : "+lfn);
			return;
		}
		
		if (lfn.guid==null){
			System.err.println("GUID is null for this LFN");
			return;
		}
		
		final GUID guid = GUIDUtils.getGUID(lfn.guid);
		
		if (guid==null){
			System.err.println("GUID was not found in the database");
			return;
		}
		
		final SE se = SEUtils.getSE(targetSE);
		
		if (se==null){
			System.err.println("Target SE doesn't exist");
			return;
		}
		
		final Set<PFN> pfns = lfn.whereisReal();
		
		if (pfns==null || pfns.size()==0){
			System.err.println("No replicas to mirror");
			return;
		}
		
		for (final PFN pfn: pfns){
			if (pfn.seNumber == se.seNumber){
				System.err.println("There is already a replica on this storage");
				return;
			}
		}
		
		final StringTokenizer st = new StringTokenizer(targetSE, ":");
		
		st.nextToken();
		final String site = st.nextToken();
		
		final List<PFN> sortedPFNs = SEUtils.sortBySite(pfns, site, false);
		
		for (final PFN source: sortedPFNs){
			final String reason = AuthorizationFactory.fillAccess(source, AccessType.READ);
		
			if (reason!=null){
				System.err.println("Source authorization failed: "+reason);
				return;
			}
		}
				
		final PFN target;
		
		try{
			target = BookingTable.bookForWriting(lfn, guid, null, 0, se);
		}
		catch (IOException ioe){
			final String reason = ioe.getMessage();
			System.err.println("Target authorization failed: "+reason);
			return;
		}
		
		final Transfer t = new Transfer(transferId, sortedPFNs, target);
		
		t.run();
		
		System.err.println(t);
		
		TransferBroker.getInstance().notifyTransferComplete(t);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		//removeFZK();
			
		//XrootdCleanup.main(new String[]{"ALICE::CyberSar_Cagliari::SE", "-t", "100"});
		
		if (true){
			//performTransfer(28107486);
			
			BufferedReader br = new BufferedReader(new FileReader("/home/costing/temp/list.txt"));
			
			String sLine;
			
			long totalsize = 0;
			
			while ( (sLine=br.readLine()) != null ){
				if (sLine.startsWith("/")){			
					LFN l = LFNUtils.getLFN(sLine);
					
					System.err.println(sLine+"\t\t"+Format.size(l.size));
					
					totalsize += l.size;
				}
			}
			
			System.err.println("Total size : "+totalsize+" ("+Format.size(totalsize)+")");
			
			return;
		}
		
		if (true){
			LFN c = LFNUtils.createCollection("/alice/cern.ch/user/g/grigoras/testCollection", UserFactory.getByUsername("grigoras"));
			
			System.err.println("Create collection : "+c);
			
			LFN l1 = LFNUtils.getLFN("/alice/cern.ch/user/g/grigoras/myNewFile");
			LFN l2 = LFNUtils.getLFN("/alice/cern.ch/user/g/grigoras/run_selection.xml");
			LFN l3 = LFNUtils.getLFN("/alice/cern.ch/user/g/grigoras/test");
			
			Set<LFN> toAdd = new LinkedHashSet<LFN>();
			toAdd.add(l1);
			toAdd.add(l2);
			toAdd.add(l3);
			
			System.err.println("Add lfns : "+LFNUtils.addToCollection(c, toAdd));
			
			System.err.println("After adding : "+c);
			
			System.err.println("Contains : "+c.listCollection());
			//LFN lfn = LFNUtils.getLFN("/alice/data/2011/LHC11c/000154138/collection");
			
			GUID g = GUIDUtils.getGUID(c.guid);

			System.err.println("GUID is in : "+g.seStringList);
			
			//System.err.println(lfn);
			
			Set<LFN> toRemove = new LinkedHashSet<LFN>();
			toRemove.add(l3);
			toRemove.add(l1);
			
			System.err.println("Remove lfns: "+LFNUtils.removeFromCollection(c, toRemove));
			
			System.err.println("After removing : "+c);
			
			System.err.println("Contains : "+c.listCollection());

			g = GUIDUtils.getGUID(c.guid);

			System.err.println("GUID is in : "+g.seStringList);

			return;
		}
		
		if (true){
			String spfn = "root://pcaliense01.cern.ch:1094//01/19968/a1784140-8c50-11e0-9ec0-0019bbc62419";
			
			PFN pfn = BookingTable.getBookedPFN(spfn);
			
			System.err.println("Booked pfn : "+pfn);
			
			BookingTable.commit(UserFactory.getByUsername("sschrein"), pfn);
			
			return;
		}
		
		if (true){
			List<LFN> found = LFNUtils.find("/alice/data/2011/LHC11b/000149656/ESDs/pass1", "AliESDs.root", 0);
						
			long lStart = System.currentTimeMillis();
			
			found = LFNUtils.find("/alice/data/2011/LHC11b/000149656/ESDs/pass1", "AliESDs.root", 0);
						
			System.err.println("done : "+(System.currentTimeMillis() - lStart)+" : "+found.size());
			
			return;
		}
		
		XrootdListing listing = new XrootdListing("pcaliense01.cern.ch:1094", "/00");

		final SE se = SEUtils.getSE("ALICE::CERN::SETEST");
		
		List<XrootdFile> files = new ArrayList<XrootdFile>();
		
		for (XrootdFile f: listing.getDirs()){
			
			if (!f.path.startsWith("/00/2"))
				continue;
			
			XrootdListing listing2 = new XrootdListing("pcaliense01.cern.ch:1094", f.path);
			
			files.addAll(listing2.getFiles());
		}

		int cnt = 0;
		
		for (final XrootdFile f: files){
			if (!f.path.endsWith(".md5")){
				cnt++;
				System.err.println(f.path);
				System.err.println(XrootdCleanup.removeFile(f, se));
				
//				new Thread(){
//					@Override
//					public void run() {
//						System.err.println(XrootdCleanup.removeFile(f, se));
//					}
//				}.start();
			}
		}
		
		System.err.println("finish : "+cnt);
	}
		
	private static void checkOptions() {
		OptionParser parser = new OptionParser();
		
		parser.accepts("part", "Should be in parts");
		parser.accepts("p", "argument p doc");
		parser.accepts("a", "argument a");
		parser.accepts("l", "argument l");
		parser.accepts("h", "for help");
		
		final OptionSet options = parser.parse(new String[]{"-part", "-pl", "-a", "bubu", "-h"});
		
		for (String s: Arrays.asList("part", "p", "a", "l")){
			System.err.println("Has "+s+" : "+options.has(s));
		}
		
		if (options.has("h")){
			try {
				parser.printHelpOn(System.out);
			}
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private static void removeFZK() throws IOException {
		final BufferedReader br = new BufferedReader(new StringReader(Utils.download("http://www-ekp.physik.uni-karlsruhe.de/~jung/errors_gka5407_gka5407", null)));
			
		String sLine;
		
		SE se = SEUtils.getSE("ALICE::FZK::SE");
		
		int ok = 0;
		int missing = 0;
		int noreplica = 0;
		
		while ( (sLine=br.readLine())!=null ){
			//System.err.println(sLine);
			
			final int idx = sLine.lastIndexOf('%');
			
			System.err.print((ok+missing+noreplica)+" : ");
			
			if (idx>0){
				String sGUID = sLine.substring(idx+1).trim();
				
				GUID guid = GUIDUtils.getGUID(UUID.fromString(sGUID));
				
				if (guid==null){
					System.err.println("GUID "+sGUID+" doesn't exist in the catalogue");
					missing ++;
				}
				else{
					String removedPFN = guid.removePFN(se);
					
					if (removedPFN==null){
						System.err.println("GUID "+sGUID+" didn't have a replica on ALICE::FZK::SE");
						noreplica ++;
					}
					else{
						System.err.println("GUID "+sGUID+" successfully removed a copy from ALICE::FZK::SE");
						ok ++;
					}
				}
			}
		}
		
		System.err.println("OK : "+ok);
		System.err.println("Missing : "+missing);
		System.err.println("No such replica : "+noreplica);
	}
	
	private static final void xrdstat(){
		// "/alice/data/2010/LHC10h/000139513/ESDs/pass2/QA65/Stage_2/002/QAresults.root"
		final Map<PFN, XRDStatus> check = XRDChecker.fullCheckLFN("/alice/data/2010/LHC10h/000139466/ESDs/pass2/QA65/Stage_4/001/QAresults.root");
		
		// /alice/data/2010/LHC10h/000139466/ESDs/pass2/QA65/Stage_4/006/QAresults.root
		
		for (final Map.Entry<PFN, XRDStatus> entry: check.entrySet()){
			System.err.println(entry.getKey().pfn+" : "+entry.getValue());
		}
	}
	
	private static final void compareFiles(final String f1, final String f2) throws IOException {
		
		final InputStream is1 = new FileInputStream(f1);
		final InputStream is2 = new FileInputStream(f2);
		
		final byte[] b1 = new byte[256*1024];
		final byte[] b2 = new byte[256*1024];
		
		long pos = 0;
		
		int r;
		
		long bad_start = -1;

		int count = 0;
		
		final int ok_threshold = 1024;
		
		long ok_start = -1;
		
		long diffbytes = 0;
		
		do {
			r = is1.read(b1);
			
			if (r>0){
				int r2 = is2.read(b2, 0, r);
			
				if (r2!=r){
					System.err.println("f2 is smaller than f1!");
					return;
				}
				
				for (int i=0; i<r; i++){
					if (b1[i] == b2[i]){
						if (bad_start>=0){
							if (ok_start<0)
								ok_start = pos+i;
			
							if (pos+i-ok_start >= ok_threshold){
								diffbytes += (ok_start - bad_start);
							
								System.err.println("Bad range: "+bad_start+" .. "+(ok_start-1)+" ("+(ok_start-bad_start)+" bytes)");
								bad_start = -1;
								ok_start = -1;
							
								count ++;
							}
						}
					}
					else{
						System.err.println((pos+i)+" : ok="+( b1[i] & 0xFF) +" - bad="+( b2[i] & 0xFF));
						
						if (bad_start<0)
							bad_start = pos+i;
						
						ok_start = -1;
					}
				}
			}
			
			pos += r;
		}
		while (r>0);
		
		System.err.println("Different blocks : "+count+", bytes : "+diffbytes);
	}
}
