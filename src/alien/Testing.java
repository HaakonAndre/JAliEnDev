package alien;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import lazyj.Utils;
import utils.XRDChecker;
import alien.catalogue.BookingTable;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
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
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		//removeFZK();
			
		//XrootdCleanup.main(new String[]{"ALICE::CyberSar_Cagliari::SE", "-t", "100"});
		
		if (true){
			LFN lfn = LFNUtils.getLFN("/alice/data/2011/LHC11c/000154138/collection");
			
			System.err.println(lfn);
			
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
						System.err.println((pos+i)+" : ok="+((int) b1[i] & 0xFF) +" - bad="+((int) b2[i] & 0xFF));
						
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
