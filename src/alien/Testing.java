package alien;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

import lazyj.Format;
import lazyj.Utils;
import utils.XRDChecker;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.io.protocols.XRDStatus;
import alien.io.xrootd.XrootdCleanup;
import alien.io.xrootd.XrootdFile;
import alien.io.xrootd.XrootdListing;
import alien.se.SE;
import alien.se.SEUtils;

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
		
		final long lStart = System.currentTimeMillis();
		
//		SE se = SEUtils.getSE("ALICE::Trieste::SE");
		
		XrootdCleanup cleanup = new XrootdCleanup("ALICE::Trieste::SE", false);
		
//		XrootdListing listing = new XrootdListing("alired.ts.infn.it:1094", "/14/35947/");
//		
//		for (XrootdFile f: listing.getFiles()){
//			if (f.getName().equals("afa8fe42-3c2a-11e0-b760-0018fe730ae5")){
//				System.err.println(f);
//				
//				System.err.println(XrootdCleanup.removeFile(f, se));
//			}
//		}
		
		System.err.println(cleanup+", took "+Format.toInterval(System.currentTimeMillis() - lStart));
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
