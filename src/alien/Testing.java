package alien;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import utils.XRDChecker;

import alien.catalogue.BookingTable;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.catalogue.access.XrootDEnvelope;
import alien.config.ConfigUtils;
import alien.io.Transfer;
import alien.io.TransferBroker;
import alien.io.protocols.Protocol;
import alien.io.protocols.XRDStatus;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.se.SE;
import alien.se.SEUtils;
import alien.services.XrootDEnvelopeSigner;
import alien.taskQueue.Job;
import alien.taskQueue.TaskQueueUtils;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;
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
		//testCatalogue();
		
//		testBT();
		
		//timing();
		
		//testMonitoring();
		
		//testGET();
		
		//rename();

//		Job j = TaskQueueUtils.getJob(81348926);
//		
//		System.err.println(j.getJDL());
		
		xrdstat();
		
		//compareFiles("/tmp/xrdstatus-5741189761428427102-download.tmp.CERN", "/tmp/xrdstatus-6842356284876004826-download.tmp.ISS");
		//compareFiles("/tmp/xrdstatus-5741189761428427102-download.tmp.CERN", "/tmp/xrdstatus-8650443894764062995-download.tmp.NIHAM");
	}
	
	private static final void xrdstat(){
		final Map<PFN, XRDStatus> check = XRDChecker.fullCheckLFN("/alice/data/2010/LHC10h/000139172/ESDs/pass2/AOD044/0049/AliAOD.root");
		
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
	
	private static void rename(){
		for (String s: Arrays.asList("/alice/data/2011/LHC11a/000145455/ESDs/Pass1",
"/alice/data/2011/LHC11a/000145454/ESDs/Pass1",
"/alice/data/2011/LHC11a/000145448/ESDs/Pass1",
"/alice/data/2011/LHC11a/000145385/ESDs/Pass1",
"/alice/data/2011/LHC11a/000145384/ESDs/Pass1",                                                                                                                                     
"/alice/data/2011/LHC11a/000145383/ESDs/Pass1",                                                                                                                                     
"/alice/data/2011/LHC11a/000145379/ESDs/Pass1",                                                                                                                                     
"/alice/data/2011/LHC11a/000145355/ESDs/Pass1",                                                                                                                                     
"/alice/data/2011/LHC11a/000145354/ESDs/Pass1",                                                                                                                                     
"/alice/data/2011/LHC11a/000145353/ESDs/Pass1",                                                                                                                                     
"/alice/data/2011/LHC11a/000145314/ESDs/Pass1",                                                                                                                                     
"/alice/data/2011/LHC11a/000145309/ESDs/Pass1",                                                                                                                                     
"/alice/data/2011/LHC11a/000145300/ESDs/Pass1",                                                                                                                                     
"/alice/data/2011/LHC11a/000145299/ESDs/Pass1",                                                                                                                                     
"/alice/data/2011/LHC11a/000145294/ESDs/Pass1",                                                                                                                                     
"/alice/data/2011/LHC11a/000145292/ESDs/Pass1",                                                                                                                                     
"/alice/data/2011/LHC11a/000145291/ESDs/Pass1",                                                                                                                                     
"/alice/data/2011/LHC11a/000145290/ESDs/Pass1",                                                                                                                                     
"/alice/data/2011/LHC11a/000145289/ESDs/Pass1",                                                                                                                                     
"/alice/data/2011/LHC11a/000145288/ESDs/Pass1",                                                                                                                                     
"/alice/data/2011/LHC11a/000145182/ESDs/Pass1",                                                                                                                                     
"/alice/data/2011/LHC11a/000145180/ESDs/Pass1",                                                                                                                                     
"/alice/data/2011/LHC11a/000145157/ESDs/Pass1",                                                                                                                                     
"/alice/data/2011/LHC11a/000145156/ESDs/Pass1",                                                                                                                                     
"/alice/data/2011/LHC11a/000145145/ESDs/Pass1",                                                                                                                                     
"/alice/data/2011/LHC11a/000145144/ESDs/Pass1",
"/alice/data/2011/LHC11a/000145075/ESDs/Pass1",
"/alice/data/2011/LHC11a/000145074/ESDs/Pass1",
"/alice/data/2011/LHC11a/000145008/ESDs/Pass1",
"/alice/data/2011/LHC11a/000144998/ESDs/Pass1",
"/alice/data/2011/LHC11a/000144991/ESDs/Pass1",
"/alice/data/2011/LHC11a/000144774/ESDs/Pass1"
)){
			LFN lfn = LFNUtils.getLFN(s);
			
			if (lfn!=null){
				System.err.println(lfn);
				System.err.println(lfn.indexTableEntry);
			}
		}
	}
	
	private static void testCatalogue() throws IOException {
		LFN root = LFNUtils.getLFN("/alice");
		
		List<LFN> list = root.list();
		
		for (LFN l: list){
			System.err.println(l.getCanonicalName());
			
			List<LFN> list2 = l.list();
			
			for (LFN l2: list2){
				System.err.println(l2.getCanonicalName());
//				
//				List<LFN> list3 = l2.list();
//				
//				for (LFN l3: list3){
//					System.err.println(l3.getCanonicalName());
//				}
			}
		}
	}
	
	private static void testBT() throws IOException {
		SE se = SEUtils.getSE("ALICE::CERN::ALICEDISK");
		
		File toUpload = new File("/etc/passwd");
		
		AliEnPrincipal user = UserFactory.getByUsername("alidaq");
		
		GUID guid = GUIDUtils.createGuid(toUpload, user);

		System.err.println(guid);
		
		PFN pfn = new PFN(guid, se);
		
		System.err.println(pfn);
		
		LFN lfn = LFNUtils.getLFN("/alice/cern.ch/user/a/alidaq/testjupload", true);
		
		if (!lfn.exists){
			lfn.ctime = guid.ctime;
			lfn.gowner = guid.gowner;
			lfn.owner = guid.owner;
			lfn.guid = guid.guid;
			lfn.md5 = guid.md5;
			lfn.perm = guid.perm;
			lfn.size = guid.size;
			lfn.type = 'f';
		}
		
		System.err.println(lfn.getCanonicalName());
		System.err.println(lfn);
		
		PFN bookedPFN = BookingTable.bookForWriting(user, lfn, guid, pfn, 1234, se);
		
		System.err.println("Got booked pfn: "+bookedPFN);
		
		List<Protocol> protocols = Transfer.getProtocols(pfn);
		
		for (Protocol p: protocols){
			System.err.println("Trying protocol "+p);
			
			try{
				String reply = p.put(bookedPFN, toUpload);
				
				System.err.println("Reply was: "+reply);
				
				break;
			}
			catch (Exception e){
				System.err.println("Got exception uploading: "+e);
				e.printStackTrace();
			}
		}
	}

	public static void timing() throws GeneralSecurityException{
		LFN lfn = LFNUtils.getLFN("/alice/cern.ch/user/a/alidaq/LHC10h/rec_pass16.jdl");
		
		GUID guid = GUIDUtils.getGUID(lfn.guid);
		
		Set<PFN> pfns = guid.getPFNs();

		PFN pfn = pfns.iterator().next();
		
		XrootDEnvelope env =  new XrootDEnvelope(AccessType.READ, pfn);
		
		for (int i=0; i<100; i++){
			final long start = System.currentTimeMillis();
			for (int j=0; j<10000; j++){
				XrootDEnvelopeSigner.encryptEnvelope(env);
				//XrootDEnvelopeSigner.signEnvelope(env);
			}
			
			System.err.println(i+" : "+(System.currentTimeMillis() - start));
		}
	}
	
	private static void testGET(){
		//LFN lfn = LFNUtils.getLFN("/alice/cern.ch/user/s/sschrein/jtest");
		//LFN lfn = LFNUtils.getLFN("/alice/cern.ch/user/a/alidaq/AOD/AOD030/FILTERsim.jdl");
		
		LFN lfn = LFNUtils.getLFN("/alice/cern.ch/user/s/sschrein/jtest2");
		
		System.err.println(lfn);
		
		GUID guid = GUIDUtils.getGUID(lfn.guid);
		
		System.err.println(guid);
		
		Set<PFN> pfns = guid.getPFNs();
		
		final List<PFN> sortedPFNs = SEUtils.sortBySite(pfns, "CERN", false);
		
		final AliEnPrincipal admin = UserFactory.getByUsername("monalisa");
		
		for (PFN pfn: sortedPFNs){
			System.err.println(pfn);
			
			String reason = AuthorizationFactory.fillAccess(admin, pfn, AccessType.READ);
			
			if (reason!=null){
				System.err.println("Access refused because: "+reason);
				continue;
			}
			
			List<Protocol> protocols = Transfer.getProtocols(pfn);
			
			for (Protocol p: protocols){
				System.err.println("Trying "+p.getClass().getName()+" on "+pfn.pfn);
				
				try {
					File f = p.get(pfn, null);
					
					if (f!=null){
						System.err.println("Success : "+f+" / "+f.length());
						return;
					}
				}
				catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	private static void testTransfers(){
		Transfer t = TransferBroker.getInstance().getWork();
		System.err.println("Start: "+t);
		
		if (t!=null){
			t.run();
			System.err.println("End: "+t);
			TransferBroker.getInstance().notifyTransferComplete(t);
		}		
	}
	
	private static void testMonitoring(){	
		final Monitor m = MonitorFactory.getMonitor(Testing.class.getCanonicalName());
		
		for (int i=0; i<100; i++){
			testWhereisAndRankings();
			testFileOperations();
			
			m.addMeasurement("timing", i);
			m.incrementCounter("counter");
			
			if (i%3==0)
				m.incrementCacheHits("cache");
			else
				m.incrementCacheMisses("cache");
			
			try {
				Thread.sleep(i*500);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private static void testWhereisAndRankings(){
		LFN lfn = LFNUtils.getLFN("/alice/data/2010/LHC10e/000130848/ESDs/pass1/AOD019/0001/AliAOD.Dielectron.root");
		
		AliEnPrincipal user = UserFactory.getByUsername("grigoras");
		
		if (lfn==null){
			System.err.println("LFN is null, cannot continue testing");
			return;
		}
		
		System.err.println(user+" can read "+lfn.getName()+" : "+AuthorizationChecker.canRead(lfn, user));
		System.err.println(user+" can write "+lfn.getName()+" : "+AuthorizationChecker.canWrite(lfn, user));
		System.err.println(user+" can execute "+lfn.getName()+" : "+AuthorizationChecker.canExecute(lfn, user));

		user = UserFactory.getByUsername("agrigora");
		
		System.err.println(user+" can read "+lfn.getName()+" : "+AuthorizationChecker.canRead(lfn, user));
		System.err.println(user+" can write "+lfn.getName()+" : "+AuthorizationChecker.canWrite(lfn, user));
		System.err.println(user+" can execute "+lfn.getName()+" : "+AuthorizationChecker.canExecute(lfn, user));
		
		System.err.println(lfn);
		System.err.println("");
		Set<PFN> whereis = lfn.whereis();
		
		System.err.println(" ------- Whereis --------\n"+whereis+"\n");
		
		Set<PFN> whereisReal = lfn.whereisReal();
		
		System.err.println(" ------- Whereis  Real --------\n"+whereisReal+"\n");
		
		System.err.println(SEUtils.sortBySite(whereisReal, "CCIN2P3", false));
	}
	
	private static void testFileOperations(){
		UUID startingGUID = UUID.fromString("1d4a5d3a-038f-11e0-b2b3-001e0bd3f44c");
		
		System.err.println("UUID Variant : "+startingGUID.variant());
		System.err.println("UUID Version : "+startingGUID.version());
		
		System.err.println("UUID Variant : "+startingGUID.timestamp());
		//System.err.println("UUID Version : "+startingGUID.version());
		
		GUID guid = GUIDUtils.getGUID(startingGUID);
				
		if (guid==null){
			System.err.println("GUID is null, cannot continue");
			return;
		}
		
		System.err.println(guid);
		System.err.println("CHash : "+guid.getCHash());
		System.err.println("Hash  : "+guid.getHash());
		
		Set<PFN> pfns = guid.getPFNs();
		
		for (PFN pfn: pfns){
			System.err.println("---- PFN ------");
			System.err.println(pfn);
		}
		
		Set<LFN> lfns = guid.getLFNs();
		
		for (LFN lfn: lfns){
			System.err.println("---- LFN ------");
			System.err.println(lfn);
			System.err.println(lfn.getCanonicalName());
		}
		
		PFN pfn = new PFN(guid, SEUtils.getSE("ALICE::CERN::SE"));
		
		System.err.println("----- NEW PFN ------");
		System.err.println(pfn);
		
		LFN lfn = LFNUtils.getLFN("/alice/sim/LHC10a18/140014/128/QA.root");
		
		LFN parent = lfn;
		
		while ( (parent = parent.getParentDir())!=null ){
			System.err.println("--- Parent ----");
			System.err.println(parent);
			System.err.println("Canonical name : "+parent.getCanonicalName());
		}
	}
	
}
