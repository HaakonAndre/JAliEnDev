package alien;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.logging.Logger;

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
import alien.io.protocols.Protocol;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.se.SEUtils;
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
		//testMonitoring();
		
		testGET();
		
//		for (int i=0; i<10; i++)
//			System.err.println(GUIDUtils.generateTimeUUID());
		
//		Properties prop = System.getProperties();
//		
//		for (Object o: prop.keySet()){
//			System.err.println(o+ " : " + prop.get(o));
//		}
	}
	
	private static void testGET(){
		LFN lfn = LFNUtils.getLFN("/alice/cern.ch/user/a/alidaq/LHC10h/rec.jdl");
		
		GUID guid = GUIDUtils.getGUID(lfn.guid);
		
		Set<PFN> pfns = guid.getPFNs();
		
		final AliEnPrincipal admin = UserFactory.getByUsername("monalisa");
		
		for (PFN pfn: pfns){
			System.err.println(pfn.pfn);
			
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
						System.err.println("Success : "+f);
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
