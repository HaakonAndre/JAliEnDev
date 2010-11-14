package alien;

import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
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
	public static void main(String[] args) {
		//testMonitoring();
		
		//testFileOperations();
		
		for (int i=0; i<10; i++)
			System.err.println(GUIDUtils.generateTimeUUID());
		
//		Properties prop = System.getProperties();
//		
//		for (Object o: prop.keySet()){
//			System.err.println(o+ " : " + prop.get(o));
//		}
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
		UUID startingGUID = UUID.fromString("00270ff2-3bd3-11df-9bee-001cc45cb5dc");
		
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
		
		LFN lfn = LFNUtils.getLFN("/alice/sim/LHC10a18/140014/128/QA.root");
		
		LFN parent = lfn;
		
		while ( (parent = parent.getParentDir())!=null ){
			System.err.println("--- Parent ----");
			System.err.println(parent);
			System.err.println("Canonical name : "+parent.getCanonicalName());
		}
	}
	
}
