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
		//testWhereisAndRankings();
		//testFileOperations();
		testMonitoring();
	}
	
	private static void testMonitoring(){
		Monitor m = MonitorFactory.getMonitor(Testing.class.getCanonicalName());
		
		for (int i=0; i<100; i++){
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
		
		System.err.println(lfn);
		System.err.println("");
		Set<PFN> whereis = lfn.whereis();
		
		System.err.println(" ------- Whereis --------\n"+whereis+"\n");
		
		Set<PFN> whereisReal = lfn.whereisReal();
		
		System.err.println(" ------- Whereis  Real --------\n"+whereisReal+"\n");
		
		System.err.println(SEUtils.sortBySite(whereisReal, "CCIN2P3"));
	}
	
	private static void testFileOperations(){
		UUID startingGUID = UUID.fromString("00270ff2-3bd3-11df-9bee-001cc45cb5dc");
		
		GUID guid = GUIDUtils.getGUID(startingGUID);
				
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
