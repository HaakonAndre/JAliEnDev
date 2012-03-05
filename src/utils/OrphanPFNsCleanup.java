package utils;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import lazyj.DBFunctions;
import lazyj.Format;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessTicket;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.XrootDEnvelope;
import alien.config.ConfigUtils;
import alien.io.protocols.Factory;
import alien.io.protocols.Xrootd;
import alien.io.xrootd.envelopes.XrootDEnvelopeSigner;
import alien.se.SE;
import alien.se.SEUtils;

/**
 * Go over the orphan_pfns and try to physically remove the entries
 * 
 * @author costing
 *
 */
public class OrphanPFNsCleanup {

	/**
	 * Thread pool per SE
	 */
	static final Map<Integer, ThreadPoolExecutor> EXECUTORS = new HashMap<Integer, ThreadPoolExecutor>(); 

	private static Map<Integer, SEThread> SE_THREADS = new HashMap<Integer, SEThread>();
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		final DBFunctions db = ConfigUtils.getDB("alice_users");

		final Xrootd x = Factory.xrootd;
		x.setUseXrdRm(true);

		long lastCheck = 0;
		
		while (true){
			if (System.currentTimeMillis() - lastCheck > 1000*60*60*6){
				db.query("SELECT distinct se FROM orphan_pfns;");
		
				while (db.moveNext()){
					final Integer se = Integer.valueOf(db.geti(1));
					
					SE theSE = SEUtils.getSE(se);
					
					if (theSE==null){
						System.err.println("No such SE: "+se);
						continue;
					}
					
					if (!SE_THREADS.containsKey(se)){
						final SEThread t = new SEThread(se.intValue());
						
						t.start();
						
						SE_THREADS.put(se, t);
					}
				}
				
				lastCheck = System.currentTimeMillis();
			}
				
			try{
				Thread.sleep(1000*5);
			}
			catch (InterruptedException ie){
				// ignore
			}
			
			System.err.println("Removed: "+removed+" ("+Format.size(reclaimedSpace.longValue())+"), failed to remove: "+failed);
		}
	}
	
	private static final class SEThread extends Thread {
		private final int seNumber;
		
		public SEThread(final int seNumber) {
			this.seNumber = seNumber;
		}
		
		@Override
		public void run() {
			final DBFunctions db = ConfigUtils.getDB("alice_users");
			
			ThreadPoolExecutor executor = EXECUTORS.get(Integer.valueOf(seNumber));
			
			if (executor==null){
				executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(8);
				
				EXECUTORS.put(Integer.valueOf(seNumber), executor);				
			}
			
			while (true){
				concurrentQueryies.acquireUninterruptibly();
				
				try{
					// TODO : what to do with these PFNs ? Iterate over them and release them from the catalogue nevertheless ?
//					db.query("DELETE FROM orphan_pfns WHERE se="+seNumber+" AND fail_count>10;");
					
					db.query("SELECT binary2string(guid) FROM orphan_pfns WHERE se="+seNumber+" AND fail_count<10 ORDER BY fail_count ASC LIMIT 10000;");
				}
				finally{
					concurrentQueryies.release();
				}
				
				if (!db.moveNext()){
					// there are no tasks for this SE now, check again sometime later
					
					try{
						Thread.sleep(1000*60*30);
					}
					catch (InterruptedException ie){
						// ignore
					}
					
					continue;
				}
				
				do {
					executor.submit(new CleanupTask(db.gets(1), seNumber));
				}
				while (db.moveNext());
				
				while (executor.getQueue().size()>0 || executor.getActiveCount()>0){
					try{
						Thread.sleep(1000);
					}
					catch (InterruptedException ie){
						// ignore
					}
				}
			}
		}
	}
	
	/**
	 * Number of files kept
	 */
	static final AtomicInteger kept = new AtomicInteger();
	
	/**
	 * Number of failed attempts
	 */
	static final AtomicInteger failed = new AtomicInteger();
	
	/**
	 * Number of successfully removed files 
	 */
	static final AtomicInteger removed = new AtomicInteger();
	
	/**
	 * Amount of space reclaimed
	 */
	static final AtomicLong reclaimedSpace = new AtomicLong();
	
	/**
	 * Fail one file
	 */
	static final void failOne(){
		failed.incrementAndGet();		
	}
	
	/**
	 * Successful deletion of one file
	 * 
	 * @param size
	 */
	static final void successOne(final long size){
		removed.incrementAndGet();
		
		final DBFunctions db = ConfigUtils.getDB("alice_users");
		
		db.query("UPDATE orphan_pfns_status SET status_value=status_value+1 WHERE status_key='reclaimedc';");
		
		if (size>0){
			db.query("UPDATE orphan_pfns_status SET status_value=status_value+"+size+" WHERE status_key='reclaimedb';");
			reclaimedSpace.addAndGet(size);
		}
	}
	
	/**
	 * Lock for a fixed number of DB queries in parallel 
	 */
	static final Semaphore concurrentQueryies = new Semaphore(16);
	
	private static class CleanupTask implements Runnable{
		final String sGUID;
		final int seNumber;
		
		public CleanupTask(final String sGUID, final int se) {
			this.sGUID = sGUID;
			this.seNumber = se; 
		}
		
		@Override
		public void run() {
			final UUID uuid = UUID.fromString(sGUID);
			
			final GUID guid;
			
			concurrentQueryies.acquireUninterruptibly();
			
			try{
				guid = GUIDUtils.getGUID(uuid, true);
			}
			finally{
				concurrentQueryies.release();
			}
			
			final SE se = SEUtils.getSE(seNumber);
			
			if (se==null){
				System.err.println("Cannot find any se with seNumber="+seNumber);
				kept.incrementAndGet();
				return;
			}
			
			if (!guid.exists()){
				guid.size = 123456;
				guid.md5 = "130254d9540d6903fa6f0ab41a132361";
			}
			
			final PFN pfn;
			
			try{
				pfn = new PFN(guid, se);
			}
			catch (Throwable t){
				System.err.println("Cannot generate the entry for "+seNumber+" ("+se.getName()+") and "+sGUID);
				t.printStackTrace();
				
				kept.incrementAndGet();
				return;
			}
			
			final XrootDEnvelope env =  new XrootDEnvelope(AccessType.DELETE, pfn);
			
			try {
				if (se.needsEncryptedEnvelope){
						XrootDEnvelopeSigner.encryptEnvelope(env);
				}
				else{
					// new xrootd implementations accept signed-only envelopes
					XrootDEnvelopeSigner.signEnvelope(env);	
				}
			}
			catch (final GeneralSecurityException e) {
				e.printStackTrace();
				return;
			}
			
			pfn.ticket = new AccessTicket(AccessType.DELETE, env);

			final DBFunctions db2 = ConfigUtils.getDB("alice_users");
			
			try {
				if (!Factory.xrootd.delete(pfn)){
					System.err.println("Could not delete from "+se.getName());
			
					concurrentQueryies.acquireUninterruptibly();
					try{
						db2.query("UPDATE orphan_pfns SET fail_count=fail_count+1 WHERE guid=string2binary('"+sGUID+"') AND se="+seNumber);
					
						failOne();
					}
					finally{
						concurrentQueryies.release();
					}
				}
				else{
					concurrentQueryies.acquireUninterruptibly();
				
					try{
						if (guid.exists()){
							//System.err.println("Successfuly deleted one file of "+Format.size(guid.size)+" from "+se.getName());
							successOne(guid.size);
							
							if (guid.removePFN(se)!=null){
								if (guid.getPFNs().size()==0){
									if (guid.delete()){
										//System.err.println("  Deleted the GUID since this was the last replica");
									}
									else{
										System.err.println("  Failed to delete the GUID even if this was the last replica:\n"+guid);
									}
								}
								else{
									//System.err.println("  Kept the GUID since it still has "+guid.getPFNs().size()+" replicas");
								}
							}
							else{
								System.err.println("  Failed to remove the PFN for this GUID");
							}
						}
						else{
							successOne(0);
							
							System.err.println("Successfuly deleted from "+se.getName()+" but GUID doesn't exist in the catalogue ...");
						}			
											
						db2.query("DELETE FROM orphan_pfns WHERE guid=string2binary('"+sGUID+"') AND se="+seNumber);
					}
					finally{
						concurrentQueryies.release();
					}
				}
			}
			catch (final IOException e) {
				//e.printStackTrace();
				
				failOne();
				
				System.err.println("Exception deleting from "+se.getName());
				
				concurrentQueryies.acquireUninterruptibly();
				
				try{
					db2.query("UPDATE orphan_pfns SET fail_count=fail_count+1 WHERE guid=string2binary('"+sGUID+"') AND se="+seNumber);				
				}
				finally{
					concurrentQueryies.release();
				}
			}
		}
	}
	
}
