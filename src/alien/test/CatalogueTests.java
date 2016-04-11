package alien.test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.LFN_JSON;

public class CatalogueTests  {
	
	// Thread pool
	static ThreadPoolExecutor tPool = null;
	
	// Entries processed
	static AtomicInteger global_count = new AtomicInteger();
	static Integer limit = 1000000;
	static AtomicLong ms_count = new AtomicLong(); 
	
	// File for tracking created folders
	static PrintWriter out 	= null;
	static PrintWriter failed_folders = null;
	static PrintWriter pw = null;
	static String logs_suffix = "";
		
	static boolean limit_reached = false;

	public static void main(String[] args) throws IOException {
		int nargs = args.length;
		
		if(nargs<1){
			System.err.println("Usage: ./run.sh alien/src/test/CatalogueTests <type> <alien_path> [<pool_size>] [<logs_suffix>] [<limit>]");
			System.err.println("E.g. <type> 0-FS 1-DB");
			System.err.println("E.g. <alien_path> -> /catalogue/jalien/alice/data/2016/");
			System.err.println("E.g. <pool_size> -> 8");
			System.err.println("E.g. <logs-suffix> -> alice-data-2016");
			System.err.println("E.g. <limit> -> 2000000");
			System.exit(-3);
		}
		
		Integer type = 0;
		if(nargs>1){
			type = Integer.valueOf(args[1]);		
		}
		
		Integer pool_size = 16;
		if(nargs>2){
			pool_size = Integer.valueOf(args[2]);		
		}
		System.out.println("Pool size: "+pool_size);
		tPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(pool_size);
		tPool.setKeepAliveTime(5, TimeUnit.SECONDS);
		tPool.allowCoreThreadTimeOut(true);		
		
		if(nargs>3){
			logs_suffix = "-"+args[3];		
		}
		
		if(nargs>4){
			limit = Integer.valueOf(args[4]);		
		}
		
		System.out.println("Printing output to: test_out"+logs_suffix+"-"+pool_size+"t");
		out = new PrintWriter(new FileOutputStream("test_out"+logs_suffix+"-"+pool_size+"t"));
		out.println("Starting: "+ new Date());
		out.flush();
		
		System.out.println("Printing folders to: test_folders"+logs_suffix+"-"+pool_size+"t");
		pw = new PrintWriter(new FileOutputStream("test_folders"+logs_suffix+"-"+pool_size+"t"));
		
		System.out.println("Printing failed folders to test_failed_folders"+logs_suffix+"-"+pool_size+"t");
		failed_folders = new PrintWriter(new FileOutputStream("test_failed_folders"+logs_suffix+"-"+pool_size+"t"));
		
		System.out.println("Going to loop over " + args[0] + " hierarchy. Time: "
				+ new Date());
		
		// Control-C catch
		Runtime.getRuntime().addShutdownHook(new Thread() {
		    public void run() {			    	
		        tPool.shutdown();
		    	
				try {
					while (!tPool.awaitTermination(5, TimeUnit.SECONDS)) {
						  System.out.println("Waiting for threads finishing..."+tPool.getActiveCount());
						}
				} catch (InterruptedException e) {
					System.err.println("Something went wrong in shutdown!: "+e);
				}
		      			      
		    }
		 });

		if (type==0){
			System.out.println("Running as LFN_JSON");
			tPool.submit(new RecurseLFNJSON(new LFN_JSON(args[0])));
		} else if (type ==1) {
			System.out.println("Running as DB LFN");
			tPool.submit(new RecurseLFN(LFNUtils.getLFN(args[0])));
		}
		
		try {
			while (!tPool.awaitTermination(10, TimeUnit.SECONDS)) {
				  int tCount = tPool.getActiveCount();
				  int qSize  = tPool.getQueue().size();
				  System.out.println("Awaiting completion of threads..."+tCount+ " - "+ qSize);
				  if(tCount==0 && qSize==0){
					tPool.shutdown();
					System.out.println("Shutdown executor");
				  }			  
				}
		} catch (InterruptedException e) {
			System.err.println("Something went wrong!: "+e);
		}
		
		double ms_per_ls = 0;
		
		if (global_count.get() > 0 ){
			if (limit_reached){
				System.out.println("Limit reached!");
				ms_per_ls = ms_count.get()/limit;
			}
			else{
				System.out.println("Limit not reached!");
				ms_per_ls = ms_count.get()/global_count.get();
			}
		}
		else
			System.out.println("!!!!! Zero folders !!!!!");
		
		System.out.println("Final count: " + ( limit_reached ? limit.toString() : global_count.toString() ));
		System.out.println("Final ms/list: " + ms_per_ls);
		
		out.println("Final count: " + ( limit_reached ? limit.toString() : global_count.toString() ) + " - " + new Date());
		out.println("Final ms/list: " + ms_per_ls);
		out.close();
		pw.close();
		failed_folders.close();
	}

	
	private static class RecurseLFN implements Runnable {
		final LFN dir;
				
		public RecurseLFN(LFN folder){
			this.dir=folder;
		}
		
		@Override
		public String toString(){
			return this.dir.getCanonicalName();
		}
		
		public void run() {
			if(dir == null){
				String msg="LFN DIR is null! "+dir.getCanonicalName();
				failed_folders.println(msg);
				failed_folders.flush();
				return;
			}		
			
			int counter = global_count.incrementAndGet();
			if (counter >= limit){
				limit_reached = true;
				return;
			}
			
			if(counter%2000==0){
				out.println("LFN: " + dir.getCanonicalName() + " - Count: " + counter
						+ " Time: " + new Date());
				out.flush();
			}
						
			pw.println(dir.getCanonicalName());
			pw.flush();
			
//			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
			long start = System.nanoTime();
			List<LFN> list = dir.list();
			long duration_ms = (long) ((System.nanoTime() - start) / 1000000d);
			
			ms_count.addAndGet(duration_ms);

			if(list.isEmpty())
				return;
							
			for (final LFN l : list) {
				if (l.isDirectory()) {
					try {
						if(!limit_reached)
							tPool.submit(new RecurseLFN(l));
					}
					catch (RejectedExecutionException ree){
						String msg = "Interrupted directory: "
								+ l.getCanonicalName() + 
								" Parent: " +dir.getCanonicalName() + " Time: " + new Date();
						System.err.println(msg);
						failed_folders.println(msg);
						failed_folders.flush();
						return;
					}
				}
//				else if (counter%20000==0){
//				out.println("LFN listed: " + l.getCanonicalName() + " - Type: "+ l.type + " - Size: "+l.size);
//				out.flush();
//			}
			}
		}
	}
	

	private static class RecurseLFNJSON implements Runnable {
		final LFN_JSON dir;
				
		public RecurseLFNJSON(LFN_JSON folder){
			this.dir=folder;
		}
		
		@Override
		public String toString(){
			return this.dir.getCanonicalName();
		}
		
		public void run() {
			if(dir == null){
				String msg="LFN DIR is null! "+dir.getCanonicalName();
				failed_folders.println(msg);
				failed_folders.flush();
				return;
			}	
			
			int counter = global_count.incrementAndGet();
			if (counter >= limit){
				limit_reached = true;
				return;
			}
			
			if(counter%2000==0){
				out.println("LFN: " + dir.getCanonicalName() + " - Count: " + counter
						+ " Time: " + new Date());
				out.flush();
			}
			
			pw.println(dir.getCanonicalName());
			pw.flush();
			
//			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
			long start = System.nanoTime();
			List<LFN_JSON> list = dir.list();
			long duration_ms = (long) ((System.nanoTime() - start) / 1000000d);
			
			ms_count.addAndGet(duration_ms);
			
			if(list.isEmpty())
				return;
				
			for (final LFN_JSON l : list) {
				if (l.isDirectory()) {
					try {
						if(!limit_reached)
							tPool.submit(new RecurseLFNJSON(l));
					}
					catch (RejectedExecutionException ree){
						String msg = "Interrupted directory: "
								+ l.getCanonicalName() + 
								" Parent: " +dir.getCanonicalName() + " Time: " + new Date();
						System.err.println(msg);
						failed_folders.println(msg);
						failed_folders.flush();
						return;
					}
				} 
//				else if (counter%20000==0){
//					out.println("LFN listed: " + l.getCanonicalName() + " - Type: "+ l.type + " - Size: "+l.size);
//					out.flush();
//				}
			}
		}
	}
	
	
}



