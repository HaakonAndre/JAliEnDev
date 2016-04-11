package alien.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import lia.util.Utils;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;

public class CatalogueToJsonThreads  {
	
	// Array of thread-dir
	static final HashMap<Long, LFN> activeThreadFolders = new HashMap<>();
	
	// Thread pool
	static ThreadPoolExecutor tPool = null;
	
	// Entries processed
	static final int origlimit = 5000000;
	static  AtomicInteger global_count = new AtomicInteger();
	static  AtomicInteger limit = new AtomicInteger(origlimit);
	
	// File for tracking created folders
	static PrintWriter out 					= null;
	static PrintWriter pw 					= null;
	static PrintWriter failed_folders 		= null;
	static PrintWriter failed_files 		= null;
	static PrintWriter failed_collections 	= null;	
	static PrintWriter failed_ses		 	= null;
	static PrintWriter used_threads		 	= null;
	static String logs_suffix = "";
	
	static String suffix = "/catalogue";
	
	// Method to check readability of JSON created files
	public static void checkJsonReadable(String[] args) throws IOException {

		JSONParser parser = new JSONParser();

		try {
			if(args[0]==null){
				System.err.println("You need to pass an AliEn path to a file as first argument.");
				System.exit(-3);
			}
			
			Object obj = parser
					.parse(new FileReader(args[0]));

			JSONObject jsonObject = (JSONObject) obj;

			String guid = (String) jsonObject.get("guid");
			System.out.println(guid);

			String ctime = (String) jsonObject.get("ctime");
			System.out.println(ctime);

			// loop array
//			JSONArray mem = (JSONArray) jsonObject.get("zip_members");
//
//			for (int i = 0; i < mem.size(); i++) {
//				JSONObject zipmem = (JSONObject) mem.get(i);
//
//				String lfn = (String) zipmem.get("lfn");
//				System.out.println(lfn);
//
//				String md5 = (String) zipmem.get("md5");
//				System.out.println(md5);
//
//				String size = (String) zipmem.get("size");
//				System.out.println(size);
//			}
			
//			
//			JSONArray colmembers = (JSONArray) jsonObject.get("lfns");
//			if (colmembers != null && colmembers.size()>0) {
//				for (int i = 0; i < colmembers.size(); i++) {
//					String lfn = (String) colmembers.get(i);
//					System.out.println(lfn);
//				}
//			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		int nargs = args.length;
		Scanner reader = new Scanner(System.in);
		
		if(nargs<1){
			System.err.println("Usage: ./run.sh alien/src/test/CatalogueToJsonThreads <alien_path> [<pool_size>] [<logs_suffix>]");
			System.err.println("E.g. <alien_path> -> /alice/sim/2016/");
			System.err.println("E.g. <pool_size> -> 8");
			System.err.println("E.g. <logs-suffix> -> alice-sim-2016");
			System.exit(-3);
		}
		
		Integer pool_size = 16;
		if(nargs>1){
			pool_size = Integer.valueOf(args[1]);		
		}
		System.out.println("Pool size: "+pool_size);
		tPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(pool_size);
		tPool.setKeepAliveTime(1, TimeUnit.MINUTES);
		tPool.allowCoreThreadTimeOut(true);
				
		
		if(nargs>2){
			logs_suffix = "-"+args[2];		
		}
		
		System.out.println("Printing output to: out"+logs_suffix);
		out = new PrintWriter(new FileOutputStream("out"+logs_suffix));
		out.println("Starting: "+ new Date());
		out.flush();
		
		System.out.println("Printing folders to: folders"+logs_suffix);
		pw = new PrintWriter(new FileOutputStream("folders"+logs_suffix));
		
		System.out.println("Printing failed folders to failed_folders"+logs_suffix);
		failed_folders = new PrintWriter(new FileOutputStream("failed_folders"+logs_suffix));
		
		System.out.println("Printing failed collections to failed_collections"+logs_suffix);
		failed_collections = new PrintWriter(new FileOutputStream("failed_collections"+logs_suffix));
		
		System.out.println("Printing failed files to failed_files"+logs_suffix);
		failed_files = new PrintWriter(new FileOutputStream("failed_files"+logs_suffix));
		
		System.out.println("Printing failed ses to failed_ses"+logs_suffix);
		failed_ses = new PrintWriter(new FileOutputStream("failed_ses"+logs_suffix));
		
		System.out.println("Printing threads to used_threads"+logs_suffix);
		used_threads = new PrintWriter(new FileOutputStream("used_threads"+logs_suffix));
		used_threads.println(logs_suffix + " - " + pool_size);
		used_threads.close();
		
		System.out.println("Going to create " + args[0] + " hierarchy. Time: "
				+ new Date());
		System.out.println(Utils.getOutput("df -h ."));
		System.out.println(Utils.getOutput("df -hi ."));
		
		// create directories
		File f = new File(suffix + args[0]);
		if(f.exists()){
			System.out.println("!!!!! Base folder already exists. Continue? (default yes) !!!!!");
			String cont = reader.next();
			if(cont.equals("no")){
				System.err.println("User stopping on base directory");
				System.exit(-1);
			}
		}
		else{
			if (!f.mkdirs()) {
				System.err.println("Error creating base directory");
				System.exit(-1);
			}
		}
		
		// Control-C catch
		Runtime.getRuntime().addShutdownHook(new Thread() {
		    public void run() {			    	
		      List<Runnable> tQueue = tPool.shutdownNow();
		    	
				try {
					while (!tPool.awaitTermination(5, TimeUnit.SECONDS)) {
						  System.out.println("Waiting for threads finishing..."+tPool.getActiveCount());
						}
				} catch (InterruptedException e) {
					System.err.println("Something went wrong in shutdown!: "+e);
				}
		      			      
			    try {
					PrintWriter pendingTasks = new PrintWriter(new FileOutputStream("pendingTasks"+logs_suffix));
					
					pendingTasks.println("Dumping tQueue\n");
					
					for (Runnable r: tQueue){
						  Object realTask = JobDiscoverer.findRealTask(r);
				    	  pendingTasks.println(realTask.toString());
				    }

					pendingTasks.println("Dumping activeThreadFolders\n");
					
					for (LFN l : activeThreadFolders.values()){
				    	  pendingTasks.println(l.lfn);
				    }
					
					pendingTasks.close();
			    } catch (Exception e) {
					System.err.println("Something went wrong dumping tasks!: "+e.toString()+" - "+tQueue.toString());
			    }
		    }
		 });

		tPool.submit(new Recurse(LFNUtils.getLFN(args[0])));
		
		try {
			while (!tPool.awaitTermination(20, TimeUnit.SECONDS)) {
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

		System.out.println("Final count: " + global_count.toString());
		
		out.println("Final count: " + global_count.toString() + " - " + new Date());
		out.close();
		pw.close();
		failed_folders.close();
		failed_collections.close();
		failed_files.close();
		failed_ses.close();

		// Utils.getOutput("tar -cvzf ~/alien_folder.tar.gz folders alice");
		// Utils.getOutput("rm -rf folders alice");
	}

	
	private static class Recurse implements Runnable {
		final LFN dir;
				
		public Recurse(LFN folder){
			this.dir=folder;
		}
		
		@Override
		public String toString(){
			return this.dir.getCanonicalName();
		}
		
		static final Comparator<LFN> comparator = new Comparator<LFN>() {
			@Override
			public int compare(LFN o1, LFN o2) {
				if (o1.lfn.contains("archive") || o1.lfn.contains(".zip"))
					return -1;

				if (o2.lfn.contains("archive") || o2.lfn.contains(".zip"))
					return 1;

				return 0;
			}
		};

		public static List<LFN> getZipMembers(Map<GUID,LFN> whereis, LFN lfn){
			List<LFN> members = new ArrayList<>();
			
			final String lfn_guid = lfn.guid.toString();
			final String lfn_guid_start = "guid:";
			
			for(GUID g: whereis.keySet()){
				Set<PFN> pfns = g.getPFNs();
				for (PFN pf: pfns){
					if( pf.pfn.startsWith(lfn_guid_start) && pf.pfn.contains(lfn_guid) ){
						members.add(whereis.get(g));
					}
				}
			}
			
			return members;
		}
		
		@SuppressWarnings("unchecked")
		public void run() {
			if(dir == null){
				String msg="LFN DIR is null! "+dir.getCanonicalName();
				failed_folders.println(msg);
				failed_folders.flush();
				return;
			}		
			
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
			int counted = global_count.get();
			if (global_count.get() >= limit.get()) {
				out.println("LFN: " + dir.getCanonicalName() + " - Count: " + counted
						+ " Time: " + new Date());
				out.flush();
				limit.set(counted + origlimit);
			}
	
			// order the list
			List<LFN> list = dir.list();
	
			if(list.isEmpty())
				return;

			// If folder not null or empty, we register threadId to dir
			long threadId = Thread.currentThread().getId();
			activeThreadFolders.put(threadId, dir);
//			System.out.println("Thread "+threadId+" doing "+dir.lfn);
			
			// Sort the list by archive
			Collections.sort(list, comparator);
	
			// Files that will excluded since they are included in archives
			Set<LFN> members_of_archives = new HashSet<>();
			
			// whereis of each file
			Map<GUID,LFN> whereis = new HashMap<>();		
			for (final LFN fi : list) {
				if (fi.isFile()) {
					GUID g = GUIDUtils.getGUID(fi);
					if(g == null){
						failed_files.println("LFN is orphan!: "+fi.getCanonicalName());
						failed_files.flush();
						members_of_archives.add(fi); // Ignore files without guid
						continue;
					}
									
					if (g.getPFNs().size()==0){
						failed_files.println("LFN without pfns!: "+fi.getCanonicalName());
						failed_files.flush();
						members_of_archives.add(fi); // Ignore files without pfns
						continue;
					}
						
					whereis.put(g,fi);
				}
			}
				
			for (final LFN l : list) {
				global_count.incrementAndGet();
	
//				if(Thread.currentThread().isInterrupted()){
//					String msg = "Thead "+threadId+" interrupted on: "+dir.lfn;
//					System.out.println(msg);
//					failed_folders.println(msg);
//					failed_folders.flush();
//				}
				
				if (l.isDirectory()) {
					// create the dir
					File f = new File(suffix + l.getCanonicalName());
					if (!f.mkdirs()) {
						String msg = "Error creating directory: "
								+ l.getCanonicalName() + 
								" Time: " + new Date();
						System.err.println(msg);
						failed_folders.println(msg);
						failed_folders.flush();
						continue;
					}
					
					pw.println(l.getCanonicalName() + "\t" + l.getOwner() + ":"
							+ l.getGroup() + "\t" + l.getPermissions() + "\t"
							+ df.format(l.ctime));
					pw.flush();
					
					try {
						tPool.submit(new Recurse(l));
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
				} else if (l.isCollection()) {
					JSONObject lfnfile = new JSONObject();
					lfnfile.put("size", String.valueOf(l.size));
					lfnfile.put("owner", l.getOwner());
					lfnfile.put("gowner", l.getGroup());
					lfnfile.put("perm", l.getPermissions());
					lfnfile.put("ctime", df.format(l.ctime));
					lfnfile.put("jobid", String.valueOf(l.jobid));
					lfnfile.put("guid", l.guid.toString());
	
					JSONArray filesjson = new JSONArray();
					for (final String s : l.listCollection()) {
						filesjson.add(s);
					}
					lfnfile.put("lfns", filesjson);
	
					try (FileWriter file = new FileWriter(suffix
							+ l.getCanonicalName())) {
						String myJsonString = lfnfile.toJSONString();
						myJsonString = myJsonString.replaceAll("\\\\", "");
						myJsonString = myJsonString.replaceAll(",", ",\n");
						file.write(myJsonString);
					} catch (IOException e) {
						String msg = "Can't create LFN collection: "
								+ l.getCanonicalName() 
								+ " Time: " + new Date();
						failed_collections.println(msg);
						failed_collections.flush();
					}
				} else if (l.isFile()) {
	
					if (members_of_archives.contains(l))
						continue;
	
					List<LFN> zip_members = getZipMembers(whereis, l);
					boolean isArchive = zip_members != null
							&& !zip_members.isEmpty();
	
					// create json file in the hierarchy
					JSONObject lfnfile = new JSONObject();
					lfnfile.put("size", String.valueOf(l.size));
					lfnfile.put("owner", l.getOwner());
					lfnfile.put("gowner", l.getGroup());
					lfnfile.put("perm", String.valueOf(l.getPermissions()));
					lfnfile.put("ctime", df.format(l.ctime));
					lfnfile.put("jobid", String.valueOf(l.jobid));
					lfnfile.put("guid", String.valueOf(l.guid));
					lfnfile.put("md5", l.md5);
	
					Set<PFN> pfns = null;
					// we have the pfns in the map
				    for( GUID guidmap : whereis.keySet() ){
				        if( whereis.get(guidmap).equals(l) ) {
				            pfns = guidmap.getPFNs();
				        }
				    }
	
					if (pfns != null) {
						JSONArray pfnsjson = new JSONArray();
						for (final PFN p : pfns) {
							JSONObject pfn = new JSONObject();
							String se = p.getSE().getName();
							if(se == null){
								failed_ses.println("SE null: "+p.seNumber+" - "+p.pfn);
								failed_ses.flush();
								continue;
							}
							pfn.put("se", se);
							pfn.put("pfn", p.pfn);
							pfnsjson.add(pfn);
						}
						lfnfile.put("pfns", pfnsjson);
					}
	
					if (isArchive) {
						JSONArray members = new JSONArray();
						// we have an archive, we create ln per member
						for (LFN lfn_in_zip : zip_members) {
							members_of_archives.add(lfn_in_zip);
							JSONObject entry = new JSONObject();
							entry.put("lfn", lfn_in_zip.getFileName());
							entry.put("size", String.valueOf(lfn_in_zip.size));
							entry.put("md5", lfn_in_zip.md5);
							members.add(entry);
						}
						lfnfile.put("zip_members", members);
					}
	
					try (FileWriter file = new FileWriter(suffix
							+ l.getCanonicalName())) {					
						String myJsonString = lfnfile.toJSONString();
						myJsonString = myJsonString.replaceAll("\\\\", "");
						myJsonString = myJsonString.replaceAll(",", ",\n");					
						file.write(myJsonString);
						
						if (isArchive) {
							for (LFN lfn_in_zip : zip_members) {
								Files.createSymbolicLink(
										Paths.get(suffix
												+ lfn_in_zip.getCanonicalName()),
										Paths.get(l.getFileName()));
							}
						}
					} catch (IOException e) {
						String msg = "Can't create LFN: "
								+ l.getCanonicalName()
								+ " Time: " + new Date() + " Exception: "+e;
						failed_files.println(msg);
						failed_files.flush();
					}
				}
			}
			// Remove from list
			activeThreadFolders.remove(threadId);
		}
	}
	
}



