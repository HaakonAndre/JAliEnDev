package alien.site.supercomputing.titan;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class TitanBatchController {
		private HashMap<String, TitanBatchInfo> batchesInfo = new HashMap<>();
		private String globalWorkdir;
		private List<TitanJobStatus> idleRanks;

		private static final int minTtl = 300;
		public static HashMap<String, Object> siteMap = new HashMap<>();

		public TitanBatchController(String global_work_dir){
			if(global_work_dir == null)
				throw new IllegalArgumentException("No global workdir specified");
			globalWorkdir = global_work_dir;
			idleRanks = new LinkedList<>();
		}

		public boolean updateDatabaseList(){
			//ls -d */ -1 | sed -e 's#/$##' | grep -E '^[0-9]+$' | sort -g	
			//ProcessBuilder pb = newProcessBuilder(System.getProperty("user.dir")+"/src/generate_list.sh",filename);
			System.out.println("Starting database update");
			ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", "for i in $(ls -d " 
						+ globalWorkdir + "/*/ | egrep \"/[0-9]+/\"); do basename $i; done");
			HashMap<String, TitanBatchInfo> tmpBatchesInfo = new HashMap<>();
			int dbcount = 0;
			try{
				Process p = pb.start();
				BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
				String line = null;
				while ( (line = reader.readLine()) != null) {
					//if(batchesInfo.get(line) ==  null)
						try{
							TitanBatchInfo bi = batchesInfo.get(line);
							if(bi==null)
								tmpBatchesInfo.put(line, 
										new TitanBatchInfo(Long.parseLong(line), 
											globalWorkdir + "/" + line));
							else
								tmpBatchesInfo.put(line, bi);
							dbcount++;
							System.out.println("Now controlling batch: " + line);
						}
						catch(InvalidParameterException e){
							System.err.println("Not a batch folder at " + 
									globalWorkdir + "/" + line + " , skipping....");
						}
						catch(Exception e){
							System.err.println(e.getMessage());
							System.err.println("Unable to initialize batch folder at " + 
									globalWorkdir + "/" + line + " , skipping....");
						}
				}
				batchesInfo = tmpBatchesInfo;
			}
			catch(IOException e){
				System.err.println("Error running batch info reader process: " + e.getMessage());
			}
			catch(Exception e){
				System.err.println("Exception at database list update: " + e.getMessage());
			}
			System.out.println(String.format("Now controlling %d batches", dbcount));

			return !batchesInfo.isEmpty();
		}

		public boolean queryDatabases(){
			idleRanks.clear();
			Long current_timestamp = System.currentTimeMillis() / 1000L;
			for(Object o : batchesInfo.values()){
				TitanBatchInfo bi = (TitanBatchInfo) o;
				System.out.println("Querying: " + bi.pbsJobId);
				if(!checkBatchTtlValid(bi, current_timestamp))
					continue;
				if(!bi.isRunning()){
					System.out.println("Batch " + bi.pbsJobId + " not running, cleaning up.");
					//bi.cleanup();
					//continue;
				}
				try{
					idleRanks.addAll(bi.getIdleRanks());
				}
				catch(Exception e){
					System.err.println("Exception caught in queryDatabases: " + e.getMessage());
					continue;
				}
			}
			return !idleRanks.isEmpty();
		}

		public List<TitanJobStatus> queryRunningDatabases(){
			List<TitanJobStatus> runningRanks = new LinkedList<>();
			Long current_timestamp = System.currentTimeMillis() / 1000L;
			for(Object o : batchesInfo.values()){
				TitanBatchInfo bi = (TitanBatchInfo) o;
				if(!checkBatchTtlValid(bi, current_timestamp))
					continue;
				try{
					runningRanks.addAll(bi.getRunningRanks());
				}
				catch(Exception e){
					continue;
				}
			}
			return runningRanks;
		}

		public final List<ProcInfoPair> getBatchesMonitoringData(){
			List<ProcInfoPair> l = new LinkedList<>();
			for(Object o : batchesInfo.values()){
				TitanBatchInfo bi = (TitanBatchInfo) o;
				l.addAll(bi.getMonitoringData());
			}
			return l;
		}

		private boolean checkBatchTtlValid(TitanBatchInfo bi, Long current_timestamp){
			// EXPERIMENTAL
			//return bi.getTtlLeft(current_timestamp) > minTtl;
			return bi.getTtlLeft(current_timestamp) > minTtl*8;
		}

		public void runDataExchange(){
			//List<TitanJobStatus> idleRanks = queryDatabases();
			//for(TitanJobStatus)
			int count = idleRanks.size();
			System.out.println(String.format("We can start %d jobs", count));
			
			if(count==0){
				return;
			}

			// create upload threads
			ArrayList<Thread> upload_threads = new ArrayList<>();
			for(TitanJobStatus js: idleRanks){
				if(js.status.equals("D")){
					JobUploader ju = new JobUploader(js);
					//ju.setDbName(dbname);
					upload_threads.add(ju);
					ju.start();
				}
			}
			
			// join all threads
			for(Thread t: upload_threads){
				try{
					t.join();
				}
				catch(InterruptedException e){
					System.err.println("Join for upload thread has been interrupted");
				}
			}

			System.out.println("Everything joined");
			System.out.println("================================================");

			//if(count>0) {
			//	monitor.sendParameter("ja_status", getJaStatusForML("REQUESTING_JOB"));
			//	monitor.sendParameter("TTL", siteMap.get("TTL"));
			//}

			upload_threads.clear();
			System.out.println("========= Starting download threads ==========");
			int cnt = idleRanks.size();
			Date d1 = new Date();
			JobDownloader.initialize();
			for(TitanJobStatus js: idleRanks){
				//System.out.println(siteMap.toString());
				//TitanJobStatus js = idleRanks.pop();

				JobDownloader jd = new JobDownloader(js, siteMap);
				//jd.setDbName(dbname);
				upload_threads.add(jd);
				jd.start();
				System.out.println("Starting downloader " + cnt--);
				if(cnt==0)
					break;
				//count--;
				//System.out.println("Wants to start Downloader thread");
				//System.out.println(js.batch.origTtl);
				//System.out.println(js.batch.numCores);
			}

			System.out.println(String.format("Count: %d", count));
			Date d3 = new Date();

			// join all threads
			for(Thread t: upload_threads){
				try{
					t.join();
					System.out.println("Joined downloader " + ++cnt);
				}
				catch(InterruptedException e){
					System.err.println("Join for upload thread has been interrupted");
				}
			}
			idleRanks.clear();
			Date d2 = new Date();
			System.out.println("Everything joined");
			System.out.println("Downloading took: " + (d2.getTime()-d1.getTime())/1000 + " seconds");
			System.out.println("Created downloaders during: " + 
					(d3.getTime()-d1.getTime())/1000 + " seconds");
			System.out.println("================================================");
		}
}
