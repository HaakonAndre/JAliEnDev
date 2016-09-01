package alien.site.supercomputing.titan;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.io.IOUtils;

public class FileDownloadController extends Thread{
	private BlockingQueue<Pair<LFN, Thread>> lfnQueue;
	private HashMap<LFN, LinkedList<Thread>> lfnQueueInProcess;
	private String cacheFolder;
	private final int maxQueueCapacity = 1000;
	private final int maxDownloaderSleep = 10000;


	public FileDownloadController(String cacheFolder) throws IOException, FileNotFoundException{
		if(cacheFolder==null || cacheFolder.equals(""))
			throw new IOException("Cache folder name can not be null");

		lfnQueue = new ArrayBlockingQueue<Pair<LFN, Thread>>(maxQueueCapacity);
		lfnQueueInProcess = new HashMap<>();
	}

	synchronized public void applyForDownload(List<LFN> inputFiles, Thread t){
		//for()
		// here the files are added to the list
		// IOUtils are called

		// if LFN present -> add t to the list
		// else create a new item
	}

	public void run(){
		while(true){
			// if nothing in the lfnQueue -> sleep, continue
			boolean emptyQueue;
			synchronized(this){
				emptyQueue = lfnQueue.isEmpty();
			}
			if(emptyQueue){
				try{
					Thread.sleep(maxDownloaderSleep);
				}
				catch(InterruptedException e){}
				continue;
			}
			//for(LFN l: lfnQueue){
			//}

			// else foreach:
			// if file in the cache -> notify all waiting threads with existing path, continue, 
			//  ...... check size, md5
			// else run IOUtils.get, notify about the result
			;
			// what if download fails?
			// report the result?
		}
	}

	private void notifyDownloadFinished(LFN l){
		// for each in LinkedList -> push notify
		for(Thread t: lfnQueueInProcess.get(l)){
			synchronized(t){
				t.notify();
			}
		}
	}

	private boolean fileIsInCache(LFN l){
		return false;
	}

	public String getCachedFile(LFN l){
		return cacheFolder + l.getName();
	}


	private boolean checkMd5(){
		return true;
	}

	private boolean checkSize(){
		return true;
	}
}
