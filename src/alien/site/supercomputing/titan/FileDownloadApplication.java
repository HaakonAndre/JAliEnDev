package alien.site.supercomputing.titan;

import java.util.LinkedList;
import java.util.List;

import alien.catalogue.LFN;

public class FileDownloadApplication{
	List<LFN> fileList;
	List<Pair<LFN, String>> dlResult;
	
	FileDownloadApplication(List<LFN> inputFiles){
		fileList = inputFiles;
		dlResult = new LinkedList();
	}

	synchronized void putResult(LFN l, String s){
		//System.out.println("Really putting: " + s);
		Pair<LFN, String> p = new Pair<>(l,s);
		//System.out.println("Really put: " + p.getSecond());
		dlResult.add(p);
	}

	public boolean isCompleted(){
		return fileList.size()== dlResult.size();
	}

	public final List<Pair<LFN, String>> getResults(){
		return dlResult;
	}

	synchronized public void print(){
		//System.out.println("=================: " + this);
		//System.out.println("Ordered: ");
		for(LFN l: fileList){
			System.out.println(l.getCanonicalName());
		}
		//System.out.println("Downloaded: ");
		for(Pair<LFN,String> p: dlResult){
			System.out.println(p.getFirst().getCanonicalName() + ": " + p.getSecond());
		}
		//System.out.println("=================: ");
	}
}
