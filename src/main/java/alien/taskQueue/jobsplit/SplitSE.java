package alien.taskQueue.jobsplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.se.SEUtils;
import alien.taskQueue.JDL;

public class SplitSE extends JobSplitter{
	
	
public Map<String,List<String>> groupInputFiles (JDL j) throws IOException{
		Map<String,List<String>> groupedFiles = new HashMap<String,List<String>>();
		Collection<LFN> inputFiles = getInputFiles(j);
		
			for (LFN file : inputFiles) {
				String fileS = file.getCanonicalName();
				Set<PFN> pfns = c_api.getPFNs(fileS);
				if (pfns != null && pfns.size() > 0) {
					List<String> seNames = new ArrayList<String>();
					for (final PFN pfn : pfns) {
						seNames.add(pfn.getSE().getName());
					}
					Collections.sort(seNames);
					String allSe = String.join(",", seNames);
					if (!groupedFiles.containsKey(allSe)) {
						groupedFiles.put(allSe, new ArrayList<String>());
					}
					groupedFiles.get(allSe).add(fileS);
					
				}
			}
			return groupedFiles;
		}
	
public List<JDL> splitJobs (final JDL j, long masterId) throws IOException {
	
	
		Map<String,List<String>> groupInputFiles = groupInputFiles(j);
	
		Map<String,List<String>> inputFilesLow = new HashMap<String,List<String>>();
		//int MAX_SIZE = Integer.parseInt(j.gets("SplitMaxInputFileSize"));
		int MAX_INPUT= Integer.parseInt(j.gets("SplitMaxInputFileNumber"));
		int LIMIT = MAX_INPUT / 5;
		
		for (Entry<String, List<String>> entry : groupInputFiles.entrySet()) {
			List<JDL> subjobs = new ArrayList<JDL>();
			int number = 0;

			List<String> inputdata = entry.getValue();
			if (inputdata.size() < MAX_INPUT / 5) {
				inputFilesLow.put(entry.getKey(), inputdata);
				groupInputFiles.remove(entry.getKey());
			}

			if (inputdata.size() > MAX_INPUT) {
				while (!inputdata.isEmpty()) {
					number = Math.min(MAX_INPUT, inputdata.size());
					if (inputdata.size() < MAX_INPUT * 2 && MAX_INPUT < inputdata.size()) {
						number = inputdata.size() / 2;
					}

					JDL tempJ = new JDL();
					tempJ.set("InputData", inputdata.subList(0, number));
					subjobs.add(j);
					inputdata.subList(0, number).clear();
				}
			}



		}
			
		
		String minBasket = "";
		int min = 0;
		while (min < LIMIT) {
			min = 0;
			for (Entry<String, List<String>> entry : inputFilesLow.entrySet()) {
				if (min > entry.getValue().size() || min == 0) {
					min = entry.getValue().size();
					minBasket = entry.getKey();
				}
			} 
			
			String[] tmpArr = getCloseMaxShared(minBasket, inputFilesLow,LIMIT).split(";", 2);
			
			String cBasket = tmpArr[0];
			System.out.println("cBasket: " + cBasket);
			
			String shared = tmpArr[1];
			System.out.println("shared: " + shared);
			
			if (shared.equals("")) {
			String newBasket = String.join(",", minBasket, cBasket);
			List<String> newInput = inputFilesLow.get(minBasket);
			newInput.addAll(inputFilesLow.get(cBasket));
			inputFilesLow.put(newBasket, newInput);
			inputFilesLow.remove(minBasket);
			inputFilesLow.remove(cBasket);
			}
			else {
				List<String> newInput = inputFilesLow.get(minBasket);
				newInput.addAll(inputFilesLow.get(cBasket));
				inputFilesLow.put(shared, newInput);
				inputFilesLow.remove(minBasket);
				inputFilesLow.remove(cBasket);
			}
			groupInputFiles.putAll(inputFilesLow);
		}
		return null;
	}
	
	public static String getCloseMaxShared(String minBasket, Map<String, List<String>> seListLow, int setMax) {

		String closest = "";
		int maxBasket = 0;
		int minSize = 0;
		String minBasketTmp  = "";
		boolean foundMatch = false;
		double minDist = 0.0;
		String shareSe = "";
		
		boolean foundShared = false;
		String[] tmp = minBasket.split(",");
		List<String> tmpList = new ArrayList<String>();
		for (Entry<String, List<String>> entry : seListLow.entrySet()) {
			if (!(entry.getKey().equals(minBasket))) {
				boolean shared = false;
				for (String s : tmp) {
					if (entry.getKey().contains(s)) {
						shared = true;
						shareSe = s;
					}
				}
				if (shared) {
					foundShared = true;
					String[] tmp2 = entry.getKey().split(",");
					if ((maxBasket == 0 || maxBasket < entry.getValue().size()) && (setMax == 0 || setMax > entry.getValue().size())) {
						foundMatch = true;
						maxBasket = entry.getValue().size();
						closest = entry.getKey();
					}
					else {
						if (minSize == 0 || minSize > entry.getValue().size()) {
						minSize = entry.getValue().size();
						minBasketTmp = entry.getKey();
						}
					}
				}
			}
		}
		
		if (!foundMatch) {
			closest = minBasketTmp;
		}

		if (!foundShared) {
			for (Entry<String, List<String>> entry : seListLow.entrySet()) {
				double tmpDist = 0.0;
				String[] tmp2 = entry.getKey().split(",");
				for (String s : tmp) {
					for (String s2 : tmp2) {
						tmpDist += SEUtils.getDistance(s, s2, false);
					}
				}
				if (minDist > tmpDist || minDist == 0.0) {
					minDist = tmpDist;
					closest = entry.getKey();
				}
			}
		}

		return closest + ";" + shareSe;
	}

}
