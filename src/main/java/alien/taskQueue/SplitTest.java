package alien.taskQueue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SplitTest {
	final static int MAX_SIZE = 100;
	static Map<String, Map<String, String>> seDistance = new HashMap<String, Map<String, String>>();

	public static void temp_distance() throws IOException {
		@SuppressWarnings("resource")
		BufferedReader reader = new BufferedReader(
				new FileReader("C:\\Users\\Haakon Andre\\OneDrive\\jalien\\seDistance.csv"));
		reader.readLine();
		Map<String, Map<String, String>> seTmp = new HashMap<String, Map<String, String>>();
		Map<String, Map<String, String>> seList = new HashMap<String, Map<String, String>>();

		List<String> se = new ArrayList<String>();
		String line1 = reader.readLine();
		String[] tmp = line1.split(",");
		boolean start = false;
		String startS = tmp[0];
		se.add(tmp[1]);
		seTmp.put(startS, new HashMap<String, String>());
		seTmp.get(startS).put(tmp[1], tmp[2]);
		while ((line1 = reader.readLine()) != null) {
			tmp = line1.split(",");
			if (tmp[0].equals(startS)) {
				if (!start) {
					se.add(tmp[1]);
					System.out.println(tmp[1]);
				}
			} else {
				start = true;
				startS = tmp[0];
				seTmp.put(startS, new HashMap<String, String>());
			}
			seTmp.get(startS).put(tmp[1], tmp[2]);
		}

		Pattern p = Pattern.compile("^.*::(.*?)::.*"); // java.util.regex.Pattern;

		for (String s : se) {
			Matcher m = p.matcher(s);
			String sTmp;
			if (m.find()) {
				sTmp = m.group(1);
				System.out.println("Match is: " + sTmp);
				if (seTmp.containsKey(sTmp)) {
					seList.put(s, seTmp.get(sTmp));
				} else
					System.out.println("Does not contain key: " + sTmp);
			} else
				System.out.println("Did not find match for: " + s);
		}

		@SuppressWarnings("resource")
		PrintWriter writer = new PrintWriter("C:\\Users\\Haakon Andre\\OneDrive\\jalien\\seDistance2.csv");
		for (Map.Entry<String, Map<String, String>> entry : seList.entrySet()) {
			for (Entry<String, String> entry2 : entry.getValue().entrySet()) {
				writer.println(String.join(",", entry.getKey(), entry2.getKey(), entry2.getValue()));
			}
		}

	}

	@SuppressWarnings("resource")
	public static void main(final String[] args) throws IOException {
		Map<String, List<String>> seList = new HashMap<String, List<String>>();
		Map<String, List<String>> seListLow = new HashMap<String, List<String>>();
		Map<Integer, Integer> jobPlot = new HashMap<Integer, Integer>();
		try {

			// temp_distance();

			BufferedReader reader = new BufferedReader(
					new FileReader("C:\\Users\\Haakon Andre\\OneDrive\\jalien\\whereis2.csv"));
			reader.readLine();
			reader.readLine();

			String line1 = null;
			while ((line1 = reader.readLine()) != null) {
				String[] tmp = line1.split(",");

				String[] seTmp = tmp[2].split(";");
				Arrays.sort(seTmp);

				String seFin = String.join(",", seTmp);

				if (!seList.containsKey(seFin)) {
					seList.put(seFin, new ArrayList<String>());
				}
				seList.get(seFin).add(tmp[1]);

			}

			for (Iterator<Map.Entry<String, List<String>>> iterator = seList.entrySet().iterator(); iterator
					.hasNext();) {

				Map.Entry<String, List<String>> entry = iterator.next();
				List<JDL> subjobs = new ArrayList<JDL>();
				int number = 0;

				List<String> inputdata = entry.getValue();
				System.out.println("Key = " + entry.getKey() + " -> Jobs # = " + inputdata.size());

				if (!(jobPlot.containsKey(inputdata.size()))) {
					jobPlot.put(inputdata.size(), 1);
				} else {
					int i = jobPlot.get(inputdata.size());
					jobPlot.put(inputdata.size(), i + 1);
				}

				if (inputdata.size() < MAX_SIZE / 5) {
					seListLow.put(entry.getKey(), inputdata);
				}

				if (!(inputdata.size() < (MAX_SIZE / 2))) {
					while (!inputdata.isEmpty()) {
						number = Math.min(MAX_SIZE, inputdata.size());
						if (inputdata.size() < MAX_SIZE * 2 && MAX_SIZE < inputdata.size()) {
							number = inputdata.size() / 2;
						}

						JDL j = new JDL();
						j.set("InputData", inputdata.subList(0, number));
						subjobs.add(j);
						inputdata.subList(0, number).clear();
					}
					// seList.remove(entry.getKey());
				}

				System.out.println("Number of subjobs = " + subjobs.size() + " - > Last subjobs inputdata = " + number);

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Size is: " + seListLow.size());
		getAllDistance();
		for (Map.Entry<String, Map<String, String>> entry : seDistance.entrySet()) {
			System.out.println("Key is : " + entry.getKey());
		}
		
		
		/*System.out.println("MIN TO MAX WITH LIMI!!!!!!!");
		minToMaxLimit(seListLow);
		maxToMinNoLimit(seListLow);
		System.out.println("MAX TO MIN WITH LIMI!!!!!!!");
		maxToMinLimit(seListLow);
		*/
		
		//minToMaxLimit(seListLow);
		//minToMaxExclude(seListLow);
		minToMaxShared(seListLow);

	}

	public void algorithmMinDistance(Map<String, List<String>> seListLow) {

	}

	public static void algorithmMinGroup(Map<String, List<String>> seListLow) {
		String minBasket = "";
		int min = 0;
		while (min < MAX_SIZE / 5) {
			min = 0;
			for (Entry<String, List<String>> entry : seListLow.entrySet()) {
				if (min > entry.getValue().size() || min == 0) {
					min = entry.getValue().size();
					System.out.println("Size is ! " + min);
					minBasket = entry.getKey();
				}
			}

			String cBasket = getClosest(minBasket, seListLow).split(";")[0];
			String newBasket = String.join(",", minBasket, cBasket);
			System.out.println("Basket : " + minBasket + " and: " + cBasket);
			List<String> newInput = seListLow.get(minBasket);
			newInput.addAll(seListLow.get(cBasket));
			seListLow.put(newBasket, newInput);
			seListLow.remove(minBasket);
			seListLow.remove(cBasket);
		}
		readBaskets(seListLow);

	}
	
	public static void algorithmMaxGroup(Map<String, List<String>> seListLow) {
		String minBasket = "";
		int max = 0;
		while (max < MAX_SIZE / 5) {
			max = 0;
			for (Entry<String, List<String>> entry : seListLow.entrySet()) {
				if (max < entry.getValue().size() && entry.getValue().size() < MAX_SIZE / 5 ) {
					max = entry.getValue().size();
					System.out.println("Size is ! " + max);
					minBasket = entry.getKey();
				}
			}
			
			if(max == 0)
				break;
			
			String cBasket = getClosest(minBasket, seListLow).split(";")[0];
			String newBasket = String.join(",", minBasket, cBasket);
			System.out.println("Basket : " + minBasket + " and: " + cBasket);
			List<String> newInput = seListLow.get(minBasket);
			newInput.addAll(seListLow.get(cBasket));
			seListLow.put(newBasket, newInput);
			seListLow.remove(minBasket);
			seListLow.remove(cBasket);
		}
		readBaskets(seListLow);

	}

	public static void readBaskets(Map<String, List<String>> seListLow) {
		for (Iterator<Map.Entry<String, List<String>>> iterator = seListLow.entrySet().iterator(); iterator
				.hasNext();) {
			Map.Entry<String, List<String>> entry = iterator.next();
			System.out.println("Basket is: " + entry.getKey() + " and number of inputs is: " + entry.getValue().size());
		}
	}
	
	public static void minToMaxNoLimit (Map<String, List<String>> seListLow) {
		String minBasket = "";
		int min = 0;
		while (min < MAX_SIZE / 5) {
			min = 0;
			for (Entry<String, List<String>> entry : seListLow.entrySet()) {
				if (min > entry.getValue().size() || min == 0) {
					min = entry.getValue().size();
					minBasket = entry.getKey();
				}
			}

			String cBasket = getCloseMax(minBasket, seListLow,0);
			String newBasket = String.join(",", minBasket, cBasket);
			List<String> newInput = seListLow.get(minBasket);
			newInput.addAll(seListLow.get(cBasket));
			seListLow.put(newBasket, newInput);
			seListLow.remove(minBasket);
			seListLow.remove(cBasket);
		}
		readBaskets(seListLow);

	}
	
	public static void minToMaxLimit (Map<String, List<String>> seListLow) {
		String minBasket = "";
		int min = 0;
		final int LIMIT = MAX_SIZE/5;
		while (min < MAX_SIZE / 5) {
			min = 0;
			for (Entry<String, List<String>> entry : seListLow.entrySet()) {
				if (min > entry.getValue().size() || min == 0) {
					min = entry.getValue().size();
					minBasket = entry.getKey();
				}
			}

			String cBasket = getCloseMax(minBasket, seListLow,LIMIT);
			String newBasket = String.join(",", minBasket, cBasket);
			List<String> newInput = seListLow.get(minBasket);
			newInput.addAll(seListLow.get(cBasket));
			seListLow.put(newBasket, newInput);
			seListLow.remove(minBasket);
			seListLow.remove(cBasket);
		}
		readBaskets(seListLow);

	}
	
	public static void minToMaxShared (Map<String, List<String>> seListLow) {
		String minBasket = "";
		int min = 0;
		final int LIMIT = MAX_SIZE/5;
		while (min < MAX_SIZE / 5) {
			min = 0;
			for (Entry<String, List<String>> entry : seListLow.entrySet()) {
				if (min > entry.getValue().size() || min == 0) {
					min = entry.getValue().size();
					minBasket = entry.getKey();
				}
			} 
			
			String[] tmpArr = getCloseMaxShared(minBasket, seListLow,LIMIT).split(";", 2);
			
			String cBasket = tmpArr[0];
			System.out.println("cBasket: " + cBasket);
			
			String shared = tmpArr[1];
			System.out.println("shared: " + shared);
			
			if (shared.equals("")) {
			String newBasket = String.join(",", minBasket, cBasket);
			List<String> newInput = seListLow.get(minBasket);
			newInput.addAll(seListLow.get(cBasket));
			seListLow.put(newBasket, newInput);
			seListLow.remove(minBasket);
			seListLow.remove(cBasket);
			}
			else {
				List<String> newInput = seListLow.get(minBasket);
				newInput.addAll(seListLow.get(cBasket));
				seListLow.put(shared, newInput);
				seListLow.remove(minBasket);
				seListLow.remove(cBasket);
			}
		}
		readBaskets(seListLow);

	}
	
	public static void minToMaxExclude (Map<String, List<String>> seListLow) {
		String minBasket = "";
		int min = 0;
		int count = 0;
		int divisor = 10;
		final int LIMIT = MAX_SIZE/5;
		while (min < MAX_SIZE / 5) {
			min = 0;
			for (Entry<String, List<String>> entry : seListLow.entrySet()) {
				if (min > entry.getValue().size() || min == 0) {
					min = entry.getValue().size();
					minBasket = entry.getKey();
				}
			}

			String cBasket = getCloseMax(minBasket, seListLow,LIMIT);
			
			if ((seListLow.get(minBasket).size() + divisor - 1)/ divisor >= seListLow.get(cBasket).size()) {
				seListLow.get(minBasket).addAll(seListLow.get(cBasket));
				seListLow.remove(cBasket);
				count ++;
			}
			else if (seListLow.get(minBasket).size() <= ((seListLow.get(cBasket).size() + divisor - 1) / divisor)) {
				seListLow.get(cBasket).addAll(seListLow.get(minBasket));
				seListLow.remove(minBasket);
				count++;
			}
			else {
			String newBasket = String.join(",", minBasket, cBasket);
			List<String> newInput = seListLow.get(minBasket);
			newInput.addAll(seListLow.get(cBasket));
			seListLow.put(newBasket, newInput);
			seListLow.remove(minBasket);
			seListLow.remove(cBasket);
			}
		}
		
		System.out.println("Count of missing SE is: " + count);
		readBaskets(seListLow);

	}
	
	public static String getCloseMax(String minBasket, Map<String, List<String>> seListLow, int setMax) {

		String closest = "";
		int maxBasket = 0;
		int minSize = 0;
		String minBasketTmp  = "";
		boolean foundMatch = false;
		double minDist = 0.0;
		
		boolean foundShared = false;
		String[] tmp = minBasket.split(",");
		List<String> tmpList = new ArrayList<String>();
		for (Entry<String, List<String>> entry : seListLow.entrySet()) {
			if (!(entry.getKey().equals(minBasket))) {
				boolean shared = false;
				for (String s : tmp) {
					if (entry.getKey().contains(s)) {
						shared = true;
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
						tmpDist += Double.parseDouble(seDistance.get(s).get(s2));
					}
				}
				if (minDist > tmpDist || minDist == 0.0) {
					minDist = tmpDist;
					closest = entry.getKey();
				}
			}
		}

		return closest;
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
						tmpDist += Double.parseDouble(seDistance.get(s).get(s2));
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
	
	public static void maxToMinNoLimit (Map<String, List<String>> seListLow) {
		String minBasket = "";
		int max = 0;
		while (max < MAX_SIZE / 5) {
			max = 0;
			for (Entry<String, List<String>> entry : seListLow.entrySet()) {
				if (max < entry.getValue().size() && entry.getValue().size() < MAX_SIZE / 5 ) {
					max = entry.getValue().size();
					minBasket = entry.getKey();
				}
			}
			
			if(max == 0)
				break;
			
			String cBasket = getCloseMin(minBasket, seListLow, 0);
			String newBasket = String.join(",", minBasket, cBasket);
			List<String> newInput = seListLow.get(minBasket);
			newInput.addAll(seListLow.get(cBasket));
			seListLow.put(newBasket, newInput);
			seListLow.remove(minBasket);
			seListLow.remove(cBasket);
		}
		readBaskets(seListLow);

	}
	
	public static void maxToMinLimit (Map<String, List<String>> seListLow) {
		String minBasket = "";
		int max = 0;
		final int LIMIT = MAX_SIZE / 5;
		while (max < MAX_SIZE / 5) {
			max = 0;
			for (Entry<String, List<String>> entry : seListLow.entrySet()) {
				if (max < entry.getValue().size() && entry.getValue().size() < MAX_SIZE / 5 ) {
					max = entry.getValue().size();
					minBasket = entry.getKey();
				}
			}
			
			if(max == 0)
				break;
			
			String cBasket = getCloseMin(minBasket, seListLow, LIMIT);
			String newBasket = String.join(",", minBasket, cBasket);
			List<String> newInput = seListLow.get(minBasket);
			newInput.addAll(seListLow.get(cBasket));
			seListLow.put(newBasket, newInput);
			seListLow.remove(minBasket);
			seListLow.remove(cBasket);
		}
		readBaskets(seListLow);

	}
	
	public static String getCloseMin(String minBasket, Map<String, List<String>> seListLow, int setMax) {

		String closest = "";
		int maxBasket = 0;
		int minSize = 0;
		String minBasketTmp  = "";
		boolean foundMatch = false;
		double minDist = 0.0;
		
		boolean foundShared = false;
		String[] tmp = minBasket.split(",");
		List<String> tmpList = new ArrayList<String>();
		for (Entry<String, List<String>> entry : seListLow.entrySet()) {
			if (!(entry.getKey().equals(minBasket))) {
				boolean shared = false;
				for (String s : tmp) {
					if (entry.getKey().contains(s)) {
						shared = true;
					}
				}
				if (shared) {
					foundShared = true;
					String[] tmp2 = entry.getKey().split(",");
					if ((maxBasket == 0 || maxBasket > entry.getValue().size()) && (setMax == 0 || setMax > entry.getValue().size())) {
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
						tmpDist += Double.parseDouble(seDistance.get(s).get(s2));
					}
				}
				if (minDist > tmpDist || minDist == 0.0) {
					minDist = tmpDist;
					closest = entry.getKey();
				}
			}
		}

		return closest;
	}
	
	
	public static String getClosest(String minBasket, Map<String, List<String>> seListLow) {

		String closest = "";
		double minDist = 0.0;
		boolean foundShared = false;
		String[] tmp = minBasket.split(",");
		List<String> tmpList = new ArrayList<String>();
		for (Entry<String, List<String>> entry : seListLow.entrySet()) {
			if (!(entry.getKey().equals(minBasket))) {
				boolean shared = false;
				double tmpDist = 0.0;
				for (String s : tmp) {
					if (entry.getKey().contains(s)) {
						shared = true;
					}
				}
				if (shared) {
					foundShared = true;
					String[] tmp2 = entry.getKey().split(",");
					for (String s : tmp) {
						for (String s2 : tmp2) {
							System.out.println("S = " + s + " S2= " + s2 + ", S: " + seDistance.containsKey(s));
							tmpDist += Double.parseDouble(seDistance.get(s).get(s2));
						}
					}
					if (minDist > tmpDist || minDist == 0.0) {
						minDist = tmpDist;
						closest = entry.getKey();
					}
				}
			}
		}

		if (!foundShared) {
			for (Entry<String, List<String>> entry : seListLow.entrySet()) {
				double tmpDist = 0.0;
				String[] tmp2 = entry.getKey().split(",");
				for (String s : tmp) {
					for (String s2 : tmp2) {
						tmpDist += Double.parseDouble(seDistance.get(s).get(s2));
					}
				}
				if (minDist > tmpDist || minDist == 0.0) {
					minDist = tmpDist;
					closest = entry.getKey();
				}
			}
		}

		System.out.println("Closest basket is: " + closest + " and is : " + minDist);
		return closest + ";" + minDist;
	}

	public static void getAllDistance() throws IOException {

		@SuppressWarnings("resource")
		BufferedReader reader = new BufferedReader(
				new FileReader("C:\\Users\\Haakon Andre\\OneDrive\\jalien\\seDistance2.csv"));
		String line1 = "";
		while ((line1 = reader.readLine()) != null) {
			String[] tmp = line1.split(",", 3);
			System.out.println("0= " + tmp[0] + " 1= " + tmp[1] + " 2= " + tmp[2]);
			if (!(seDistance.containsKey(tmp[0].toUpperCase()))) {
				seDistance.put(tmp[0].toUpperCase(), new HashMap<String, String>());
			}
			seDistance.get(tmp[0].toUpperCase()).put(tmp[1].toUpperCase(), tmp[2]);
		}
		System.out.println("Size is: " +seDistance.size());

	}

	public void graph(Map<String, List<String>> seList, Map<String, List<String>> seListLow,
			Map<Integer, Integer> jobPlot) throws IOException {
		Map<String, Integer> barGraph = new HashMap<String, Integer>();
		Map<String, Integer> barGraphMax = new HashMap<String, Integer>();
		for (Iterator<Map.Entry<String, List<String>>> iterator = seListLow.entrySet().iterator(); iterator
				.hasNext();) {
			Map.Entry<String, List<String>> entry = iterator.next();
			List<String> inputdata = entry.getValue();
			Matcher m = Pattern.compile("\\[([^)]+)\\]").matcher(entry.getKey());
			String tmpString = "";
			if (m.find())
				tmpString = m.group(1);
			else
				System.out.println("Error matching!");

			String[] tmp = tmpString.split(", ");
			for (String se : tmp) {
				if (!barGraph.containsKey(se)) {
					barGraph.put(se, 0);
				}
				int number = inputdata.size() + barGraph.get(se);
				barGraph.put(se, number);
			}

			if (!barGraphMax.containsKey(tmp[0]) && !barGraphMax.containsKey(tmp[1])) {
				barGraphMax.put(tmp[0], inputdata.size());
				System.out.println("New, adding: " + inputdata.size());
			} else if (barGraphMax.containsKey(tmp[0])) {
				if (barGraphMax.containsKey(tmp[1]) && barGraphMax.get(tmp[1]) > barGraphMax.get(tmp[0])) {
					int tmpData = barGraphMax.get(tmp[1]) + inputdata.size();
					System.out
							.println("Adding " + inputdata.size() + " + " + barGraphMax.get(tmp[1]) + " = " + tmpData);
					barGraphMax.put(tmp[1], tmpData);
				} else {
					int tmpData = barGraphMax.get(tmp[0]) + inputdata.size();
					;
					System.out
							.println("Adding " + inputdata.size() + " + " + barGraphMax.get(tmp[0]) + " = " + tmpData);
					barGraphMax.put(tmp[0], tmpData);
				}
			} else {
				int tmpData = barGraphMax.get(tmp[1]) + inputdata.size();
				;
				System.out.println("Adding " + inputdata.size() + " + " + barGraphMax.get(tmp[1]) + " = " + tmpData);
				barGraphMax.put(tmp[1], tmpData);
			}

		}

		String barLabel = "";
		String barNumber = "";

		String barLabelMax = "";
		String barNumberMax = "";

		for (Iterator<Map.Entry<String, Integer>> iterator = barGraphMax.entrySet().iterator(); iterator.hasNext();) {
			Map.Entry<String, Integer> entry = iterator.next();
			barLabelMax += entry.getKey() + " ";
			barNumberMax += entry.getValue() + " ";
			System.out.println("SEMax: " + entry.getKey() + " | Max Number of jobs: " + entry.getValue());
		}

		for (Iterator<Map.Entry<String, Integer>> iterator = barGraph.entrySet().iterator(); iterator.hasNext();) {
			Map.Entry<String, Integer> entry = iterator.next();
			barLabel += entry.getKey() + " ";
			barNumber += entry.getValue() + " ";
			System.out.println("SE: " + entry.getKey() + " | Number of jobs: " + entry.getValue());

		}

		PrintWriter writerBar = new PrintWriter("C:\\Users\\Haakon Andre\\OneDrive\\jalien\\jobBar.txt");
		System.out.println(barLabel);
		System.out.println(barNumber);
		writerBar.println(barLabel);
		writerBar.println(barNumber);
		writerBar.close();

		PrintWriter writerBarMax = new PrintWriter("C:\\Users\\Haakon Andre\\OneDrive\\jalien\\jobBarMax.txt");
		System.out.println(barLabelMax);
		System.out.println(barNumberMax);
		writerBarMax.println(barLabelMax);
		writerBarMax.println(barNumberMax);
		writerBarMax.close();

		// File file = new File ("C:\\\\\\\\Users\\\\\\\\Haakon
		// Andre\\\\\\\\OneDrive\\\\\\\\jalien\\\\\\\\jobPlot.txt");
		PrintWriter writer = new PrintWriter("C:\\Users\\Haakon Andre\\OneDrive\\jalien\\jobPlot.txt");
		for (Entry<Integer, Integer> entry : jobPlot.entrySet()) {

			System.out.print(entry.getKey() + " ");
			writer.println(entry.getKey() + " " + entry.getValue() + " ");

		}
		writer.close();

		BufferedReader reader = new BufferedReader(
				new FileReader("C:\\Users\\Haakon Andre\\OneDrive\\jalien\\seDistance.csv"));
		reader.readLine();

		List<String[]> distance = new ArrayList<String[]>();
		String line1 = "";

		while ((line1 = reader.readLine()) != null) {
			String[] tmp = line1.split(",");
			distance.add(tmp);

		}

		for (Entry<String, List<String>> entry : seList.entrySet()) {

		}
	}

}
