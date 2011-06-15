package utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lazyj.Format;

/**
 * @author costing
 *
 */
public class CacheLogAnalyzer {

	private static final Map<String, Map<String, Map<String, AtomicInteger>>> stats = new TreeMap<String, Map<String, Map<String, AtomicInteger>>>();
	
	private static String cachedFirstLevelKey = null;
	private static Map<String, Map<String, AtomicInteger>> cachedFirstLevel = null;
	
	private static String cachedSecondLevelKey = null;
	private static Map<String, AtomicInteger> cachedSecondLevel = null;
	
	private static String cachedKey = null;
	private static AtomicInteger cached = null;
	
	private static void incStats(final String ns1, final String ns2, final String key, final int hit){
		Map<String, Map<String, AtomicInteger>> h1;
		Map<String, AtomicInteger> h2;
		AtomicInteger ai;
		
		if (ns1.equals(cachedFirstLevelKey)){
			if (ns2.equals(cachedSecondLevelKey)){				
				if (key.equals(cachedKey)){
					cached.addAndGet(hit);
					return;
				}

				h2 = cachedSecondLevel;
				ai = null;
			}
			else{
				h2 = null;
				ai = null;
			}
			
			h1 = cachedFirstLevel;
		}
		else{
			h1 = stats.get(ns1);
					
			if (h1==null){
				h1 = new TreeMap<String, Map<String, AtomicInteger>>();
				stats.put(ns1, h1);
			}
			
			cachedFirstLevelKey = ns1;
			cachedFirstLevel = h1;
			
			h2 = null;
			ai = null;
		}
		
		if (h2 == null){
			h2 = h1.get(ns2);
				
			if (h2==null){
				h2 = new ConcurrentHashMap<String, AtomicInteger>(10240);
				h1.put(ns2, h2);
			}
			
			cachedSecondLevelKey = ns2;
			cachedSecondLevel = h2;
		}
		
		ai = h2.get(key);
			
		if (ai==null){
			ai = new AtomicInteger(hit);
			h2.put(key, ai);
		}
			
		cachedKey = key;
		cached = ai;
		
		ai.addAndGet(hit);
	}
	
	private static final Pattern SE_NAME_PATTERN = Pattern.compile("^.+::.+::.+$");
	
	private static final Pattern RAW = Pattern.compile("^/alice/data/(20[01][0-9])/(LHC[^/]+)/[0]{0,3}([0-9]{6,9})/.*");
	
	private static final Pattern ESD = Pattern.compile(".*/ESDs/(pass[^/]+)/.*");
	
	private static final Pattern AOD = Pattern.compile(".*/ESDs/(pass[^/]+)/AOD[0-9]{3}/.*");
	
	private static final Pattern RAWOCDB = Pattern.compile("^/alice/data/(20[01][0-9])/OCDB/([^/]+)/.*");
	
	// /alice/sim/LHC11b10b/146824/AOD046/0091/
	private static final Pattern SIM = Pattern.compile("^/alice/sim/(LHC[^/]+)/([0-9]{6,9})/.*");
	
	private static final Pattern SIM_OCDB = Pattern.compile("^/alice/simulation/20[01][0-9]/[^/]+/(Ideal|Residual|Full)/([^/]+)/.*");
	
	/**
	 * @param hit
	 * @param key
	 */
	static void processAccess(final int hit, final String key){
		int idx = key.lastIndexOf('_');
		
		String image = key.substring(idx+1);
		
		incStats("access", "image_no", image, hit);
		System.out.println("Incrementing image_no "+image+" with "+hit);
		
		int idx2 = key.lastIndexOf('_', idx-1);
		
		//String unknown = key.substring(idx2+1, idx);
		
		int idx3 = key.lastIndexOf('_', idx2-1);
		
		String se = key.substring(idx3+1, idx2).toUpperCase();
		
		if (se.length()>0 && !SE_NAME_PATTERN.matcher(se).matches()){
			idx3 = key.lastIndexOf('_', idx3-1);
			se = key.substring(idx3+1, idx2).toUpperCase();
		}

		incStats("access", "sename", se, hit);
		
		int idx4 = key.lastIndexOf('_', idx3-1);
		
		String site = key.substring(idx4+1, idx3);
		
		incStats("access", "site", site, hit);
		
		String lfn = key.substring(0, idx4);
		
		if (lfn.equals("/alice/data/OCDBFoldervsRunRange.xml")){
			incStats("access", "data_type", "raw_ocdb_range_xml", hit);
			return;
		}
		
		incStats("access", "hot_lfns", lfn, hit);
		System.out.println("Incrementing lfn "+lfn+" with "+hit);
		
		if (lfn.startsWith("/alice/data/")){
			if (lfn.indexOf("/OCDB/")>=0){
				Matcher mocdb = RAWOCDB.matcher(lfn);
				
				if (mocdb.matches()){
					incStats("access", "data_type", "raw_ocdb", hit);
					
					String year = mocdb.group(1);
					
					String det = mocdb.group(2);
					
					incStats("access", "raw_ocdb_year", year, hit);
					
					incStats("access", "raw_ocdb_det", det, hit);
					
					return;
				}

				System.out.println("This LFN contains /OCDB/ but doesn't match the pattern : "+lfn);
				
				return;
			}
			
			Matcher mraw = RAW.matcher(lfn);
			
			if (mraw.matches()){
				incStats("access", "data_type", "raw", hit);
				
				String year = mraw.group(1);
				
				incStats("access", "raw_year", year, hit);
				
				String period = mraw.group(2);
				
				incStats("access", "raw_period", period, hit);
				
				String run = mraw.group(3);
				
				incStats("access", "raw_run", run, hit);
				
				Matcher m = lfn.indexOf("/AOD")>=0 ? AOD.matcher(lfn) : null;
				
				if (m!=null && m.matches()){
					incStats("access", "raw_data", "AOD", hit);
					
					incStats("access", "raw_data_pass", m.group(1), hit);
				}
				else{
					m = lfn.indexOf("/ESDs/")>=0 ? ESD.matcher(lfn) : null;
					
					if (m!=null && m.matches()){
						incStats("access", "raw_data", "ESDs", hit);
						
						incStats("access", "raw_data_pass", m.group(1), hit);
					}
					else
					if (lfn.indexOf("/raw/")>=0){
						incStats("access", "raw_data", "raw", hit);
					}
					else{
						System.out.println("What is this raw file ? "+lfn);
					}
				}
				
				return;
			}
			
			System.out.println("What is this raw data file : "+lfn);
			
			return;
		}
		
		if (lfn.startsWith("/alice/simulation/")){
			Matcher m = SIM_OCDB.matcher(lfn);
			
			if (m.matches()){
				String type = m.group(1);
				String det = m.group(2);
				
				incStats("access", "data_type", "sim_ocdb_"+type, hit);
				incStats("access", "sim_ocdb_det", det, hit);
				
				return;
			}
			
			System.out.println("What is this simulation file : "+lfn);
			
			return;
		}
		
		if (lfn.startsWith("/alice/sim/")){
			// /alice/sim/LHC11b10b/146824/AOD046/0091/
			
			Matcher m = SIM.matcher(lfn);
			
			if (m.matches()){
				String period = m.group(1);
				String run = m.group(2);
				
				incStats("access", "data_type", "sim", hit);
				incStats("access", "sim_period", period, hit);
				incStats("access", "sim_run", run, hit);
				
				return;
			}
			
			System.out.println("What is this sim file : "+lfn);
			
			return;
		}
		
		if (lfn.startsWith("/alice/cern.ch/user/")){
			incStats("access", "data_type", "user", hit);
			
			return;
		}
		
		System.out.println("What is this other file : "+lfn);
	}
	
	/**
	 * @param hit
	 * @param key
	 */
	static void processEnvelope(final int hit, final String key){
		incStats("envelope", "account", key.substring(0, key.indexOf('_')), hit);
	}
	
	/**
	 * @param args
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	public static void main(final String args[]) throws NumberFormatException, IOException{
		final BufferedReader br = new BufferedReader(new FileReader("cache.log"));
		
		String sLine;
		
		while ( (sLine=br.readLine())!=null ){
			final StringTokenizer st = new StringTokenizer(sLine);
			
			// timestamp
			st.nextToken();
			final int hits = Integer.parseInt(st.nextToken());
			final String namespace = st.nextToken();
			final String key = st.nextToken();
			
			if (namespace.equals("access"))
				processAccess(hits, key);
			else
			if (namespace.equals("envelope"))
				processEnvelope(hits, key);
		}

		for (final Map.Entry<String, Map<String, Map<String, AtomicInteger>>> me: stats.entrySet()){
			System.err.println("********************* "+me.getKey());
			
			for (final Map.Entry<String, Map<String, AtomicInteger>> me2: me.getValue().entrySet()){
				System.err.println("+++++++++++++++++++ "+me2.getKey()+" ++++++++++++++++++++++");

				long lTotal = 0;
				
				for (final AtomicInteger ai: me2.getValue().values()){
					lTotal += ai.intValue();
				}
				
				int iCount = 0;
				
				for (final Map.Entry<String, AtomicInteger> me3: sortByAccesses(me2.getValue())){
					final AtomicInteger ai = me3.getValue(); 
					
					final double percentage = ai.intValue() * 100d / lTotal;
					
					System.err.println(me3.getKey()+" : "+ai+" : "+Format.point(percentage)+"%");
					
					if ((++iCount)==100 && me2.getKey().equals("hot_lfns"))
						break;
				}
				
				System.err.println("TOTAL : "+lTotal+" ("+me2.getValue().size()+" entries)");
			}
		}
	}
	
	private static final Comparator<Map.Entry<String, AtomicInteger>> entryComparator = new Comparator<Map.Entry<String,AtomicInteger>>(){

		@Override
		public int compare(Entry<String, AtomicInteger> o1, Entry<String, AtomicInteger> o2) {
			return o2.getValue().intValue() - o1.getValue().intValue();
		}
	
	}; 
	
	private static List<Map.Entry<String, AtomicInteger>> sortByAccesses(final Map<String, AtomicInteger> map){
		final List<Map.Entry<String, AtomicInteger>> ret = new ArrayList<Map.Entry<String,AtomicInteger>>(map.entrySet());
		
		Collections.sort(ret, entryComparator);
		
		return ret;
	}
	
}
