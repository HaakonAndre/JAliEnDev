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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * @author costing
 *
 */
public class CacheLogAnalyzer {

	private static final Map<String, Map<String, Map<String, AtomicInteger>>> stats = new TreeMap<String, Map<String, Map<String, AtomicInteger>>>();
	
	private static void incStats(final String ns1, final String ns2, final String key, final int hit){
		Map<String, Map<String, AtomicInteger>> h1 = stats.get(ns1);
		
		if (h1==null){
			h1 = new TreeMap<String, Map<String, AtomicInteger>>();
			stats.put(ns1, h1);
		}
		
		Map<String, AtomicInteger> h2 = h1.get(ns2);
		
		if (h2==null){
			h2 = new TreeMap<String, AtomicInteger>();
			h1.put(ns2, h2);
		}
		
		AtomicInteger ai = h2.get(key);
		
		if (ai==null){
			ai = new AtomicInteger(hit);
			h2.put(key, ai);
		}
		
		ai.addAndGet(hit);
	}
	
	private static final Pattern SE_NAME_PATTERN = Pattern.compile("^.+::.+::.+$");
	
	private static final Pattern YEAR = Pattern.compile("^20[01][0-9]$");
	
	private static final Pattern RUNNO = Pattern.compile("^\\d{6,9}$");
	
	private static final Pattern RUNNO2 = Pattern.compile("^Run\\d{6,9}\\_");
	
	private static void processAccess(final int hit, final String key){
		int idx = key.lastIndexOf('_');
		
		String image = key.substring(idx+1);
		
		incStats("access", "image_no", image, hit);
		
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
		
		// now let's analyze the LFN for interesting bits
		
		final StringTokenizer st = new StringTokenizer(lfn, "/");
				
		boolean runfound = false;
		boolean yearfound = false;
		boolean periodfound = false;
		
		while (st.hasMoreTokens()){
			final String tok = st.nextToken();
			
			if (tok.equals("data"))
				incStats("access", "data_type", "raw", hit);
			else
			if (tok.equals("sim"))
				incStats("access", "data_type", "sim", hit);
			else
			if (!yearfound && !runfound && !periodfound && YEAR.matcher(tok).matches()){
				yearfound = true;
				incStats("access", "year", tok, hit);
			}
			else
			if (!runfound && RUNNO.matcher(tok).matches()){
				runfound = true;				
				String run = tok;
				
				while (run.length()>0 && run.charAt(0)=='0')
					run=run.substring(1);
				
				incStats("access", "runno", run, hit);
			}
			else
			if (tok.startsWith("pass"))
				incStats("access", "pass", tok, hit);
			else
			if (tok.equals("AOD"))
				incStats("access", "data", "AOD", hit);
			else
			if (tok.equals("ESDs"))
				incStats("access", "data", "ESDs", hit);
			else
			if (tok.equals("raw"))
				incStats("access", "data", "raw", hit);
			else
			if (!periodfound && tok.startsWith("LHC1")){
				periodfound = true;
				incStats("access", "period", tok, hit);
			}
			else
			if (tok.equals("OCDB")){
				incStats("access", "data", "OCDB", hit);
				
				String tok2 = st.nextToken();
				
				incStats("access", "OCDB_DET", tok2, hit);
			}
			
			if (!runfound && RUNNO2.matcher(tok).matches()){
				runfound = true;
				String run = tok.substring(3, tok.indexOf('_'));
				incStats("access", "runno", run, hit);
			}
		}
	}
	
	private static void processEnvelope(final int hit, final String key){
		incStats("envelope", "account", key.substring(0, key.indexOf('_')), hit);
	}
	
	/**
	 * @param args
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	public static void main(String args[]) throws NumberFormatException, IOException{
		BufferedReader br = new BufferedReader(new FileReader("cache.log"));
		
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
				
				for (final Map.Entry<String, AtomicInteger> me3: sortByAccesses(me2.getValue())){
					final AtomicInteger ai = me3.getValue(); 
					
					System.err.println(me3.getKey()+" : "+ai);
					
					lTotal += ai.intValue();
				}
				
				System.err.println("TOTAL : "+lTotal);
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
