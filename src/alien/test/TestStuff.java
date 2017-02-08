package alien.test;

import java.util.List;

import alien.catalogue.LFN_CSD;
import alien.test.cassandra.DBCassandra;

public class TestStuff {

	public static void main(String[] args) {

		System.out.println("Starting");

//		LFN_CSD lfnc = new LFN_CSD("/cassandra/0/89/4251/file9_9894251", true);
//		System.out.println(lfnc.toString());
//		
//		final List<LFN_CSD> list = lfnc.list();
//		
//		for (LFN_CSD l : list){
//			System.out.println("LFN: "+l.path+" "+l.child);
//		}
		final List<LFN_CSD> found = LFN_CSD.find("/cassandra/0/89/4251/", "file10_2*", "", "");
		
		if(found == null) {
			System.out.println("Null found");
			DBCassandra.shutdown();
			return;
		}
		
		for (LFN_CSD l : found){
			System.out.println(l.path+l.child);
		}

		DBCassandra.shutdown();
	}

}
