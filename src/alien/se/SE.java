package alien.se;

import java.util.Set;

import lazyj.DBFunctions;

public class SE {

	String seName;
	
	int seNumber;
	
	Set<String> qos;
	
	String seioDaemons;
	
	String seStoragePath;
	
	long seUsedSpace;
	
	long seNumFiles;
	
	long seMinSize;
	
	String seType;
	
	Set<String> exclusiveUsers;
	
	String seExclusiveWrite;
	
	String seExclusiveRead;
	
	SE(final DBFunctions db){
		seName = db.gets("seName");
		
		seNumber = db.geti("seNumber");
		
		qos = parseArray(db.gets("seQoS"));
		
		seioDaemons = db.gets("seioDaemons");
		
		
	}
}
