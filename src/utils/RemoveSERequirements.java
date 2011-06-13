/**
 * 
 */
package utils;

import lazyj.DBFunctions;
import lazyj.Format;
import alien.taskQueue.TaskQueueUtils;

/**
 * @author costing
 * @since Jun 13, 2011
 */
public class RemoveSERequirements {

	/**
	 * Find waiting jobs having CloseSE requirements and remove the requirements so that they can run anywhere
	 * 
	 * @param args
	 */
	public static void main(final String[] args) {
		final DBFunctions db = TaskQueueUtils.getDB();
		
		if (args.length==0){			
			db.query("SELECT queueId, jdl FROM QUEUE where status='WAITING' and jdl rlike '.*other.CloseSE.*';");
			
			while (db.moveNext()){
				cleanupRequirements(db.geti(1), db.gets(2));
			}
		}
		else{
			for (final String arg: args){
				try{
					final int queueId = Integer.parseInt(arg);
					
					db.query("SELECT queueId, jdl FROM QUEUE where status='WAITING' and queueId="+queueId+" AND jdl rlike '.*other.CloseSE.*';");
					
					if (db.moveNext())
						cleanupRequirements(db.geti(1), db.gets(2));
				}
				catch (NumberFormatException nfe){
					db.query("SELECT queueId, jdl FROM QUEUE where status='WAITING' and jdl rlike '.*other.CloseSE.*"+Format.escSQL(arg)+".*';");
					
					if (db.moveNext())
						cleanupRequirements(db.geti(1), db.gets(2));					
				}
			}
		}
	}

	/**
	 * @param geti
	 * @param gets
	 */
	private static void cleanupRequirements(final int queueId, final String jdl) {
		int idx = jdl.indexOf("Requirements = ");
		
		if (idx<0)
			return;
		
		String newJDL = jdl.substring(0, idx);
		
		int idx2 = jdl.indexOf('\n', idx);
		
		if (idx2<0)
			idx2 = jdl.length();
		
		String requirements = jdl.substring(idx, idx2);
		
		requirements = requirements.replaceAll("member\\(other.CloseSE,\".+::.+::.+\"\\)", "true");
		
		newJDL += requirements;
		
		if (idx2<jdl.length())
			newJDL += jdl.substring(idx2);
		
		System.err.println("New JDL for "+queueId+" is \n"+newJDL);
	}
	
}
