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
			if (!db.query("SELECT queueId, jdl FROM QUEUE where status='WAITING' and jdl rlike '.*other.CloseSE.*';")){
				System.err.println("Could not query the QUEUE, check your config/password.properies and config/processes.properties");
				return;
			}
			
			while (db.moveNext()){
				cleanupRequirements(db.geti(1), db.gets(2));
			}
		}
		else{
			for (final String arg: args){
				try{
					final int queueId = Integer.parseInt(arg);
					
					if (!db.query("SELECT queueId, jdl FROM QUEUE where status='WAITING' and queueId="+queueId+" AND jdl rlike '.*other.CloseSE.*';")){
						System.err.println("Could not query the QUEUE, check your config/password.properies and config/processes.properties");
						return;
					}
					
					if (db.moveNext())
						cleanupRequirements(db.geti(1), db.gets(2));
				}
				catch (NumberFormatException nfe){
					if (!db.query("SELECT queueId, jdl FROM QUEUE where status='WAITING' and jdl rlike '.*other.CloseSE.*"+Format.escSQL(arg)+".*';")){
						System.err.println("Could not query the QUEUE, check your config/password.properies and config/processes.properties");
						return;
					}					
					
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
		int idx = jdl.indexOf(" Requirements = ");
		
		if (idx<0){
			System.err.println(queueId+" : could not locate Requirements");
			return;
		}
		
		String newJDL = jdl.substring(0, idx);
		
		int idx2 = jdl.indexOf('\n', idx);
		
		if (idx2<0)
			idx2 = jdl.length();
		
		String requirements = jdl.substring(idx, idx2);
		
		System.err.println(queueId+" : old requirements : "+requirements);
		
		requirements = requirements.replaceAll("&& \\( member\\(other.CloseSE,\\\".+::.+::.+\\\"\\)( || member\\(other.CloseSE,\\\".+::.+::.+\\\"\\))* \\)", "");

		System.err.println(queueId+" : new requirements : "+requirements);
		
		newJDL += requirements;
		
		if (idx2<jdl.length())
			newJDL += jdl.substring(idx2);
		
		final DBFunctions db = TaskQueueUtils.getDB();
		
		final boolean ok = db.query("UPDATE QUEUE SET jdl='"+Format.escSQL(newJDL)+"' WHERE queueId="+queueId+" AND status='WAITING'");
		//final boolean ok = false;
		
		if (ok && db.getUpdateCount()==1)
			System.err.println(queueId+" : queue updated successfully");
		else
			System.err.println(queueId+" : failed to update the waiting job : queue ok="+ok+", update count="+db.getUpdateCount());
	}
	
}
