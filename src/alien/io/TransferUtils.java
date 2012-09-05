package alien.io;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import lazyj.Format;
import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.se.SE;

/**
 * @author costing
 *
 */
public final class TransferUtils {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(TransferUtils.class.getCanonicalName());
	
	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(TransferUtils.class.getCanonicalName());
	
	/**
	 * @return database connection to the transfers
	 */
	static DBFunctions getDB(){
		return ConfigUtils.getDB("transfers");
	}
	
	/**
	 * @param id
	 * @return the transfer with the given ID
	 */
	public static TransferDetails getTransfer(final int id){
		final DBFunctions db = getDB();
		
		if (db==null)
			return null;
		
		if (monitor!=null){
			monitor.incrementCounter("TRANSFERS_db_lookup");
			monitor.incrementCounter("TRANSFERS_get_by_id");
		}
		
		db.query("SELECT * FROM TRANSFERS_DIRECT WHERE transferId="+id);
		
		if (!db.moveNext())
			return null;
		
		return new TransferDetails(db);
	}
	
	/**
	 * @param targetSE
	 * @return transfers to this SE
	 */
	public static List<TransferDetails> getActiveTransfersBySE(final String targetSE){
		final DBFunctions db = getDB();
		
		if (db==null)
			return null;

		if (monitor!=null){
			monitor.incrementCounter("TRANSFERS_db_lookup");
			monitor.incrementCounter("TRANSFERS_get_by_destination");
		}
		
		db.query("SELECT * FROM TRANSFERS_DIRECT WHERE destination='"+Format.escSQL(targetSE)+"' ORDER BY transferId");
		
		final List<TransferDetails> ret = new ArrayList<TransferDetails>(db.count());
		
		while (db.moveNext()){
			ret.add(new TransferDetails(db));
		}
		
		return ret;
	}
	
	
	/**
	 * @param username
	 * @return transfers to this SE
	 */
	public static List<TransferDetails> getActiveTransfersByUser(final String username){
		final DBFunctions db = getDB();
		
		if (db==null)
			return null;
		
		if (monitor!=null){
			monitor.incrementCounter("TRANSFERS_db_lookup");
			monitor.incrementCounter("TRANSFERS_get_by_user");
		}
		
		String q = "SELECT * FROM TRANSFERS_DIRECT ";
		
		if (username!=null && username.length()>0)
			q += "WHERE user='"+Format.escSQL(username)+"' ";
			
		q += "ORDER BY transferId"; 
		
		db.query(q);
		
		final List<TransferDetails> ret = new ArrayList<TransferDetails>(db.count());
		
		while (db.moveNext()){
			ret.add(new TransferDetails(db));
		}
		
		return ret;
	}
	
	/**
	 * @param l
	 * @param se
	 * @return the transfer ID, <code>0</code> in case the file is already on the target SE, or a negative number in case of problems (-1=wrong parameters, -2=database connection missing, -3=cannot locate real pfns
	 * 			-4=the insert query failed, -5=insert query didn't generate a transfer ID. -6=cannot locate the archive LFN to mirror (for a file inside a zip archive))
	 */
	public static int mirror(final LFN l, final SE se){
		if (l==null || !l.exists || !l.isFile() || se==null)
			return -1;

		final DBFunctions db = getDB();
		
		if (db==null)
			return -2;
		
		if (monitor!=null){
			monitor.incrementCounter("TRANSFERS_db_lookup");
			monitor.incrementCounter("TRANSFERS_get_by_lfn_and_destination");
		}
				
		final Set<PFN> pfns = l.whereisReal();
		
		if (pfns==null)
			return -3;
		
		for (final PFN p: pfns){
			if (p.seNumber == se.seNumber)
				return 0;
		}

		if (monitor!=null){
			monitor.incrementCounter("TRANSFERS_db_insert");
		}

		LFN lfnToCopy;
		
		if (!l.isReal()){
			// infer the real LFN from the same directory
			
			UUID guid = null;
			
			for (final PFN p: l.whereis()){
				if (p.pfn.startsWith("guid:/")){
					try{
						guid = UUID.fromString(p.pfn.substring(p.pfn.lastIndexOf('/')+1, p.pfn.indexOf('?')));
					}
					catch (Exception e){
						return -6;
					}
				}
			}
			
			if (guid==null)
				return -6;
			
			lfnToCopy = null;
			
			try{
				for (final LFN otherFile: l.getParentDir().list()){
					if (otherFile.isFile() && otherFile.guid.equals(guid)){
						lfnToCopy = otherFile;
						break;
					}
				}
			}
			catch (Exception e){
				return -6;
			}
			
			if (lfnToCopy == null)
				return -6;
		}
		else
			lfnToCopy = l;
				
		db.query("SELECT transferId FROM TRANSFERS_DIRECT WHERE lfn='"+Format.escSQL(lfnToCopy.getCanonicalName())+"' AND destination='"+Format.escSQL(se.seName)+"' AND status IN ('WAITING', 'TRANSFERRING');");
		
		if (db.moveNext())
			return db.geti(1);
		
		db.setLastGeneratedKey(true);
		
		if (!db.query("INSERT INTO TRANSFERS_DIRECT (lfn,destination,size,status,sent,received,options,user,type,agentid,started,finished,attempts) VALUES ('"+
				Format.escSQL(lfnToCopy.getCanonicalName())+"', '"+Format.escSQL(se.seName)+"', " +
				""+lfnToCopy.size+", 'WAITING', "+(System.currentTimeMillis()/1000)+", "+(System.currentTimeMillis()/1000)+",'ur','"+Format.escSQL(lfnToCopy.owner)+"','mirror',0,null,null,0);"))
			return -4;
		
		final Integer i = db.getLastGeneratedKey();
		
		if (i==null)
			return -5;
		
		return i.intValue();
	}
	
	/**
	 * @param guid
	 * @param se
	 * @return the transfer ID, <code>0</code> in case the file is already on the target SE, or a negative number in case of problems (-1=wrong parameters, -2=database connection missing, -3=cannot locate real pfns
	 * 			-4=the insert query failed, -5=insert query didn't generate a transfer ID. -6=cannot locate the archive LFN to mirror (for a file inside a zip archive))
	 */
	public static int mirror(final GUID guid, final SE se){
		if (guid==null || !guid.exists() || se==null)
			return -1;
		
		final Set<GUID> realGUIDs = guid.getRealGUIDs(); 
		
		final Set<PFN> pfns = new LinkedHashSet<PFN>();
		
		if (realGUIDs!=null && realGUIDs.size()>0){
			for (final GUID realId: realGUIDs){
				final Set<PFN> replicas = realId.getPFNs();
			
				if (replicas==null)
					continue;

				pfns.addAll(replicas);
			}
		}

		if (pfns.size()==0)
			return -3;
		
		for (final PFN p: pfns){
			if (p.seNumber == se.seNumber)
				return 0;
		}
		
		final DBFunctions db = getDB();
		
		final String sGUID = guid.guid.toString();
		
		db.query("SELECT transferId FROM TRANSFERS_DIRECT where lfn='"+Format.escSQL(sGUID)+"' AND destination='"+Format.escSQL(se.seName)+"' AND status in ('WAITING', 'TRANSFERRING');");
		
		if (db.moveNext())
			return db.geti(1);
		
		db.setLastGeneratedKey(true);
		
		if (!db.query("INSERT INTO TRANSFERS_DIRECT (lfn,destination,size,status,sent,received,options,user,type,agentid,started,finished,attempts) VALUES ('"+
				Format.escSQL(sGUID)+"', '"+Format.escSQL(se.seName)+"', " +
				""+guid.size+", 'WAITING', "+(System.currentTimeMillis()/1000)+", "+(System.currentTimeMillis()/1000)+",'ur','"+Format.escSQL(guid.owner)+"','mirror',0,null,null,0);"))
			return -4;
		
		final Integer i = db.getLastGeneratedKey();
		
		if (i==null)
			return -5;
		
		return i.intValue();
	}
}
