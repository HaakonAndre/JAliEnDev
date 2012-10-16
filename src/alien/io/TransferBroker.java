/**
 * 
 */
package alien.io;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import lazyj.DBFunctions.DBConnection;
import lazyj.Format;
import alien.catalogue.BookingTable;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.monitoring.MonitorFactory;
import alien.se.SE;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;
import alien.user.UserFactory;
import apmon.ApMon;

/**
 * @author costing
 * @since Dec 9, 2010
 */
public class TransferBroker {
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(TransferBroker.class.getCanonicalName());
	
	private TransferBroker(){
		// just hide it
	}
	
	private static TransferBroker instance = null;
	
	/**
	 * @return singleton
	 */
	public static synchronized TransferBroker getInstance(){
		if (instance==null){
			instance = new TransferBroker();
		}
		
		return instance;
	}
	
	private ResultSet resultSet = null;
	
	private Statement stat = null;
	
	private final void executeClose(){
		if (resultSet!=null){
			try{
				resultSet.close();
			}catch (Throwable t){
				// ignore
			}
			
			resultSet = null;
		}
		
		if (stat!=null){
			try{
				stat.close();
			}
			catch (Throwable t){
				// ignore
			}
			
			stat = null;
		}
	}
	
	private final void executeQuery(final DBConnection dbc, final String query){
		executeClose();
		
		try{
			stat = dbc.getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
			if (stat.execute(query, Statement.NO_GENERATED_KEYS)){
				resultSet = stat.getResultSet();
			}
			else{
				executeClose();
			}
		}
		catch (SQLException e){
			logger.log(Level.WARNING, "Exception executing the query", e);
		}
	}
	
	private long lastTimeNoWork = 0;
	
	/**
	 * @return the next transfer to be performed, or <code>null</code> if there is nothing to do
	 */
	public synchronized Transfer getWork(){
		if (System.currentTimeMillis() - lastTimeNoWork < 1000*30)
			return null;
		
		final DBFunctions db = ConfigUtils.getDB("transfers");
		
		if (db==null){
			logger.log(Level.WARNING, "Could not connect to the transfers database");
			
			lastTimeNoWork = System.currentTimeMillis();
			
			return null;
		}
		
		cleanup();
		
		final DBConnection dbc = db.getConnection();
		
		executeQuery(dbc, "lock tables TRANSFERS_DIRECT write, PROTOCOLS read, active_transfers read;");
		executeQuery(dbc, "select transferId,lfn,destination from TRANSFERS_DIRECT inner join PROTOCOLS on (sename=destination) where status='WAITING' and (SELECT count(1) FROM active_transfers WHERE se_name=sename)<max_transfers order by transferId asc limit 1;");
	
		int transferId = -1;
		String sLFN = null;
		String targetSE = null;
		
		try{
			if (resultSet!=null && resultSet.next()){
				transferId = resultSet.getInt(1);
				sLFN = resultSet.getString(2);
				targetSE = resultSet.getString(3);
			}
			else{
				logger.log(Level.FINE, "There is no waiting transfer in the queue");
				
				lastTimeNoWork = System.currentTimeMillis();
				
				return null;
			}
			
			if (transferId<0 || sLFN==null || sLFN.length()==0 || targetSE==null || targetSE.length()==0){
				logger.log(Level.INFO, "Transfer details are wrong");
				
				lastTimeNoWork = System.currentTimeMillis();
				
				return null;
			}

			executeQuery(dbc, "update TRANSFERS_DIRECT set status='TRANSFERRING' where transferId="+transferId+";");
		}
		catch (final Exception e){
			logger.log(Level.WARNING, "Exception fetching data from the query", e);
			// ignore
		}
		finally{
			executeQuery(dbc, "unlock tables;");
			executeClose();
			
			dbc.free();
		}
		
		GUID guid;
		final LFN lfn;
		
		boolean runningOnGUID = false;
		
		if (GUIDUtils.isValidGUID(sLFN)){
			guid = GUIDUtils.getGUID(sLFN);
			
			if (guid==null){
				logger.log(Level.WARNING, "GUID '"+sLFN+"' doesn't exist in the catalogue for transfer ID "+transferId);
				markTransfer(transferId, Transfer.FAILED_SYSTEM, "GUID was not found in the database");
				return null;
			}
			
			// because of this only admin will be allowed to mirror GUIDs without indicating the LFN (eg for storage replication)
			lfn = LFNUtils.getLFN("/"+sLFN, true);
			lfn.guid = guid.guid;
			lfn.size = guid.size;
			lfn.md5 = guid.md5;
			
			guid.lfnCache = new HashSet<LFN>();
			guid.lfnCache.add(lfn);
			
			runningOnGUID = true;
		}
		else{
			lfn = LFNUtils.getLFN(sLFN);
		
			if (!lfn.exists){
				logger.log(Level.WARNING, "LFN '"+sLFN+"' doesn't exist in the catalogue for transfer ID "+transferId);
				markTransfer(transferId, Transfer.FAILED_SYSTEM, "LFN doesn't exist in the catalogue");
				return null;
			}
		
			logger.log(Level.FINE, transferId+" : LFN is "+lfn);
			
			if (lfn.guid==null){
				logger.log(Level.WARNING, "GUID '"+lfn.guid+"' is null for transfer ID "+transferId+", lfn '"+sLFN+"'");
				markTransfer(transferId, Transfer.FAILED_SYSTEM, "GUID is null for this LFN");
				return null;
			}
		
			guid = GUIDUtils.getGUID(lfn);

			if (guid==null){
				logger.log(Level.WARNING, "GUID '"+lfn.guid+"' doesn't exist in the catalogue for transfer ID "+transferId+", lfn '"+sLFN+"'");
				markTransfer(transferId, Transfer.FAILED_SYSTEM, "GUID was not found in the database");
				return null;
			}

			guid.lfnCache = new HashSet<LFN>();
			guid.lfnCache.add(lfn);
		}
		
		logger.log(Level.FINE, transferId+" : GUID is "+guid);
			
		final Set<PFN> pfns;
		
		if (!runningOnGUID){
			pfns = lfn.whereisReal();
			
			if (pfns!=null){
				for (final PFN p: pfns){
					final GUID pfnGUID = p.getGuid();
					
					if (!pfnGUID.equals(guid)){
						logger.log(Level.INFO, "Switching to mirroring "+pfnGUID.guid+" instead of "+guid.guid+" because this is the real file for "+lfn.getCanonicalName());
						
						guid = pfnGUID;	// switch to mirroring the archive instead of the pointer to it
						
						break;
					}
				}
			}
		}
		else{
			final Set<GUID> realGUIDs = guid.getRealGUIDs(); 
			
			pfns = new LinkedHashSet<PFN>();
			
			if (realGUIDs!=null && realGUIDs.size()>0){
				for (final GUID realId: realGUIDs){
					final Set<PFN> replicas = realId.getPFNs();
				
					if (replicas==null)
						continue;
	
					pfns.addAll(replicas);
					
					if (!guid.equals(realId)){
						logger.log(Level.INFO, "Switching to mirroring "+realId.guid+" instead of "+guid.guid+" because this is the real file");
						
						guid = realId;	// switch to mirroring the archive instead of the pointer to it
					}
				}
			}
		}
		
		if (pfns==null || pfns.size()==0){
			logger.log(Level.WARNING, "No existing replicas to mirror for transfer ID "+transferId);
			markTransfer(transferId, Transfer.FAILED_SYSTEM, "No replicas to mirror");
			return null;
		}
		
		final StringTokenizer seTargetSEs = new StringTokenizer(targetSE, ",; \t\r\n");
		
		final Collection<PFN> targets = new ArrayList<PFN>();
		
		final int targetSEsCount = seTargetSEs.countTokens();
		
		int replicaExists = 0;
		int seDoesntExist = 0;
		int sourceAuthFailed = 0;
		int targetAuthFailed = 0;
		
		String lastReason = null;
		
		while (seTargetSEs.hasMoreTokens()){
			final SE se = SEUtils.getSE(seTargetSEs.nextToken());
			
			if (se==null){
				logger.log(Level.WARNING, "Target SE '"+targetSE+"' doesn't exist for transfer ID "+transferId);
				seDoesntExist++;
				continue;
			}
			
			logger.log(Level.FINE, transferId+" : Target SE is "+se);
						
			for (final PFN pfn: pfns){
				if (pfn.seNumber == se.seNumber){
					logger.log(Level.WARNING, "There already exists a replica of '"+sLFN+"' on '"+targetSE+"' for transfer ID "+transferId);
					replicaExists++;
					continue;
				}
			}
					
			for (final PFN source: pfns){
				final String reason = AuthorizationFactory.fillAccess(source, AccessType.READ);
			
				if (reason!=null){
					logger.log(Level.WARNING, "Could not obtain source authorization for transfer ID "+transferId+" : "+reason);
					sourceAuthFailed++;
					lastReason = reason;
					continue;
				}
			}
					
			final PFN target;
			
			try{
				AliEnPrincipal account = AuthorizationFactory.getDefaultUser();
				
				if (account.canBecome("admin"))
					account = UserFactory.getByUsername("admin");
				
				target = BookingTable.bookForWriting(account, lfn, guid, null, 0, se);
			}
			catch (final IOException ioe){
				final String reason = ioe.getMessage();
				logger.log(Level.WARNING, "Could not obtain target authorization for transfer ID "+transferId+" : "+reason);
				targetAuthFailed++;
				lastReason = reason;
				continue;
			}
			
			logger.log(Level.FINE, transferId+" : booked PFN is "+target);
			
			targets.add(target);
		}
		
		if (targets.size()==0){
			String message = "";
			
			if (targetSEsCount==0)
				message = "No target SE indicated";
			else{
				if (replicaExists>0)
					message = "There is already a replica on "+(replicaExists>1 ? "these storages" : "this storage")+(replicaExists<targetSEsCount ? " ("+replicaExists+")" : "");
				
				if (seDoesntExist>0){
					if (message.length()>0)
						message+=", ";
					
					message += "Target SE is not defined"+(seDoesntExist<targetSEsCount ? " ("+seDoesntExist+")" : "");
				}
				
				if (sourceAuthFailed>0){
					if (message.length()>0)
						message+=", ";
					
					message += "Source authorization failed: "+lastReason+(sourceAuthFailed<targetSEsCount ? " ("+sourceAuthFailed+")" : "");
				}
				
				if (targetAuthFailed>0){
					if (message.length()>0)
						message+=", ";
					
					message += "Target authorization failed: "+lastReason+(targetAuthFailed<targetSEsCount ? " ("+targetAuthFailed+")" : "");
				}
			}
			
			markTransfer(transferId, Transfer.FAILED_SYSTEM, message);
			return null;
		}
		
		final Transfer t = new Transfer(transferId, pfns, targets);
		
		reportMonitoring(t);
		
		return t;
	}
	
	private static final String getTransferStatus(final int exitCode){
		switch (exitCode){
			case Transfer.OK:
				return "DONE";
			case Transfer.FAILED_SOURCE:
				return "FAILED";
			case Transfer.FAILED_TARGET:
				return "FAILED";
			case Transfer.FAILED_UNKNOWN:
				return "FAILED";
			case Transfer.FAILED_SYSTEM:
				return "KILLED";
			case Transfer.DELAYED:
				return "WAITING";
			default:
				return "TRANSFERRING";
		}		
	}
	
	private static final int getAliEnTransferStatus(final int exitCode){
		switch (exitCode){
			case Transfer.OK:
				return 7;
			case Transfer.FAILED_SOURCE:
				return -1;
			case Transfer.FAILED_TARGET:
				return -1;
			case Transfer.FAILED_UNKNOWN:
				return -1;
			case Transfer.FAILED_SYSTEM:
				return -2;
			case Transfer.DELAYED:
				return -3;
			default:
				return 5;	// transferring
		}		
	}
	
	private static long lastCleanedUp = 0;
	
	private static long lastArchived = System.currentTimeMillis();
	
	private static synchronized void cleanup(){
		if (System.currentTimeMillis() - lastCleanedUp<1000*30)
			return;
		
		lastCleanedUp = System.currentTimeMillis();
		
		try{
			final DBFunctions db = ConfigUtils.getDB("transfers");
			
			if (db==null)
				return;
			
			db.query("DELETE FROM active_transfers WHERE last_active<"+((lastCleanedUp/1000) - 300));
			
			db.query("UPDATE TRANSFERS_DIRECT SET status='KILLED', finished="+(lastCleanedUp/1000)+", reason='TransferAgent no longer active' WHERE status='TRANSFERRING' AND transferId NOT IN (SELECT transfer_id FROM active_transfers);");
			
			db.query("UPDATE TRANSFERS_DIRECT SET status='WAITING' WHERE status='INSERTING';");
		}
		catch (final Throwable t){
			logger.log(Level.SEVERE, "Exception cleaning up", t);
		}
		
		if (System.currentTimeMillis() - lastArchived<1000*60*60*6)
			return;
		
		lastArchived = System.currentTimeMillis();
		
		try{
			final DBFunctions db = ConfigUtils.getDB("transfers");
			
			if (db==null)
				return;
			
			final String archiveTableName = "TRANSFERSARCHIVE"+Calendar.getInstance().get(Calendar.YEAR);
			
			final long limit = System.currentTimeMillis() - 1000*60*60*24;
			
			final boolean ok;
			
			if (db.query("SELECT 1 FROM "+archiveTableName+" LIMIT 1;", true)){
				ok = db.query("INSERT IGNORE INTO "+archiveTableName+" SELECT * FROM TRANSFERS_DIRECT WHERE finished<"+limit);
			}
			else{
				ok = db.query("CREATE TABLE "+archiveTableName+" AS SELECT * FROM TRANSFERS_DIRECT WHERE finished<"+limit);
				
				if (ok)
					db.query("CREATE UNIQUE INDEX "+archiveTableName+"_pkey ON "+archiveTableName+"(transferId);");
			}
			
			if (ok)
				db.query("DELETE FROM TRANSFERS_DIRECT WHERE finished<"+limit);
		}
		catch (final Throwable t){
			logger.log(Level.SEVERE, "Exception archiving", t);
		}
		
		lastArchived = System.currentTimeMillis();
	}
	
	/**
	 * Mark a transfer as active
	 * 
	 * @param t
	 * @param ta
	 */
	public static synchronized void touch(final Transfer t, final TransferAgent ta) {
		try {
			final DBFunctions db = ConfigUtils.getDB("transfers");

			if (db == null)
				return;

			if (t == null) {
				db.query("DELETE FROM active_transfers WHERE transfer_agent_id=? AND pid=? AND host=?;", false, Integer.valueOf(ta.getTransferAgentID()), Integer.valueOf(MonitorFactory.getSelfProcessID()), MonitorFactory.getSelfHostname());
				return;
			}

			final Map<String, Object> values = new HashMap<String, Object>();

			String seList = "";
			
			for (final PFN pfn: t.targets){
				final SE targetSE = SEUtils.getSE(pfn.seNumber);
				
				if (targetSE!=null){
					if (seList.length()>0)
						seList += ",";
					
					seList += targetSE.seName;
				}
			}
			
			
			if (seList.length()>0)
				values.put("se_name", seList);
			else
				values.put("se_name", "unknown");
			
			values.put("last_active", Long.valueOf(System.currentTimeMillis() / 1000));
			values.put("transfer_id", Integer.valueOf(t.getTransferId()));
			values.put("transfer_agent_id", Integer.valueOf(ta.getTransferAgentID()));
			values.put("pid", Integer.valueOf(MonitorFactory.getSelfProcessID()));
			values.put("host", MonitorFactory.getSelfHostname());
			
			if (t.lastTriedSE>0){
				final SE se = SEUtils.getSE(t.lastTriedSE);
				
				if (se!=null)
					values.put("active_source", se.seName);
				else
					values.put("active_source", "unknown");
			}
			else
				values.put("active_source", "");
			
			if (t.lastTriedProtocol!=null)
				values.put("active_protocol", t.lastTriedProtocol.toString());
			else
				values.put("active_protocol", "");

			db.query(DBFunctions.composeUpdate("active_transfers", values, Arrays.asList("transfer_agent_id", "pid", "host")));

			if (db.getUpdateCount() == 0)
				db.query(DBFunctions.composeInsert("active_transfers", values));
			
			db.query("UPDATE TRANSFERS_DIRECT SET status='TRANSFERRING', reason='', finished=null WHERE transferId="+t.getTransferId()+" AND status!='TRANSFERRING';");	// just in case it was presumed expired
			
			if (db.getUpdateCount()>0){
				logger.log(Level.INFO, "Re-stated "+t.getTransferId()+" to TRANSFERRING");
			}
		}
		catch (Throwable ex) {
			logger.log(Level.SEVERE, "Exception updating status", ex);
		}
	}
	
	private static boolean markTransfer(final int transferId, final int exitCode, final String reason){
		final DBFunctions db = ConfigUtils.getDB("transfers");
		
		if (db==null)
			return false;

		String formattedReason = reason;
		
		if (formattedReason!=null && formattedReason.length()>250)
			formattedReason = formattedReason.substring(0, 250);
		
		db.query("update TRANSFERS_DIRECT set status=?, reason=?, finished=? WHERE transferId=?;", false, getTransferStatus(exitCode), formattedReason, Long.valueOf(System.currentTimeMillis()/1000), Integer.valueOf(transferId));
		
		if (db.getUpdateCount()<1)
			return false;
		
		db.query("update PROTOCOLS set current_transfers=greatest(coalesce(current_transfers,0)-1,0) WHERE sename=(SELECT destination FROM TRANSFERS_DIRECT WHERE transferId=?);", false, Integer.valueOf(transferId));
		
		return true;
	}
	
	private static final void reportMonitoring(final Transfer t) {
		try {
			final ApMon apmon;

			try {
				final Vector<String> targets = new Vector<String>();
				targets.add(ConfigUtils.getConfig().gets("CS_ApMon", "aliendb4.cern.ch"));

				apmon = new ApMon(targets);
			}
			catch (Exception e) {
				logger.log(Level.WARNING, "Could not initialize apmon", e);
				return;
			}

			final String cluster = "TransferQueue_Transfers_" + ConfigUtils.getConfig().gets("Organization", "ALICE");

			final Vector<String> p = new Vector<String>();
			final Vector<Object> v = new Vector<Object>();

			p.add("statusID");
			v.add(Integer.valueOf(getAliEnTransferStatus(t.getExitCode())));

			p.add("size");
			v.add(Double.valueOf(t.sources.iterator().next().getGuid().size));

			p.add("started");
			v.add(Double.valueOf(t.startedWork / 1000d));

			if (t.getExitCode() >= Transfer.OK) {
				p.add("finished");
				v.add(Double.valueOf(System.currentTimeMillis() / 1000d));

				if (t.lastTriedSE > 0) {
					SE se = SEUtils.getSE(t.lastTriedSE);

					if (se != null) {
						p.add("SE");
						v.add(se.seName);
					}
				}
				
				if (t.lastTriedProtocol!=null){
					p.add("Protocol");
					v.add(t.lastTriedProtocol.toString());
				}
			}
			
			String owner = null;
			String seList = "";

			for (final PFN target: t.targets){
				final SE targetSE = SEUtils.getSE(target.seNumber); 
				if (targetSE!=null){
					if (seList.length()>0)
						seList+=",";
					
					seList+=targetSE.seName;
				}
				
				if (owner==null)
					owner = target.getGuid().owner;
			}
			
			if (seList.length()>0){
				p.add("destination");
				v.add(seList);
			}

			if (owner!=null){
				p.add("user");
				v.add(owner);
			}

			try {
				apmon.sendParameters(cluster, String.valueOf(t.getTransferId()), p.size(), p, v);
			}
			catch (Exception e) {
				logger.log(Level.WARNING, "Could not send apmon message: "+p+" -> "+v, e);
			}
		}
		catch (Throwable ex) {
			logger.log(Level.WARNING, "Exception reporting the monitoring", ex);
		}
	}

	/**
	 * When a transfer has completed, call this method to update the database status
	 * 
	 * @param t
	 */
	public static void notifyTransferComplete(final Transfer t) {
		// TODO : verify the storage reply envelope here

		markTransfer(t.getTransferId(), t.getExitCode(), t.getFailureReason());

		reportMonitoring(t);

		// Update the file catalog with the new replica
		AliEnPrincipal owner = AuthorizationFactory.getDefaultUser();
			
		if (owner.canBecome("admin"))
			owner = UserFactory.getByUsername("admin");

		for (final PFN target: t.getSuccessfulTransfers()){
			if (!BookingTable.commit(owner, target)){
				logger.log(Level.WARNING, "Could not commit booked transfer: "+target);
					
				markTransfer(t.getTransferId(), Transfer.FAILED_SYSTEM, "Could not commit booked transfer: "+target);
			}
		}
	}
}
