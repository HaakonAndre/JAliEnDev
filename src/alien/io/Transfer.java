/**
 * 
 */
package alien.io;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.catalogue.GUID;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.io.protocols.Factory;
import alien.io.protocols.Protocol;
import alien.io.protocols.SourceException;
import alien.io.protocols.TargetException;
import alien.io.protocols.TempFileManager;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;

/**
 * @author costing
 * @since Dec 8, 2010
 */
public class Transfer implements Serializable, Runnable {
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(Transfer.class.getCanonicalName());
	
	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(Transfer.class.getCanonicalName());
	
	/**
	 * Transfer was successful
	 */
	public static final int OK = 0;
	
	/**
	 * Failed from internal reasons
	 */
	public static final int FAILED_SYSTEM = 1;
	
	/**
	 * Transfer failed reading the source
	 */
	public static final int FAILED_SOURCE = 2;
	
	/**
	 * Transfer failed writing the target
	 */
	public static final int FAILED_TARGET = 3;
	
	/**
	 * Transfer should be retried later (currently staging from tape for example)
	 */
	public static final int DELAYED = 10;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4620016257875988468L;
	
	private final int transferId;
	
	/**
	 * Source pfns, package protected
	 */
	final List<PFN> sources;
	
	/**
	 * Target pfn, package protected
	 */
	final PFN target;
	
	private String targetPFN;
	
	private String storageReplyEnvelope;
	
	private GUID referenceGUID = null;
	
	/**
	 * @param transferId transfer ID
	 * @param sources source PFNs (one or more, sorted by preference)
	 * @param target target PFN, can be <code>null</code> if the file is to be copied to the local disk in a temporary file
	 */
	public Transfer(final int transferId, final List<PFN> sources, final PFN target){
		this.sources = sources;
		
		if (this.sources==null || this.sources.size()==0)
			throw new IllegalArgumentException("No sources for this transfer");
		
		this.target = target;
		
		this.transferId = transferId;
	}
	
	/**
	 * Get the list of protocols via which this PFN can be accessed
	 * 
	 * @param pfn
	 * @return list of protocols
	 */
	public static List<Protocol> getAccessProtocols(final PFN pfn){
		return getProtocols(pfn, true);
	}
	
	/**
	 * Get the protocols supported by this guy
	 * 
	 * @param pfn
	 * @return list of protocols
	 */
	public static List<Protocol> getProtocols(final PFN pfn){
		return getProtocols(pfn, false);
	}
	
	private static List<Protocol> getProtocols(final PFN pfn, final boolean onlyAccess){
		final List<Protocol> ret = new LinkedList<Protocol>();
		
		if (pfn==null)
			return ret;
		
		final String sPFN = pfn.pfn;
		
		if (sPFN==null || sPFN.length()==0)
			return ret;
		
		final int idx = sPFN.indexOf("://");
		
		if (idx<=0)
			return ret;
		
		final String s = sPFN.substring(0, idx).trim().toLowerCase();
		
		if (s.equals("root")){
			if (!onlyAccess)
				ret.add(Factory.xrd3cp);
			
			ret.add(Factory.xrootd);
		}
		else
		if (s.equals("http")){
			ret.add(Factory.http);
		}
		else
		if (s.equals("torrent")){
			ret.add(Factory.torrent);
		}
		else
		if (s.equals("file")){
				ret.add(Factory.cp);
			}
		
		return ret;
	}
	
	/**
	 * Get the protocols that are common between these two PFNs
	 * 
	 * @param source
	 * @param target target PFN (can be <code>null</code>, meaning a local temporary file)
	 * @return protocols that match both
	 */
	public static List<Protocol> getProtocols(final PFN source, final PFN target){
		final List<Protocol> ret = getProtocols(source);
		
		final List<Protocol> targetProtocols = getProtocols(target);
		
		ret.retainAll(targetProtocols);
		
		return ret;
	}
	
	private int exitCode = -1;

	private String failureReason = null;	

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		final long started = System.currentTimeMillis();
		
		doWork();
		
		final long ended = System.currentTimeMillis();
		
		if (monitor!=null){
			monitor.addMeasurement("transfer_time", ended-started);
			
			monitor.incrementCounter("transfer_status_"+exitCode);
			
			if (exitCode==0 && referenceGUID!=null){
				monitor.addMeasurement("transfer_MB", referenceGUID.size / (1024*1024d));
			}
		}
	}
	
	private void doWork(){
		for (final PFN source: sources){
			if (referenceGUID==null)
				referenceGUID = source.getGuid();
			
			doWork(source);
			
			// if the target is broken, don't try again
			if (exitCode == OK || exitCode == FAILED_TARGET)
				break;
		}
	}
	
	private void doWork(final PFN source){
		final List<Protocol> protocols = getProtocols(source, target);
		
		if (protocols.size()==0){
			// no common protocols, will first fetch locally then upload to the target, if possible
			
			final List<Protocol> protocolsSource = getProtocols(source);
			
			if (protocolsSource.size()==0){
				exitCode = FAILED_SOURCE;
				failureReason = "No known protocols for source PFN "+source;
				return;
			}
			
			if (target==null){
				// file should be written locally
				
				for (final Protocol p: protocolsSource){
					try{
						targetPFN = p.get(source, null).getCanonicalPath();
						
						exitCode = OK;
						failureReason = null;
						return;
					}
					catch (final UnsupportedOperationException uoe){
						// ignore
					}
					catch (final IOException ioe){
						exitCode = FAILED_SOURCE;
						failureReason = ioe.getMessage();
					}
				}
				
				return;
			}
			
			final List<Protocol> protocolsTarget = getProtocols(target);
			
			if (protocolsTarget.size()==0){
				exitCode = FAILED_TARGET;
				failureReason = "No known protocols for target PFN "+target;
				return;
			}
			
			File temp = null;  
			
			for (final Protocol p: protocolsSource){
				try{
					temp = p.get(source, null);
					break;
				}
				catch (final UnsupportedOperationException uoe){
					// ignore
				}
				catch (final IOException ioe){
					exitCode = FAILED_SOURCE;
					failureReason = ioe.getMessage();
				}
			}
			
			if (temp==null)
				return;
			
			for (final Protocol p: protocolsTarget){
				try{
					storageReplyEnvelope = p.put(target, temp);
					exitCode = OK;
					failureReason = null;
					
					targetPFN = target.pfn;
										
					return;
				}
				catch (UnsupportedOperationException uoe){
					// ignore
				}
				catch (IOException ioe){
					exitCode = FAILED_TARGET;
					failureReason = ioe.getMessage();
				}
				finally{
					TempFileManager.release(temp);
				}
			}
			
			return;
		}
		
		for (final Protocol p: protocols){
			try{
				storageReplyEnvelope = p.transfer(source, target);
				
				exitCode = OK;
				failureReason = "OK: "+p.getClass().getSimpleName()+" ("+source.getPFN()+" -> "+target.getPFN()+")";
				
				targetPFN = target.pfn;
				
				return;
			}
			catch (final UnsupportedOperationException uoe){
				// ignore, move to the next one
			}
			catch (final SourceException se){
				exitCode = FAILED_SOURCE;
				failureReason = se.getMessage();
				
				logger.log(Level.WARNING, "Transfer "+transferId+", "+p.getClass().getSimpleName()+" ("+source.getPFN()+" -> "+target.getPFN()+") failed with source exception: "+failureReason);
				
				// don't try other methods on this source, it is known to be broken
				break;
			}
			catch (final TargetException se){
				exitCode = FAILED_TARGET;
				failureReason = se.getMessage();
				
				logger.log(Level.WARNING, "Transfer "+transferId+", "+p.getClass().getSimpleName()+" ("+source.getPFN()+" -> "+target.getPFN()+") failed with target exception: "+failureReason);
			}
			catch (final IOException e){
				exitCode = FAILED_SOURCE;
				failureReason = e.getMessage();
				
				logger.log(Level.WARNING, "Transfer "+transferId+", "+p.getClass().getSimpleName()+" ("+source.getPFN()+" -> "+target.getPFN()+") failed with generic exception: "+failureReason);
			}
		}
		
		exitCode = FAILED_SYSTEM;
		
		if (failureReason==null)
			failureReason = "None of the protocols managed to perform the transfer";
	}
	
	/**
	 * @return the exit code, if &lt;0 then the operation is ongoing still, if 0 then the transfer was successful, otherwise it failed
	 * and the message is in {@link #getFailureReason()}
	 */
	public int getExitCode(){
		return exitCode;
	}
	
	/**
	 * @return the failure reason, if any
	 */
	public String getFailureReason(){
		return failureReason;
	}
	
	/**
	 * For a successful operation, get the PFN of the target, either the local (no protocol) or remote (with protocol) file
	 * 
	 * @return target PFN
	 */
	public String getTargetPFN(){
		return targetPFN;
	}
	
	/**
	 * For a successful operation, get the reply envelope for the storage
	 * 
	 * @return storage reply envelope 
	 */
	public String getStorageReplyEnvelope(){
		return storageReplyEnvelope;
	}
	
	/**
	 * @return the transfer ID
	 */
	public int getTransferId() {
		return transferId;
	}
	
	@Override
	public String toString() {
		return "ID: "+transferId+", exitCode: "+exitCode+", reason: "+failureReason;
	}
}
