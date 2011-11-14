/**
 * 
 */
package alien.io.protocols;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcessBuilder;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;

/**
 * @author costing
 * @since Dec 8, 2010
 */
public class Xrd3cp extends Xrootd {

	/**
	 * package protected
	 */
	Xrd3cp(){
		// package protected
	}
	
	private static final String QUOTES = "";	//
	
	/* (non-Javadoc)
	 * @see alien.io.protocols.Protocol#transfer(alien.catalogue.PFN, alien.catalogue.access.CatalogueReadAccess, alien.catalogue.PFN, alien.catalogue.access.CatalogueWriteAccess)
	 */
	@Override
	public String transfer(final PFN source, final PFN target) throws IOException {
		// direct copying between two storages
		
		try{
			if (source.ticket==null || source.ticket.type != AccessType.READ) {
				throw new IOException("The ticket for source PFN " + source.toString()
						+ " could not be found or is not a READ one.");
			}
			
			if (target.ticket==null || target.ticket.type != AccessType.WRITE) {
				throw new IOException("The ticket for target PFN " + target.toString()
						+ " could not be found or is not a WRITE one.");
			}
			
			final List<String> command = new LinkedList<String>();
			command.add("xrd3cp");
			command.add("-m");
			command.add("-S");
			
			final boolean sourceEnvelope = source.ticket!=null && source.ticket.envelope!=null;
			
			final boolean targetEnvelope = target.ticket!=null && target.ticket.envelope!=null;
			
			if (sourceEnvelope)
				command.add(source.ticket.envelope.getTransactionURL());
			else
				command.add(source.pfn);
			
			if (targetEnvelope)
				command.add(target.ticket.envelope.getTransactionURL());
			else
				command.add(target.pfn);

			if (sourceEnvelope){
				if (source.ticket.envelope.getEncryptedEnvelope()!=null)
					command.add(QUOTES + "authz="+source.ticket.envelope.getEncryptedEnvelope() + QUOTES);
				else
					if (source.ticket.envelope.getSignedEnvelope()!=null)
						command.add(source.ticket.envelope.getSignedEnvelope());
			}

			if (targetEnvelope){
				if (target.ticket.envelope.getEncryptedEnvelope()!=null)
					command.add(QUOTES + "authz="+target.ticket.envelope.getEncryptedEnvelope() + QUOTES);
				else
					if (target.ticket.envelope.getSignedEnvelope()!=null)
						command.add(target.ticket.envelope.getSignedEnvelope());
			}

			final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(command);
			
	        pBuilder.returnOutputOnExit(true);
	        
	        long seconds = source.getGuid().size / 200000;	// average target speed: 200KB/s
	        
	        seconds += 5*60;	// 5 minutes extra time, handshakes and such
	        
	        pBuilder.timeout(seconds, TimeUnit.SECONDS);
	        
	        pBuilder.redirectErrorStream(true);
	        
	        final ExitStatus exitStatus;
	        
	        try{
	        	exitStatus = pBuilder.start().waitFor();
	        }
	        catch (final InterruptedException ie){
	        	throw new IOException("Interrupted while waiting for the following command to finish : "+command.toString());
	        }
	        
	        if (exitStatus.getExtProcExitStatus() != 0){
				String sMessage = parseXrootdError(exitStatus.getStdOut());
				
				logger.log(Level.WARNING, "TRANSFER failed with "+exitStatus.getStdOut()+"\nFull command was : "+command.toString());
				
				if (sMessage!=null){
					sMessage = "xrd3cp exited with "+exitStatus.getExtProcExitStatus()+": "+sMessage;
				}
				else{
					sMessage = "Exit code was " + exitStatus.getExtProcExitStatus() + " for command : " + command.toString();
				}
				
				if (exitStatus.getExtProcExitStatus() == 5 && exitStatus.getStdOut().indexOf("source or destination has 0 size")>=0){
					logger.log(Level.WARNING, "Retrying xrdstat, maybe the file shows up with the correct size in a few seconds");
					
					try{
						final String ret = xrdstat(target, (target.ticket.envelope.getSignedEnvelope()==null));
						
						if (ret!=null){
							logger.log(Level.WARNING, "xrdstat is ok, assuming transfer was successful");
							
							return ret;
						}
					}
					catch (IOException ioe){
						logger.log(Level.WARNING, "xrdstat throwed exception", ioe);
					}
				}
				
				throw new IOException(sMessage);
	        }
	        
	        return xrdstat(target,(target.ticket.envelope.getSignedEnvelope()==null));
		}
		catch (final IOException ioe){
			throw ioe;
		}
		catch (final Throwable t){
			logger.log(Level.WARNING, "Caught exception", t);
			
			throw new IOException("Transfer aborted because "+t);
		}
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "xrd3cp";
	}
}
