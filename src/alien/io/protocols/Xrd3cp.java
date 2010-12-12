/**
 * 
 */
package alien.io.protocols;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import lia.util.process.ExternalProcessBuilder;
import lia.util.process.ExternalProcess.ExecutorFinishStatus;
import lia.util.process.ExternalProcess.ExitStatus;
import alien.catalogue.PFN;
import alien.catalogue.access.XrootDEnvelope;

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
	
	/* (non-Javadoc)
	 * @see alien.io.protocols.Protocol#transfer(alien.catalogue.PFN, alien.catalogue.access.CatalogueReadAccess, alien.catalogue.PFN, alien.catalogue.access.CatalogueWriteAccess)
	 */
	@Override
	public String transfer(final PFN source, final PFN target) throws IOException {
		// direct copying between two storages
		
		try{
			final List<String> command = new LinkedList<String>();
			command.add("xrd3cp");
			command.add("-m");
			command.add("-S");
			command.add(source.pfn);
			command.add(target.pfn);

			if (source.envelope == null) {
				throw new IOException("The ticket for source PFN " + source.toString()
						+ " could not be found.");
			}
			if (target.envelope == null) {
				throw new IOException("The ticket for target PFN " + target.toString()
						+ " could not be found.");
			}
			

				if (source.envelope.getEncryptedEnvelope()!=null)
					command.add("\"authz="+source.envelope.getEncryptedEnvelope()+"\"");
				else
				if (source.envelope.getSignedEnvelope()!=null)
					command.add("\"authz="+source.envelope.getSignedEnvelope()+"\"");

				
				if (source.envelope.getEncryptedEnvelope()!=null)
					command.add("\"authz="+source.envelope.getEncryptedEnvelope()+"\"");
				else
				if (source.envelope.getSignedEnvelope()!=null)
					command.add("\"authz="+source.envelope.getSignedEnvelope()+"\"");

			final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(command);
			
	        pBuilder.returnOutputOnExit(true);
	        
	        pBuilder.timeout(24, TimeUnit.HOURS);
	        
	        pBuilder.redirectErrorStream(true);
	        
	        ExitStatus exitStatus;
	        
	        try{
	        	exitStatus = pBuilder.start().waitFor();
	        }
	        catch (final InterruptedException ie){
	        	throw new IOException("Interrupted while waiting for the following command to finish : "+command.toString());
	        }
	        
	        if(exitStatus.getExecutorFinishStatus() != ExecutorFinishStatus.NORMAL) {
	            throw new IOException("Executor finish status: " + exitStatus.getExecutorFinishStatus() + " for command: " + command.toString());
	        }
	        
	        if (exitStatus.getExtProcExitStatus() != 0){
	        	throw new IOException("Exit code was not zero but "+exitStatus.getExtProcExitStatus()+" for command : "+command.toString());
	        }
	        
	        return xrdstat(target,(source.envelope.getSignedEnvelope()==null));
		}
		catch (final IOException ioe){
			throw ioe;
		}
		catch (final Throwable t){
			logger.log(Level.WARNING, "Caught exception", t);
			
			throw new IOException("Transfer aborted because "+t);
		}
	}
}
