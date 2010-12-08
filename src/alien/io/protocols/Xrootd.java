/**
 * 
 */
package alien.io.protocols;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import lia.util.process.ExternalProcessBuilder;
import lia.util.process.ExternalProcess.ExecutorFinishStatus;
import lia.util.process.ExternalProcess.ExitStatus;
import alien.catalogue.PFN;
import alien.catalogue.access.CatalogueAccess;
import alien.catalogue.access.CatalogueReadAccess;
import alien.catalogue.access.CatalogueWriteAccess;
import alien.catalogue.access.XrootDEnvelope;
import alien.config.ConfigUtils;

/**
 * @author costing
 * @since Dec 8, 2010
 */
public class Xrootd extends Protocol {
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(Xrootd.class.getCanonicalName());
	
	/**
	 * package protected
	 */
	Xrootd(){
		// package protected
	}
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7442913364052285097L;

	/* (non-Javadoc)
	 * @see alien.io.protocols.Protocol#get(alien.catalogue.PFN, alien.catalogue.access.CatalogueReadAccess, java.lang.String)
	 */
	@Override
	public File get(final PFN pfn, final CatalogueReadAccess access, final File localFile) throws IOException {
		File target = null;

		if (localFile!=null){
			target = localFile;
			
			if (!target.createNewFile())
				throw new IOException("Local file "+localFile+" could not be created");
		}
		
		if (target==null){
			target = File.createTempFile("xrootd", null);
		}
		
		try{
			final List<String> command = new LinkedList<String>();
			command.add("xrdcpapmon");
			command.add("-DIFirstConnectMaxCnt");
			command.add("6");
			command.add(pfn.pfn);
			command.add(target.getCanonicalPath());
			
			if (access!=null)
				for (final XrootDEnvelope envelope: access.getEnvelopes())
					command.add("-OS&authz="+envelope.getEncryptedEnvelope());
			
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
	        
			if (!checkDownloadedFile(target, access))
				throw new IOException("Local file doesn't match catalogue details");
		}
		catch (final IOException ioe){
			target.delete();
			
			throw ioe;
		}
		catch (final Throwable t){
			target.delete();
			
			logger.log(Level.WARNING, "Caught exception", t);
			
			throw new IOException("Get aborted because "+t);
		}
		
		return target;
	}
	
	/* (non-Javadoc)
	 * @see alien.io.protocols.Protocol#put(alien.catalogue.PFN, alien.catalogue.access.CatalogueWriteAccess, java.lang.String)
	 */
	@Override
	public String put(final PFN pfn, final CatalogueWriteAccess access, final File localFile) throws IOException {
		if (localFile==null || !localFile.exists() || !localFile.isFile() || !localFile.canRead())
			throw new IOException("Local file "+localFile+" cannot be read");
		
		try{
			final List<String> command = new LinkedList<String>();
			command.add("xrdcpapmon");
			command.add("-DIFirstConnectMaxCnt");
			command.add("6");
			command.add("-np");
			command.add("-v");
			command.add("-f");
			command.add(localFile.getCanonicalPath());
			command.add(pfn.pfn);
			
			if (access!=null)
				for (final XrootDEnvelope envelope: access.getEnvelopes())
					command.add("-OS&authz="+envelope.getEncryptedEnvelope());
			
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
	        
	        return xrdstat(pfn, access);
		}
		catch (final IOException ioe){
			throw ioe;
		}
		catch (final Throwable t){
			logger.log(Level.WARNING, "Caught exception", t);
			
			throw new IOException("Get aborted because "+t);
		}
	}
	
	/**
	 * Check if the PFN has the correct properties, such as described in the access envelope
	 * 
	 * @param pfn
	 * @param access
	 * @return the signed envelope from the storage, if it knows how to generate one
	 * @throws IOException if the remote file properties are not what is expected
	 */
	public static String xrdstat(final PFN pfn, final CatalogueAccess access) throws IOException {
		// TODO implement remote file status checking by xrdstat
		
		return null;
	}
	
	/* (non-Javadoc)
	 * @see alien.io.protocols.Protocol#transfer(alien.catalogue.PFN, alien.catalogue.access.CatalogueReadAccess, alien.catalogue.PFN, alien.catalogue.access.CatalogueWriteAccess)
	 */
	@Override
	public String transfer(final PFN source, final CatalogueReadAccess sourceAccess, final PFN target, final CatalogueWriteAccess targetAccess) throws IOException {
		final File temp = get(source, sourceAccess, null);
		
		try{
			return put(target, targetAccess, temp);
		}
		finally{
			temp.delete();
		}
	}
}
