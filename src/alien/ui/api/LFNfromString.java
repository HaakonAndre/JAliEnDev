package alien.ui.api;

import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.ui.Request;

/**
 * Get the LFN object for this path
 * 
 * @author costing
 * @since 2011-03-04
 */
public class LFNfromString extends Request {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1720547988105993480L;
	
	private final String path;
	private final boolean evenIfDoesNotExist;

	private LFN lfn;
	
	/**
	 * @param path
	 * @param evenIfDoesNotExist
	 */
	public LFNfromString(final String path, final boolean evenIfDoesNotExist){
		this.path = path;
		this.evenIfDoesNotExist = evenIfDoesNotExist;
	}
	
	@Override
	public void run() {
		this.lfn = LFNUtils.getLFN(path, evenIfDoesNotExist);
	}
	
	/**
	 * @return the requested LFN
	 */
	public LFN getLFN(){
		return this.lfn;
	}
	
	@Override
	public String toString() {
		return "Asked for : "+this.path+" ("+this.evenIfDoesNotExist+"), reply is: "+this.lfn;
	}
}
