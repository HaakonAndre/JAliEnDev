package alien.ui.api;

import alien.se.SE;
import alien.se.SEUtils;
import alien.ui.Request;

/**
 * Get the SE object for String
 * @author ron
 * @since Jun 03, 2011
 */
public class SEfromString extends Request {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1720547988105993480L;
	
	private final String sSE;
	private final boolean evenIfDoesNotExist;

	private SE se;
	
	/**
	 * @param sSE
	 * @param evenIfDoesNotExist
	 */
	public SEfromString(final String path, final boolean evenIfDoesNotExist){
		this.sSE = path;
		this.evenIfDoesNotExist = evenIfDoesNotExist;
	}
	
	@Override
	public void run() {
		this.se = SEUtils.getSE(sSE);
	}
	
	/**
	 * @return the requested SE
	 */
	public SE getSE(){
		return this.se;
	}
	
	@Override
	public String toString() {
		return "Asked for : "+this.sSE+" ("+this.evenIfDoesNotExist+"), reply is:\n"+this.se;
	}
}
