package alien.api.catalogue;

import alien.api.Request;
import alien.se.SE;
import alien.se.SEUtils;

/**
 * 
 * @author ron
 * @since Jun 03, 2011
 */
public class SEfromString extends Request {

	private static final long serialVersionUID = 8631851052133487066L;
	private final String sSE;
	private final int seNo;

	private SE se;
	
	/**
	 * Get SE by name
	 * @param se
	 */
	public SEfromString(final String se){
		sSE = se;
		seNo = 0;
	}
	
	/**
	 * Get SE by number
	 * @param seno 
	 */
	public SEfromString(final int seno){
		this.seNo = seno;
		sSE = null;
	}
	
	@Override
	public void run() {
		if(sSE!=null)
			this.se = SEUtils.getSE(sSE);
		else 
			this.se = SEUtils.getSE(seNo);
	}
	
	/**
	 * @return the requested SE
	 */
	public SE getSE(){
		return this.se;
	}
	
	@Override
	public String toString() {
		if(sSE!=null)
			return "Asked for : "+this.sSE+", reply is:\n"+this.se;
		else 
		return "Asked for No: "+this.seNo+", reply is:\n"+this.se;
	}
}
