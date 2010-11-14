package alien.catalogue.access;

import java.util.LinkedHashSet;
import java.util.Set;

import alien.catalogue.CatalogEntity;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;


/**
 * @author ron
 * @since 2010-11-10
 */
public abstract class CatalogueAccess  {

	
	public static int INVALID = 0;
	public static int READ = 1;
	public static int WRITE = 2;
	public static int DELETE = 3;
	

	
	/**
	 * Access type
	 */
	protected int access = INVALID;
	
	protected CatalogEntity entity;
	protected LFN lfn = null;
	
	protected GUID guid = null;
	private Set<PFN> pfns = new LinkedHashSet<PFN>();
	private long size = 0;
	private String md5 = "";
	
	/** 
	 * Package protected access to the constructor.
	 * @param guid
	 * @see AuthorizationFactory
	 */
	CatalogueAccess(final CatalogEntity entity){
		this.entity = entity;
		if(entity.is() == 'l') {
			lfn = (LFN) entity;
			guid = (GUID) GUIDUtils.getGUID(lfn.guid);
		} else {
			lfn = LFNUtils.getLFN("/NOLFN", true);
			guid = (GUID) entity;
		}
		size = guid.size;
		md5 = guid.md5;
		
	}
	
	
	
	public void loadPFNS(){
		pfns = guid.getPFNs();
	}
	
	
	public int getAccess(){
		return access;
	}
	public LFN getLFN(){
		return lfn;
	}
	public GUID getGUID(){
		return guid;
	}
	public Set<PFN> getPFNS(){
		return pfns;
	}
	public long getSize(){
		return size;
	}
	public String getMD5(){
		return md5;
	}

	PFN pickPFNforAccess(){
		return null;
	}
	
	private Set<XrootDEnvelope> envelopes = new LinkedHashSet<XrootDEnvelope>();
	
//	public void setLFN(LFN lfn){
//	}
//	public LFN getLFN(){
//		return lfn;
//	}
//	
//	public void setGUID(GUID guid){
//			this.guid = guid;
//	}
//	public GUID getGUID(){
//		return guid;
//	}
//	
//	public void setPFNS(Set<PFN> pfns){
//		this.pfns = pfns;
//	}
//	public Set<PFN>  getPFNS(){
//		return pfns;
//	}
//	
//	void setSize(int size){
//		this.size = size;
//	}
//	int getSize(){
//		return size;
//	}
//	
//	void setMD5(String md5){
//		this.md5 = md5;
//	}
//	String getMD5(){
//		return md5;
//	}
	
	public void addEnvelope(final XrootDEnvelope envelope){
		envelopes.add(envelope);
		
		envelope.setCatalogueAccess(this);
	}
	
	public Set<XrootDEnvelope> getEnvelopes(){
		return envelopes;
	}
}
