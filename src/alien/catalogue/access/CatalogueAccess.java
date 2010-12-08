package alien.catalogue.access;

import java.io.Serializable;
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
public abstract class CatalogueAccess implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6351566505141657823L;
	
	/**
	 * Invalid access
	 */
	public static final int INVALID = 0;
	
	/**
	 * Read access
	 */
	public static final int READ = 1;
	
	/**
	 * Write access
	 */
	public static final int WRITE = 2;
	
	/**
	 * Delete access
	 */
	public static final int DELETE = 3;
	

	
	/**
	 * Access type
	 */
	protected int access = INVALID;
	
	/**
	 * Catalogue entity that is referred
	 */
	protected CatalogEntity entity;
	
	/**
	 * LFN, inferred from the entity above
	 */
	protected LFN lfn = null;
	
	/**
	 * GUID, inferred from the entity above
	 */
	protected GUID guid = null;
	
	private Set<PFN> pfns;
	
	private long size;
	
	private String md5;
	
	/** 
	 * Package protected access to the constructor.
	 * @param entity 
	 * @see AuthorizationFactory
	 */
	CatalogueAccess(final CatalogEntity entity){
		this.entity = entity;
		
		if(entity instanceof LFN) {
			lfn = (LFN) entity;
			guid = GUIDUtils.getGUID(lfn.guid);
		}
		else
		if (entity instanceof GUID)
		{
			guid = (GUID) entity;
			lfn = LFNUtils.getLFN("/NOLFN", true);
		}
		else
			throw new IllegalAccessError("Unknown entity type");
		
		size = guid.size;
		
		md5 = guid.md5;
	}
	
	/**
	 * Initialize the PFNs
	 */
	public void loadPFNS(){
		pfns = guid.getPFNs();
	}
	
	/**
	 * @return access type, see the constants of this class
	 */
	public int getAccess(){
		return access;
	}
	
	/**
	 * @return the LFN
	 */
	public LFN getLFN(){
		return lfn;
	}
	
	/**
	 * @return the GUID
	 */
	public GUID getGUID(){
		return guid;
	}
	
	/**
	 * @return Associated PFNs
	 */
	public Set<PFN> getPFNS(){
		return pfns;
	}
	
	/**
	 * @return file size, in bytes
	 */
	public long getSize(){
		return size;
	}
	
	/**
	 * @return MD5 checksum
	 */
	public String getMD5(){
		return md5;
	}

	/**
	 * @return one of the PFNs
	 */
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
	
	/**
	 * @param envelope envelope to add
	 */
	public void addEnvelope(final XrootDEnvelope envelope){
		envelopes.add(envelope);
		
		envelope.setCatalogueAccess(this);
	}
	
	/**
	 * @return all envelopes
	 */
	public Set<XrootDEnvelope> getEnvelopes(){
		return envelopes;
	}

	
}
