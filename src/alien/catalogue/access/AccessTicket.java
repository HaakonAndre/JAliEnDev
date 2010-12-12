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
import alien.se.SE;

/**
 * Generic envelope
 * 
 * @author costing
 */
public class AccessTicket implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6200985047035616911L;

	/**
	 * Access type
	 */
	public final AccessType type;
	
	/**
	 * @param type
	 */
	public AccessTicket(final AccessType type,final CatalogEntity entity){
		this.type = type;
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
	 * Catalogue entity that is referred
	 */
	private CatalogEntity entity;
	
	/**
	 * LFN, inferred from the entity above
	 */
	private LFN lfn = null;
	
	/**
	 * GUID, inferred from the entity above
	 */
	private GUID guid = null;
	
	private Set<PFN> pfns = null;
	
	private long size;
	
	private String md5;

	
	/**
	 * Initialize the PFNs
	 */
	public void loadPFNS(){
		pfns = guid.getPFNs();
	}
	
	/**
	 * @return access type, see the constants of this class
	 */
	public AccessType getAccessType(){
		return type;
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
	
	public void addPFN(PFN pfn, SE se){
		pfns.add(pfn);
		if((se != null) && (se.seioDaemons.startsWith("xrootd")))
			envelopes.add(new XrootDEnvelope(this,pfn,se));	
	}
	
	private Set<XrootDEnvelope> envelopes = new LinkedHashSet<XrootDEnvelope>();

	
	/**
	 * @return all envelopes
	 */
	public Set<XrootDEnvelope> getEnvelopes(){
		return envelopes;
	}

	
	
}
