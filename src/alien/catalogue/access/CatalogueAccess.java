package alien.catalogue.access;

import java.util.LinkedHashSet;
import java.util.Set;

import alien.catalogue.*;
import alien.se.*;
import java.util.Set;


public class CatalogueAccess  {

	protected String access;
	LFN lfn = null;
	GUID guid = null;
	Set<PFN> pfns = new LinkedHashSet<PFN>();
	int size = 0;
	String md5 = "";

	void decorate(){
		
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
	
	void addEnvelope(XrootDEnvelope envelope){
		envelopes.add(envelope);
		envelope.setCatalogueAccess(this);
	}
	Set<XrootDEnvelope> getEnvelopes(){
		return envelopes;
	}
}
