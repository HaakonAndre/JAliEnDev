package alien.catalogue.access;

import java.util.UUID;

import alien.catalogue.*;

 class AuthorizationFactory {

	static CatalogueAccess requestAccess(LFN lfn, String access) {
		return requestAccess(lfn.guid,access);
	}

	static CatalogueAccess requestAccess(UUID guid, String access){
		
		if(authorized(guid,access)){
			CatalogueAccess ca =  accessType(guid, access);
			ca.decorate(); // decorate the access with all catalogue info
			PFN pfn = ca.pickPFNforAccess(); // however we do that
			ca.addEnvelope(new XrootDEnvelope(ca, pfn));
		}
		return null;
	}
	
	static boolean authorized(UUID guid, String access){
		return false;
	}
	
	static CatalogueAccess accessType(UUID guid,String access){
		
		if(access.equals("delete")){
			return (CatalogueAccess) new CatalogueDeleteAccess(guid);
		}
		return null;
	}

}