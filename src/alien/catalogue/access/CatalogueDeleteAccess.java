package alien.catalogue.access;

import java.util.UUID;

public class CatalogueDeleteAccess extends CatalogueAccess{
	
	UUID guid;
	
	public CatalogueDeleteAccess(UUID guid){
		super.access = "delete";
		this.guid = guid;
	}

	
	void decorate(){
		// guid.getALlINFO;
	}
}
