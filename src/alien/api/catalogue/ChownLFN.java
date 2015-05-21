package alien.api.catalogue;

import java.util.Collection;
import java.util.HashMap;
import java.util.UUID;

import alien.api.Request;
import alien.catalogue.CatalogEntity;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;

public class ChownLFN extends Request {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4209526023185462132L;
	private String path;
	private String chown_user;
	private String chown_group;
	private boolean success ;
	private boolean recursive;
	private HashMap<String, Boolean> results;
	
	
	public ChownLFN( final AliEnPrincipal user, final String role, 
								final String fpath, 
								final String chuser,
								final String chgroup,
								final boolean recursive ){
		this.path = fpath;
		this.chown_user = chuser;
		this.chown_group = chgroup;
		this.recursive = recursive;
	}
	
	@Override
	public void run() {
		//if( !AliEnPrincipal.roleIsAdmin( getEffectiveRequester().getName() ) )
		//	throw new SecurityException( "Only administrators can do it" );
		
		this.results = new HashMap<String, Boolean>(); 
		
		if( !this.recursive ){
			CatalogEntity c = LFNUtils.getLFN(this.path);
			if( !AuthorizationChecker.isOwner( c, getEffectiveRequester() ) )
				throw new SecurityException("You do not own this file: " + c +
						", requester: " + getEffectiveRequester() );
			this.success = LFNUtils.chownLFN( this.path, 
												this.chown_user, 
												this.chown_group );
			results.put( this.path, this.success );
			return;
		}
		
		Collection<LFN> lfns = LFNUtils.find(this.path, "*", 0);
		for( LFN l : lfns ){
			if( !AuthorizationChecker.isOwner( l, getEffectiveRequester() ) )
				throw new SecurityException("You do not own this file: " + l +
						", requester: " + getEffectiveRequester() );
			this.success = LFNUtils.chownLFN( l.getCanonicalName(), 
												this.chown_user, 
												this.chown_group );
			results.put( this.path, this.success );
		}
	}
	
	public boolean getSuccess(){
		return this.success;
	}
	
	public HashMap<String, Boolean> getResults(){
		return this.results;
	}
	
}
