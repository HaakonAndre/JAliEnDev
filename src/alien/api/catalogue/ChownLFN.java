package alien.api.catalogue;

import alien.api.Request;
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
	
	
	
	public ChownLFN( final AliEnPrincipal user, final String role, 
								final String fpath, 
								final String chuser,
								final String chgroup ){
		this.path = fpath;
		this.chown_user = chuser;
		this.chown_group = chgroup;
	}
	
	@Override
	public void run() {
		if( !AliEnPrincipal.roleIsAdmin( getEffectiveRequester().getName() ) )
			throw new SecurityException( "Only administrators can do it" );
		this.success = LFNUtils.chownLFN( this.path, this.chown_user, this.chown_group );
	}
	
	public boolean getSuccess(){
		return this.success;
	}
	
}
