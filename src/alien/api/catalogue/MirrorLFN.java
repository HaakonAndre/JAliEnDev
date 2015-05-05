package alien.api.catalogue;

import alien.api.Request;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;

public class MirrorLFN extends Request {
	private String path;
	private String chown_user;
	private String chown_group;
	private boolean success ;
	
	public MirrorLFN( final AliEnPrincipal user, final String role, 
							String lfn_name, 
							boolean keepSamePath,
							boolean useLFNasGuid,
							boolean checkFileIsPresentOnDest,
							boolean transferWholeArchive,
							boolean waitUntilTransferFinished,
							String collection,
							Integer masterTransferId,
							Integer attempts ){
		this.path = lfn_name;
		//this.chown_user = chuser;
		//this.chown_group = chgroup;
	}
	
	@Override
	public void run() {
		this.success = LFNUtils.mirrorLFN( this.path );
	}
	
	public boolean getSuccess(){
		return this.success;
	}
	
}
