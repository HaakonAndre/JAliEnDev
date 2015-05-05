package alien.api.catalogue;

import alien.api.Request;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;
import java.util.List;

public class MirrorLFN extends Request {
	private String path;	
	private int success;
	private String ses;	
	
	public MirrorLFN( final AliEnPrincipal user, final String role, 
							String lfn_name,
							List<String> ses,
							List<String> exses,
							boolean keepSamePath,
							boolean useLFNasGuid,
							boolean checkFileIsPresentOnDest,
							boolean transferWholeArchive,
							boolean waitUntilTransferFinished,
							Integer masterTransferId,
							Integer attempts ){
		this.path = lfn_name;
	}
	
	public MirrorLFN( final AliEnPrincipal user, final String role, 
			String lfn_name,
			String dstSE,
			boolean keepSamePath,
			boolean useLFNasGuid,
			boolean checkFileIsPresentOnDest,
			boolean transferWholeArchive,
			Integer masterTransferId,
			Integer attempts ){
		this.path = lfn_name;		
	}
	
	@Override
	public void run() {
		this.success = LFNUtils.mirrorLFN( this.path );
	}
	
	public boolean getSuccess(){
		return this.success >= 0;
	}
	
}
