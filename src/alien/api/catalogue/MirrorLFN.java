package alien.api.catalogue;

import alien.api.Request;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;

import java.util.HashMap;
import java.util.List;

public class MirrorLFN extends Request {
	private static final long serialVersionUID = -3323349253430911576L;
	private String path;	
	private int success;
	private List<String> ses;
	private List<String> exses;
	HashMap<String, Integer> qos;
	private String dstSE;
	private boolean useGUID;
	private Integer attempts;
	HashMap<String,Integer> results;
	
	public MirrorLFN( final AliEnPrincipal user, final String role, 
							String lfn_name,
							List<String> ses,
							List<String> exses,
							HashMap<String, Integer> qos,
							boolean useLFNasGuid,
							Integer attempts_cnt ){
		this.path = lfn_name;
		this.useGUID = useLFNasGuid;
		this.attempts = attempts_cnt;
		this.ses = ses;
		this.exses = exses;
		this.qos = qos;
		this.results = new HashMap<String,Integer>();
	}
	
	public MirrorLFN( final AliEnPrincipal user, final String role, 
			String lfn_name,
			String destSE,
			boolean useLFNasGuid,			
			Integer attempts_cnt ){
		this.path = lfn_name;
		this.useGUID = useLFNasGuid;
		this.dstSE = destSE;
		this.attempts = attempts_cnt;
	}
	
	@Override
	public void run() {
		if( this.dstSE!=null ){
			this.success = LFNUtils.mirrorLFN( this.path, 
												this.dstSE, 
												this.useGUID, 
												this.attempts );
			this.results.put( dstSE, this.success );
		}
		else
			this.results = LFNUtils.mirrorLFN( this.path,
												this.ses,
												this.exses,
												this.qos,
												this.useGUID, 
												this.attempts );
	}
	
	public boolean getSuccess(){
		return this.success >= 0;
	}
		
	public int getResult(){
		return this.success;
	}
	
	public HashMap<String,Integer> getResultHashMap(){
		return this.results;
	}
}
