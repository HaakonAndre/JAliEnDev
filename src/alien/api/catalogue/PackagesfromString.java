package alien.api.catalogue;

import java.util.List;

import alien.api.Request;
import alien.catalogue.Package;
import alien.catalogue.PackageUtils;
import alien.user.AliEnPrincipal;

/**
 * Get the packages 
 * 
 * @author ron
 * @since Nov 23, 2011
 */
public class PackagesfromString extends Request {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = -9135907539296101201L;
	
	
	private final String platform;

	private List<Package> packages;
	
	/**
	 * @param user 
	 * @param role 
	 * @param platform 
	 */
	public PackagesfromString(final AliEnPrincipal user, final String role, final String platform){
		setRequestUser(user);
		setRoleRequest(role);
		this.platform = platform;
	}
	
	@Override
	public void run() {
		this.packages = PackageUtils.getPackages();
	}
	
	/**
	 * @return the requested LFN
	 */
	public List<Package> getPackages(){
		return this.packages;
	}
	
	@Override
	public String toString() {
		return "Asked for : "+this.platform + ", reply is: "+this.packages;
	}
}
