package alien.servlets;

import java.security.cert.X509Certificate;

import javax.servlet.http.HttpServletRequest;

import alien.user.AliEnPrincipal;
import alien.user.UserFactory;
import lazyj.ExtendedServlet;

/**
 * @author costing
 */
public class AuthenServlet extends ExtendedServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * @param request
	 * @return authenticated user, if any
	 */
	public static AliEnPrincipal getPrincipal(final HttpServletRequest request){
		if (!request.isSecure())
			return null;
		
		X509Certificate cert[] = (X509Certificate[]) request.getAttribute("javax.servlet.request.X509Certificate");
		
		if (cert==null || cert.length==0)
			return null;
		
		return UserFactory.getByCertificate(cert);
	}
	
	@Override
	public void execGet() {
		final AliEnPrincipal user = getPrincipal(request);
		
		if (user==null){
			pwOut.println("You are not allowed here");
			pwOut.flush();
			return;
		}
		
		pwOut.println("You are : "+user);
		pwOut.flush();
	}

	
	
}
