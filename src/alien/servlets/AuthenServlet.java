package alien.servlets;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import javax.servlet.http.HttpServletRequest;

import lazyj.ExtendedServlet;
import lazyj.Log;
import alien.commands.AlienCommandls;
import alien.soap.SoapRequestWrapper;
import alien.soap.SoapResponseWrapper;
import alien.user.AliEnPrincipal;
import alien.user.UserFactory;

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
		
		Log.log(Log.INFO, "Request from user "+user.getName());

	/*	try{
			BufferedReader br = request.getReader();
			String bubu;
			
			while( (bubu = br.readLine()) != null){
				System.err.println(bubu);	
			}
			
					
		}catch (Exception e) {
			e.printStackTrace();
			}

		*/
		Page pMasterpage = new Page(osOut, "response.res");	

		try {
			SoapRequestWrapper sreqw = new SoapRequestWrapper(request);	
			Log.log(Log.FINE, sreqw.toString());
			
			AlienCommandls ls = new AlienCommandls(sreqw.getActionArguments());
			
			SoapResponseWrapper srw = new SoapResponseWrapper(sreqw.getActionName(), sreqw.getNamespace(), ls.executeCommand());
			Log.log(Log.FINE, srw.toSOAPXML());
			
			
			pMasterpage.append(srw.toSOAPXML());

		} catch (Exception e) {
			e.printStackTrace();
		}


		pMasterpage.write();


	}
	
	@Override
	public void execPost(){
		execGet();
	}
}
