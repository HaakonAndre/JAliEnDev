package alien.servlets;

import java.io.BufferedReader;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;


import javax.servlet.http.HttpServletRequest;

import alien.soap.SoapRequestWrapper;
import alien.soap.SoapResponseWrapper;
import alien.user.AliEnPrincipal;
import alien.user.UserFactory;
import lazyj.ExtendedServlet;
import lazyj.Log;

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
		else{
			Log.log(Log.INFO, "Request from user "+user.getName());
		}

		try{
			BufferedReader br = request.getReader();
			String bubu;
			
			while( (bubu = br.readLine()) != null){
				System.err.println(bubu);	
			}
			
					
		}catch (Exception e) {
			e.printStackTrace();
			}

		
		Page pMasterpage = new Page(osOut, "response.res");	

		try {
			SoapRequestWrapper sreqw = new SoapRequestWrapper(request);	
			Log.log(Log.INFO, sreqw.toString());
			
			LinkedHashMap<String, ArrayList<String>> hm = new LinkedHashMap<String, ArrayList<String>>();
			
			ArrayList<String> ar = new ArrayList<String>();
			ar.add("TestJobDisk2Subatech.jdl");
			ar.add("TestJobDisk2Subatech1.jdl");
			ar.add("TestJobDisk2Subatech2.jdl");
			ar.add("TestJobDisk2Subatech3.jdl");
			
			hm.put("rcvalues", ar);
			
			ArrayList<String> ar1 = new ArrayList<String>();
			ar1.add("Log messsseessss 1");
			ar1.add("Log messsseessss 2");
			ar1.add("Log messsseessss 3");
			
			hm.put("rcmessages", ar1);
			
			SoapResponseWrapper srw = new SoapResponseWrapper(sreqw.getActionName(), sreqw.getNamespace(), hm);
			Log.log(Log.INFO, srw.toSOAPXML());
			
			
			pMasterpage.append(srw.toSOAPXML());

		} catch (Exception e) {
			e.printStackTrace();
		}


		pMasterpage.write();


	}
	
	public void execPost(){
		execGet();
	}
}
