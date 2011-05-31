package alien.servlets;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import javax.servlet.http.HttpServletRequest;

import lazyj.ExtendedServlet;
import lazyj.Log;
import alien.commands.AlienCommand;
import alien.commands.AlienCommandls;
import alien.commands.AlienCommands;
import alien.config.Context;
import alien.config.SOAPLogger;
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

		if (user == null){
			Log.log(Log.ERROR, "You are not alloed here");
			pwOut.println("You are not allowed here");
			pwOut.flush();
			return;
		}

		Log.log(Log.INFO, "Request from user "+user.getName());

	/*		try{
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
			Log.log(Log.INFO, sreqw.toString());

			if(!"ping".equals(sreqw.getActionName())){
				Log.log(Log.INFO, "Inainte de cmd");
				// TODO - decode the debug level and give it to SOAPLogger below
				final SOAPLogger logger = new SOAPLogger(0);
				Context.setThreadContext("logger", logger);				
				
				AlienCommand cmd = AlienCommands.getAlienCommand(user, sreqw.getActionArguments());
				Log.log(Log.INFO, "Dupa cmd");
				
				Object objResponse;
				
				//command not implemented
				if(cmd == null){
					Log.log(Log.ERROR, "We got a hit for a command that it is not implemented = "+AlienCommands.getAlienCommandString(user, sreqw.getActionArguments()));
					objResponse = "Command not implemented!";
				}	
				else{
					Log.log(Log.INFO, "Cmd = "+cmd.toString());
					objResponse = cmd.executeCommand();
				}

				// TODO - give the log messages back to the client
				final String logMessages = logger.getLog();
				
				SoapResponseWrapper srw = new SoapResponseWrapper(sreqw.getActionName(), sreqw.getNamespace(), objResponse);
				Log.log(Log.INFO, srw.toSOAPXML());

				pMasterpage.append(srw.toSOAPXML());
			}

		} catch (Throwable e) {
			Log.log(Log.ERROR, "Eroaaaree "+e.getStackTrace());
			e.printStackTrace();
		}
		finally{
			Context.setThreadContext("logger", null);
		}

		Log.log(Log.ERROR, "Chiar scriem ceva");
		pMasterpage.write();


	}

	@Override
	public void execPost(){
		execGet();
	}
}
