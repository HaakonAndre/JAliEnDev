package alien.websockets;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.security.Principal;
import java.util.ArrayList;

import javax.websocket.CloseReason;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.RemoteEndpoint;
import javax.websocket.Session;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import alien.shell.commands.JAliEnCOMMander;
import alien.shell.commands.JSONPrintWriter;
import alien.shell.commands.JShPrintWriter;
import alien.shell.commands.UIPrintWriter;
import alien.shell.commands.XMLPrintWriter;
import alien.user.AliEnPrincipal;

public class JsonWebsocketEndpoint extends Endpoint {
	private AliEnPrincipal userIdentity = null; 
	private JAliEnCOMMander commander = null;
	private UIPrintWriter out = null;
	private OutputStream os = null;
	
	private void setShellPrintWriter(final OutputStream os, final String shelltype) {
		if (shelltype.equals("jaliensh"))
			out = new JShPrintWriter(os);
		else if (shelltype.equals("json"))
			out = new JSONPrintWriter(os);
		else
			out = new XMLPrintWriter(os);
	}

    @Override
    public void onOpen(Session session, EndpointConfig endpointConfig) {
        RemoteEndpoint.Basic remoteEndpointBasic = session.getBasicRemote();
        Principal userPrincipal = session.getUserPrincipal();
        userIdentity = (AliEnPrincipal) userPrincipal;
        
        try {
			os = remoteEndpointBasic.getSendStream();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		setShellPrintWriter(os, "json");
        commander = new JAliEnCOMMander(userIdentity, null, null, null, out);        
        
		// Set metadata
		out.setMetaInfo("user", commander.getUser().getName());
		out.setMetaInfo("role", commander.getRole());
		out.setMetaInfo("currentdir", commander.getCurrentDir().getCanonicalName());
		out.setMetaInfo("site", commander.getSite());
        
        session.addMessageHandler(new EchoMessageHandlerText(remoteEndpointBasic, commander, out));
    }
    
    @Override
	public void onClose(Session session, CloseReason closeReason) {
    	commander.kill = true;
    	
    	out = null;
    	try {
			os.close();
    	} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	os = null;
    	userIdentity = null;
	}
    
    @Override
	public void onError(Session session, Throwable thr) {
    	//
    }

    private static class EchoMessageHandlerText
            implements MessageHandler.Partial<String> {

        private final RemoteEndpoint.Basic remoteEndpointBasic;
       		
		private JAliEnCOMMander commander = null;
		private UIPrintWriter out = null;

        EchoMessageHandlerText(RemoteEndpoint.Basic remoteEndpointBasic, JAliEnCOMMander commander,
        		UIPrintWriter out) {        	
            this.remoteEndpointBasic = remoteEndpointBasic;
            this.commander = commander;
            this.out = out;			
        }
		
		private void waitCommandFinish() {		
			// wait for the previous command to finish
			if (commander == null)
				return;
			while (commander.status.get() == 1)
				try {
					synchronized (commander.status) {
						commander.status.wait(1000);
					}
				} catch (@SuppressWarnings("unused") final InterruptedException ie) {
					// ignore
				}
		}

		@Override
	    public void onMessage(String message, boolean last) {						
	        try {
	            if (remoteEndpointBasic != null) {
					// Try to parse incoming JSON
					Object pobj;
					JSONObject jsonObject;
					JSONParser parser = new JSONParser();
		
					try {
						pobj = parser.parse(new StringReader(message));
						jsonObject = (JSONObject) pobj;
					} catch (@SuppressWarnings("unused") ParseException e) {
		                remoteEndpointBasic.sendText("Incoming JSON not ok", last);
						return;
					}						
					
					// Split JSONObject into strings 
					final ArrayList<String> fullCmd = new ArrayList<>();
					fullCmd.add(jsonObject.get("command").toString());
									
					JSONArray mArray = new JSONArray();
					if (jsonObject.get("options") != null) {
						mArray = (JSONArray) jsonObject.get("options");
					
						for (int i = 0; i < mArray.size(); i++) 
		                    fullCmd.add(mArray.get(i).toString()); 
					}
					
					if (!commander.isAlive())
						commander.start();
					
					// Send the command to executor and send the result back to client via OutputStream
					synchronized (commander) {
						commander.setLine(out, fullCmd.toArray(new String[0]));
						commander.notifyAll();
					}					
					waitCommandFinish();
		        }
	        } catch (IOException e) {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	        } catch (Exception e) {
	        	e.printStackTrace();
	        }
	    }
	}
}
