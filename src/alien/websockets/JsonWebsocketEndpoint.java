package alien.websockets;

import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;

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
import alien.shell.commands.UIPrintWriter;

public class JsonWebsocketEndpoint extends Endpoint {

    @Override
    public void onOpen(Session session, EndpointConfig endpointConfig) {
        RemoteEndpoint.Basic remoteEndpointBasic = session.getBasicRemote();
        session.addMessageHandler(new EchoMessageHandlerText(remoteEndpointBasic));
        session.addMessageHandler(new EchoMessageHandlerBinary(remoteEndpointBasic));
    }

    private static class EchoMessageHandlerText
            implements MessageHandler.Partial<String> {

        private final RemoteEndpoint.Basic remoteEndpointBasic;

        private EchoMessageHandlerText(RemoteEndpoint.Basic remoteEndpointBasic) {
            this.remoteEndpointBasic = remoteEndpointBasic;
        }

		static void notifyActivity() {
			//lastOperation = System.currentTimeMillis();
		}
		private JAliEnCOMMander commander = null;
		private UIPrintWriter out = null;
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
					// try to parse incoming JSON
					Object pobj;
					JSONObject jsonObject;
					JSONParser parser = new JSONParser();
		
					try {
						pobj = parser.parse(new StringReader(message));
						jsonObject = (JSONObject) pobj;
					} catch (ParseException e) {
		                remoteEndpointBasic.sendText("Incoming JSON not ok", last);
						return;
					}
					/*System.out.println("Calling AuthorizationFactory");
					//AliEnPrincipal user = AuthorizationFactory.getDefaultUser();
					System.out.println("Calling AuthorizationFactory success");
					if (commander == null) {
						System.out.println("Calling Commander");
						try {
							commander = new JAliEnCOMMander();
						}
						catch (Exception e)
						{
							System.out.println("JAliEnCOMMander creation failed: ");
							e.printStackTrace();
						}
						//commander.start();
					}
					String[] tosend = new String[1];
					tosend[0] = (String) jsonObject.get("command");
					/*synchronized (commander) {
						commander.setLine(null, tosend);
						commander.notifyAll();
					}*/
		
					String name2 = (String) jsonObject.get("name");
					if (name2 == null)
						name2 = "NOTSPECIFIED";
					
					// build JSON
					JSONObject obj = new JSONObject();
					obj.put("name", "mkyong.com");
					obj.put("name2", name2);
					obj.put("age", new Integer(100));
		
					JSONArray list = new JSONArray();
					list.add("msg 1");
					list.add("msg 2");
					list.add("msg 3");
		
					obj.put("messages", list);
					String response = obj.toJSONString();
		
		            //remoteEndpointBasic.sendText(response, last);
		            remoteEndpointBasic.sendText(obj.toJSONString(), last);
		        }
	        } catch (IOException e) {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	        } catch (Exception e) {
	        	e.printStackTrace();
	        }
	    }
	}

    private static class EchoMessageHandlerBinary
            implements MessageHandler.Partial<ByteBuffer> {

        private final RemoteEndpoint.Basic remoteEndpointBasic;

        private EchoMessageHandlerBinary(RemoteEndpoint.Basic remoteEndpointBasic) {
            this.remoteEndpointBasic = remoteEndpointBasic;
        }

        @Override
        public void onMessage(ByteBuffer message, boolean last) {
            try {
                if (remoteEndpointBasic != null) {
                    remoteEndpointBasic.sendBinary(message, last);
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
