/**
 * 
 */
package alien.websockets;

import javax.servlet.ServletContextEvent;
import javax.websocket.DeploymentException;
import javax.websocket.server.ServerContainer;
import javax.websocket.server.ServerEndpointConfig;

import org.apache.tomcat.websocket.server.Constants;
import org.apache.tomcat.websocket.server.WsContextListener;

/**
 * @author vyurchen
 *
 *         Websocket listener must be added manually to the Tomcat context to bootstrap the WsServerContainer correctly
 */
public class WebsocketListener extends WsContextListener {

	@Override
	public void contextInitialized(ServletContextEvent sce) {
		super.contextInitialized(sce);

		final ServerContainer sc = (ServerContainer) sce.getServletContext().getAttribute(Constants.SERVER_CONTAINER_SERVLET_CONTEXT_ATTRIBUTE);

		try {
			sc.addEndpoint(ServerEndpointConfig.Builder.create(WebsocketEndpoint.class, "/websocket/json").build());
			sc.addEndpoint(ServerEndpointConfig.Builder.create(WebsocketEndpoint.class, "/websocket/plain").build());
		}
		catch (DeploymentException e) {
			throw new RuntimeException(e);
		}
	}
}