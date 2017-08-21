package alien.websockets;

import java.util.HashSet;
import java.util.Set;

import javax.websocket.Endpoint;
import javax.websocket.server.ServerApplicationConfig;
import javax.websocket.server.ServerEndpointConfig;

public class EndpointConfig implements ServerApplicationConfig {

	@Override
	public Set<ServerEndpointConfig> getEndpointConfigs(Set<Class<? extends Endpoint>> scanned) {

		Set<ServerEndpointConfig> result = new HashSet<>();

		if (scanned.contains(JsonWebsocketEndpoint.class)) {
			result.add(ServerEndpointConfig.Builder.create(JsonWebsocketEndpoint.class, "/websocket/json").build());
		}

		return result;
	}

	@Override
	public Set<Class<?>> getAnnotatedEndpointClasses(Set<Class<?>> scanned) {
		// Deploy all WebSocket endpoints defined by annotations in the examples
		// web application. Filter out all others to avoid issues when running
		// tests on Gump
		Set<Class<?>> results = new HashSet<>();
		for (Class<?> clazz : scanned) {
			if (clazz.getPackage().getName().startsWith("websocket.")) {
				results.add(clazz);
			}
		}
		return results;
	}
}
