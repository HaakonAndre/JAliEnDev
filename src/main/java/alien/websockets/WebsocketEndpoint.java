package alien.websockets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.websocket.CloseReason;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.RemoteEndpoint;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpointConfig;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.shell.commands.JAliEnCOMMander;
import alien.shell.commands.JSONPrintWriter;
import alien.shell.commands.JShPrintWriter;
import alien.shell.commands.UIPrintWriter;
import alien.shell.commands.XMLPrintWriter;
import alien.user.AliEnPrincipal;
import lazyj.Utils;

/**
 * @author vyurchen
 *
 *         Implementation of websocket endpoint, that supports plain text and JSON clients
 */
public class WebsocketEndpoint extends Endpoint {
	static transient final Logger logger = ConfigUtils.getLogger(WebsocketEndpoint.class.getCanonicalName());

	static transient final Monitor monitor = MonitorFactory.getMonitor(WebsocketEndpoint.class.getCanonicalName());

	AliEnPrincipal userIdentity = null;

	/**
	 * Commander
	 */
	JAliEnCOMMander commander = null;

	private UIPrintWriter out = null;
	private OutputStream os = null;

	private void setShellPrintWriter(final OutputStream os, final String shelltype) {
		if (shelltype.equals("plain"))
			out = new JShPrintWriter(os);
		else if (shelltype.equals("json"))
			out = new JSONPrintWriter(os);
		else
			out = new XMLPrintWriter(os);
	}

	static final DelayQueue<SessionContext> sessionQueue = new DelayQueue<>();

	private static final class SessionContext implements Delayed {
		final Session session;
		final WebsocketEndpoint endpoint;

		final long startTime = System.currentTimeMillis();
		long lastActivityTime = System.currentTimeMillis();

		final long absoluteRunningDeadline;

		public SessionContext(final WebsocketEndpoint endpoint, final Session session, final long userCertExpiring) {
			this.endpoint = endpoint;
			this.session = session;

			absoluteRunningDeadline = Math.min(startTime + 2 * 24 * 60 * 60 * 1000L, userCertExpiring);
		}

		@Override
		public int compareTo(final Delayed other) {
			final long delta = getRunningDeadline() - ((SessionContext) other).getRunningDeadline();

			if (delta < 0)
				return -1;

			if (delta > 0)
				return 1;

			return 0;
		}

		final long getRunningDeadline() {
			return Math.min(absoluteRunningDeadline, lastActivityTime + 3 * 60 * 60 * 1000L);
		}

		@Override
		public long getDelay(TimeUnit unit) {
			final long delay = getRunningDeadline() - System.currentTimeMillis();

			return unit.convert(delay, TimeUnit.MILLISECONDS);
		}

		public void touch() {
			this.lastActivityTime = System.currentTimeMillis();
		}
	}

	static final Thread sessionCheckingThread = new Thread() {
		@Override
		public void run() {
			while (true) {
				try {
					SessionContext context = sessionQueue.take();

					if (context != null) {
						if (context.getRunningDeadline() <= System.currentTimeMillis()) {
							logger.log(Level.FINE, "Closing one idle / too long running session");
							context.endpoint.onClose(context.session, new CloseReason(null, "Session timed out"));

							monitor.incrementCounter("timedout_sessions");
						}
						else {
							logger.log(Level.SEVERE, "Session should still be kept in fact, deadline = " + context.getRunningDeadline() + " while now = " + System.currentTimeMillis());
							sessionQueue.add(context);
						}
					}
				}
				catch (@SuppressWarnings("unused") InterruptedException e) {
					// was told to exit
					return;
				}
			}
		}
	};

	static {
		disableAccessWarnings();
		
		sessionCheckingThread.setName("JsonWebsocketEndpoint.timeoutChecker");
		sessionCheckingThread.setDaemon(true);
		sessionCheckingThread.start();

		monitor.addMonitoring("sessions", (names, values) -> {
			names.add("active_sessions");
			values.add(Double.valueOf(sessionQueue.size()));
		});
	}

	/**
	 * Object to send notifications about the state of connection
	 */
	final Object stateObject = new Object();

	@Override
	public void onOpen(final Session session, final EndpointConfig endpointConfig) {
		final Principal userPrincipal = session.getUserPrincipal();
		userIdentity = (AliEnPrincipal) userPrincipal;

		os = new ByteArrayOutputStream();
		final ServerEndpointConfig serverConfig = (ServerEndpointConfig) endpointConfig;
		if (serverConfig.getPath() == "/websocket/json")
			setShellPrintWriter(os, "json");
		else
			setShellPrintWriter(os, "plain");

		commander = new JAliEnCOMMander(userIdentity, null, getSite(getRemoteIP(session)), out);
		commander.start();

		final SessionContext context = new SessionContext(this, session, commander.getUser().getUserCert()[0].getNotAfter().getTime());

		session.addMessageHandler(new EchoMessageHandlerText(context, commander, out, os));

		sessionQueue.add(context);

		monitor.incrementCounter("new_sessions");
	}

	/**
	 * Get the client's closest site
	 * 
	 * @param ip IP address of the client
	 * @return the name of the closest site
	 */
	private static String getSite(final String ip) {
		if (ip == null) {
			logger.log(Level.SEVERE, "Client IP address is unknown");
			return null;
		}

		try {
			final String site = Utils.download("http://alimonitor.cern.ch/services/getClosestSite.jsp?ip=" + ip, null);

			if (logger.isLoggable(Level.FINE))
				logger.log(Level.FINE, "Client IP address " + ip + " mapped to " + site);

			if (site != null)
				return site.trim();
		}
		catch (IOException ioe) {
			logger.log(Level.SEVERE, "Cannot get the closest site information for " + ip, ioe);
		}

		return null;
	}

	/**
	 * Get the IP address of the client using reflection of the socket object
	 * 
	 * @param session websocket session which contains the socket
	 * @return IP address
	 */
	private static String getRemoteIP(final Session session) {
		try {
			Object obj = session.getAsyncRemote();

			for (final String fieldName : new String[] { "base", "socketWrapper", "socket", "sc", "remoteAddress" }) {
				obj = getField(obj, fieldName);

				if (obj == null)
					return null;
			}

			return ((InetSocketAddress) obj).getAddress().getHostAddress();
		}
		catch (final Throwable t) {
			logger.log(Level.SEVERE, "Cannot extract the remote IP address from a session", t);
		}

		return null;
	}

	private static Object getField(Object obj, String fieldName) {
		Class<?> objClass = obj.getClass();

		for (; objClass != Object.class; objClass = objClass.getSuperclass()) {
			try {
				Field field;
				field = objClass.getDeclaredField(fieldName);
				field.setAccessible(true);
				return field.get(obj);
			}
			catch (@SuppressWarnings("unused") Exception e) {
				// ignore
			}
		}

		return null;
	}

	/**
	 * Disable the reflection access warning produced by getRemoteIP(Session) accessing field <code>sun.nio.ch.SocketChannelImpl.remoteAddress</code>
	 */
	private static void disableAccessWarnings() {
		try {
			Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
			Field field = unsafeClass.getDeclaredField("theUnsafe");
			field.setAccessible(true);
			Object unsafe = field.get(null);

			Method putObjectVolatile = unsafeClass.getDeclaredMethod("putObjectVolatile", Object.class, long.class, Object.class);
			Method staticFieldOffset = unsafeClass.getDeclaredMethod("staticFieldOffset", Field.class);

			Class<?> loggerClass = Class.forName("jdk.internal.module.IllegalAccessLogger");
			Field loggerField = loggerClass.getDeclaredField("logger");
			Long offset = (Long) staticFieldOffset.invoke(unsafe, loggerField);
			putObjectVolatile.invoke(unsafe, loggerClass, offset, null);
		}
		catch (Exception e) {
			logger.log(Level.FINE, "Could not disable warnings regarding access to sun.nio.ch.SocketChannelImpl.remoteAddress", e);
		}
	}

	@Override
	public void onClose(final Session session, final CloseReason closeReason) {
		monitor.incrementCounter("closed_sessions");

		logger.log(Level.INFO, "Closing session of commander ID "+commander.commanderId);
		
		commander.kill = true;
		commander.setLine(null, null);
		commander.interrupt();

		out = null;
		try {
			if (os != null)
				os.close();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		os = null;
		userIdentity = null;

		try {
			if (session != null) {
				final Iterator<SessionContext> it = sessionQueue.iterator();

				while (it.hasNext()) {
					final SessionContext sc = it.next();

					if (sc.session.equals(session))
						it.remove();
				}

				session.close();
			}
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		synchronized (stateObject) {
			stateObject.notifyAll();
		}
	}

	@Override
	public void onError(final Session session, final Throwable thr) {
		//
	}

	private static class EchoMessageHandlerText implements MessageHandler.Partial<String> {

		private final RemoteEndpoint.Basic remoteEndpointBasic;

		private JAliEnCOMMander commander = null;
		private UIPrintWriter out = null;
		private OutputStream os = null;

		private final SessionContext context;

		EchoMessageHandlerText(final SessionContext context, final JAliEnCOMMander commander, final UIPrintWriter out, OutputStream os) {
			this.context = context;
			this.remoteEndpointBasic = context.session.getBasicRemote();
			this.commander = commander;
			this.out = out;
			this.os = os;
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
				}
				catch (@SuppressWarnings("unused") final InterruptedException ie) {
					// ignore
				}
		}

		@Override
		public void onMessage(final String message, final boolean last) {
			monitor.incrementCounter("commands");

			try {
				if (remoteEndpointBasic != null) {
					final ArrayList<String> fullCmd = new ArrayList<>();

					if (this.out.getClass().getCanonicalName() == "alien.shell.commands.JSONPrintWriter") {
						// Try to parse incoming JSON
						Object pobj;
						JSONObject jsonObject;
						JSONParser parser = new JSONParser();

						try {
							pobj = parser.parse(new StringReader(message));
							jsonObject = (JSONObject) pobj;
						}
						catch (@SuppressWarnings("unused") ParseException e) {
							synchronized (remoteEndpointBasic) {
								remoteEndpointBasic.sendText("{\"metadata\":{\"exitcode\":\"-1\",\"error\":\"Incoming JSON not ok\"},\"results\":[]}", true);
							}
							return;
						}

						// Filter out cp commands
						if (jsonObject.get("command").toString().equals("cp")) {
							synchronized (remoteEndpointBasic) {
								remoteEndpointBasic.sendText(
										"{\"metadata\":{\"exitcode\":\"-1\",\"error\":\"'cp' grid command is not implemented. Please use native client's Cp() method\"},\"results\":[]}", true);
							}
							return;
						}

						// Split JSONObject into strings
						fullCmd.add(jsonObject.get("command").toString());

						JSONArray mArray = new JSONArray();
						if (jsonObject.get("options") != null) {
							mArray = (JSONArray) jsonObject.get("options");

							for (int i = 0; i < mArray.size(); i++)
								fullCmd.add(mArray.get(i).toString());
						}
					}
					else if (this.out.getClass().getCanonicalName() == "alien.shell.commands.JShPrintWriter") {
						// Split incoming message into tokens
						final StringTokenizer st = new StringTokenizer(message, " ");

						while (st.hasMoreTokens())
							fullCmd.add(st.nextToken());

						// Filter out cp commands
						if (fullCmd.get(0).equals("cp")) {
							synchronized (remoteEndpointBasic) {
								remoteEndpointBasic.sendText("'cp' grid command is not implemented. Please use native client's Cp() method", true);
							}
							return;
						}
					}
					else {
						// this is XMLPrintWriter or some other type of writer
						logger.log(Level.SEVERE, "Tried to use unsopported writer " + this.out.getClass().getCanonicalName() + " in the websocket endpoint");
						return;
					}

					if (!commander.isAlive()) {
						commander.kill = true;

						final JAliEnCOMMander comm = new JAliEnCOMMander(commander.getUser(), commander.getCurrentDir(), commander.getSite(), out);
						commander = comm;

						commander.start();
					}

					// Send the command to executor and send the result back to
					// client via OutputStream
					synchronized (commander) {
						commander.status.set(1);
						commander.setLine(out, fullCmd.toArray(new String[0]));
						commander.notifyAll();
					}
					waitCommandFinish();

					// Send back the result to the client
					synchronized (remoteEndpointBasic) {
						final ByteArrayOutputStream baos = (ByteArrayOutputStream) os;
						remoteEndpointBasic.sendText(baos.toString(), true);
						baos.reset();
					}

					context.touch();
				}
			}
			catch (final IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch (final Exception e) {
				e.printStackTrace();
			}
		}
	}
}
