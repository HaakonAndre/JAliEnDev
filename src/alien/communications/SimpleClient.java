package alien.communications;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;

import alien.ui.Request;

/**
 * @author costing
 *
 */
public class SimpleClient extends Thread {

	private final Socket connection;
	
	private final ObjectInputStream ois;
	private final ObjectOutputStream oos;
	
	private final OutputStream os;
	
	protected SimpleClient(final Socket connection) throws IOException{
		this.connection = connection;
		
		connection.setTcpNoDelay(true);
		connection.setTrafficClass(0x10);
		
		this.ois = new ObjectInputStream(connection.getInputStream());
	
		this.os = connection.getOutputStream();
		
		this.oos = new ObjectOutputStream(this.os);
		this.oos.flush();
	}
	
	@Override
	public String toString() {
		return this.connection.getInetAddress().toString();
	}
	
	@Override
	public void run() {
		// check
	}
	
	private static SimpleClient instance = null;
	
	private static String address = null;
	private static int port = 0;
	
	
	
	/**
	 * @param addr
	 * @param p
	 * @return instance
	 * @throws IOException
	 */
	public static SimpleClient getInstance(String addr, int p) throws IOException {
		if (instance == null){
			// connect to the other end
			
			address=addr;
			port = p;
			
			Socket s = new Socket(address, port);
			
			instance = new SimpleClient(s);
		}
		
		return instance;
	}
	
	private void close(){
		if (ois!=null){
			try{
				ois.close();
			}
			catch (IOException ioe){
				// ignore
			}
		}
		
		if (oos!=null){
			try{
				oos.close();
			}
			catch (IOException ioe){
				// ignore
			}
		}
		
		if (connection!=null){
			try{
				connection.close();
			}
			catch (IOException ioe){
				// ignore
			}
		}
		
		instance = null;
	}
	
	/**
	 * Total amount of time (in milliseconds) spent in writing objects to the socket.
	 */
	public static long lSerialization = 0;
	
	/**
	 * @param r
	 * @return the processed request, if successful
	 * @throws IOException in case of connectivity problems
	 */
	public static synchronized Request dispatchRequest(final Request r) throws IOException {
		final SimpleClient c = getInstance(address,port);
		
		if (c==null)
			throw new IOException("Connection is null");
		
		try{
			long lStart = System.currentTimeMillis();
			
			c.oos.writeObject(r);
			//c.oos.reset();		
			c.oos.flush();
			c.os.flush();
			
			lSerialization += System.currentTimeMillis() - lStart;
			
			Object o;
			try {
				o = c.ois.readObject();
			}
			catch (ClassNotFoundException e) {
				throw new IOException(e.getMessage());
			}
			
			return (Request) o;
		}
		catch (IOException ioe){
			c.close();
			
			throw ioe;
		}
	}
	
}
