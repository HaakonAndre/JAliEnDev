package alien;

import java.security.KeyStoreException;


import joptsimple.OptionParser;
import joptsimple.OptionSet;

import alien.api.APIServer;

/**
 * @author ron
 * @since Jun 21, 2011
 */
public class JBox {

	/**
	 * Debugging method
	 * 
	 * @param args
	 * @throws KeyStoreException 
	 */
	public static void main(String[] args) throws KeyStoreException {
		final OptionParser parser = new OptionParser();
		parser.accepts("login");
		parser.accepts( "debug" ).withRequiredArg().ofType( Integer.class );

		int iDebug = 0;

		try{
			OptionSet options = parser.parse(args);

			if(options.has("debug")){
				//iDebug = Integer.parseInt((String) options.valueOf("debug"));
				iDebug = (Integer) options.valueOf("debug");
			}

		} catch (Exception e) {
			//nothing, we just let it 0, nothing to debug
			e.printStackTrace();
		}


		APIServer.startAPIService(iDebug);


	}
}