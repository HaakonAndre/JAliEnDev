package alien;

import java.io.Console;
import java.io.IOException;
import java.security.Security;
import java.util.Arrays;

import org.bouncycastle.openssl.EncryptionException;
import org.bouncycastle.openssl.PasswordFinder;

import alien.communications.APIServer;
import alien.user.AuthenticationChecker;

public class APIService {

	/**
	 * Debugging method
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		
		String pass = "";
			 Console cons;
			 char[] passwd = new char[] {};
			 if((cons = System.console()) == null)
				 System.out.println("console null");
			 
			 if ((cons = System.console()) != null &&
			     (passwd = cons.readPassword("[%s]", "Private key password: ")) != null)
				 	pass = String.valueOf(passwd);
			 
			 Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
			System.out.println("pass is: " + pass);
		PasswordFinder pf = new DefaultPasswordFinder(
				passwd);
		try{
		AuthenticationChecker.loadPrivKey(pf);
		APIServer.startAPIServer();

		}
		catch(EncryptionException e ){
			System.err.println("Invalid password.");
		}
		catch(IOException e){
			System.err.println("Not able to load Grid certificate.");
			e.printStackTrace();
		}
		
	}
	
	private static class DefaultPasswordFinder implements PasswordFinder {

		private final char[] password;

		private DefaultPasswordFinder(char[] password) {
			this.password = password;
		}

		@Override
		public char[] getPassword() {
			return Arrays.copyOf(password, password.length);
		}
	}
}
