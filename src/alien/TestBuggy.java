package alien;

import java.io.File;
import java.io.IOException;

import alien.test.utils.Functions;

/**
 * @author ron
 *
 */
public class TestBuggy {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		try {
			Functions.unzip(new File("testsys/ldap_schema.zip") ,new File("/tmp/"));
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("error unzipping ldap schema");
		}
	}
}
