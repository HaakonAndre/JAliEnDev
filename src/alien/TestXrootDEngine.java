package alien;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;
import java.util.logging.Logger;

import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessTicket;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.io.protocols.Xrootd;
import alien.services.XrootDEnvelopeSigner;
import alien.user.AliEnPrincipal;
import alien.user.UserFactory;

public class TestXrootDEngine {
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(Testing.class
			.getCanonicalName());

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		AliEnPrincipal user = UserFactory.getByUsername("ali");

		System.out.println("my user is: " + user.toString());
		if (user.hasRole("admin"))
			System.out.println("I got root! ");

		String lfnOrGUID = "/pcalice46/user/a/ali/24-createorgportal.t";

		// lfnOrGUID = "dcf4fbf0-0636-11e0-aa7a-00235a36bb1b";

		String access = "read";
		File myCopy = new File("/tmp/javateststuff");

		AccessTicket ticket = AuthorizationFactory.requestAccess(user,
				lfnOrGUID, access);

		ticket.loadPFNS();

		Set<PFN> pfns = ticket.getPFNS();

		System.out.println("PFN1: " + pfns.iterator().next().toString());

		XrootDEnvelopeSigner signEngine = new XrootDEnvelopeSigner();

		signEngine.signEnvelopesForAccess(ticket);

		PFN pfn = pfns.iterator().next();

		Xrootd xrd = new Xrootd();
		try {
			xrd.get(pfn, myCopy);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String content = "";
		if (myCopy.exists()) {
			try {
				FileInputStream fin = new FileInputStream(
						myCopy.getCanonicalPath());
				content = new DataInputStream(fin).readLine();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("Local file copy content: " + content);

	}
}
