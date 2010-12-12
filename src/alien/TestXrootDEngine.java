package alien;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;
import java.util.logging.Logger;

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
	static transient final Logger logger = ConfigUtils.getLogger(Testing.class.getCanonicalName());
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		AliEnPrincipal user = UserFactory.getByUsername("sschrein");
		
		String lfnOrGUID = "/alice/cern.ch/user/s/sschrein/testJDLFULL2.jdl";
		String access = "read";
		File myCopy = new File("/tmp/javateststuff");
		
		
		AccessTicket ticket = AuthorizationFactory.requestAccess(user, lfnOrGUID, access);

		XrootDEnvelopeSigner signEngine = new XrootDEnvelopeSigner();
		
		signEngine.signEnvelopesForAccess(ticket);
		
		
		Set<PFN> pfns = ticket.getPFNS();
		
		PFN pfn = pfns.iterator().next();
		
		
		Xrootd xrd  = new Xrootd();
		try {
			xrd.get(pfn,myCopy);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String content = "";
		try {
			FileReader fr = new FileReader(myCopy);
			content = fr.toString();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Local file copy content: " + content);
		

	}
}

