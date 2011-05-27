package alien.soap.services;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import lazyj.Format;



import alien.catalogue.BookingTable;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.se.SE;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;
import alien.user.UserFactory;

/**
 * @author ron
 * @since Nov 9, 2010
 */
public class AuthenEngine {


	@SuppressWarnings("unused")
	
	
//	private String verificationEnvelope = "-----BEGIN SEALED CIPHER-----\n"
//			+ "Amkq4hz7cJBtP4SxPyk-8d7OGPokdSewpfqwwIbilH1PfH7hAY7pnVXTLDd1E00+4uNsbwh81Rog\n"
//			+ "oMB4FtTb3ccjqQ9bsQ0fAcXXGboSG1fu-Trk1dg-3os35tjsMcNMEg662qMcdtOLSxCIOsQs5HJP\n"
//			+ "G+DvvH6GL-0xqw3veko=\n"
//			+ "-----END SEALED CIPHER-----\n"
//			+ "-----BEGIN SEALED ENVELOPE-----\n"
//			+ "AAAAgF0cAcKEjcklYmCWQlL+L5nIRS-qBZfShf2X5zbnB4atPhl7RRQBWOhJn1oXcjoNMYtiC0RP\n"
//			+ "raA+oetWr04-C2lkcJkIyI4yO70vBIDF4W-JuR33o9xVA7tVVG6cKyVsOfw5GygJNFBkNrtt8XVx\n"
//			+ "R8L79guHNUUvni0GNZvNrok2KjkmCR+2c9ZQsSdoOk1hIrfd0E71VJsHOa0a0U925aUZOX6ETFt-\n"
//			+ "rh+qfHHJiULAHk9sl-oTvpWYLgyCN0+2ImT9r5R+GpF0e+PiZu9h3kFDuE6N4UTigOI78+hv0daH\n"
//			+ "QuAbEG6-k0QX0UTqE5X45lBAVAx75ddh3xGERywhYqWywwGxSWENjTQ+L9+xMi19CTDWlkuRxmzE\n"
//			+ "kJKN6fxX4mheov64wwDm-e5CiKJIoKYoWxqrMXdI6FyJhL5wuA0jAUzSaVg3y3mGE2CT-mGDEx0E\n"
//			+ "gpho5cT56FWLaBmXlmsJxpVgIvK7Vkf8N4kgibRVkng59gbFnIQkWsUjGNyDqxZRSKJhFj3DR+fP\n"
//			+ "BzxQM5iTPuZxSeUxzAXE5Df9NFV8eeN8W9reXwUJqfl7xbwNI-AAfcRgFAeC9He2y8O15SlkgEog\n"
//			+ "hmOepWKTIfJ8Ei6MKYFZ2DkQh3QCfZ8HPVG3lsI1+1xQBYoOjhPuMk70o2OABO1z56a1EuGTWBS9\n"
//			+ "eUGu-2bJqTbQND5NzQRUzdae9QwfE0F9OsoX5NSg+dYGEHGEjerHN4kNvvBGAj6gVYbieFOdmzm5\n"
//			+ "gNgU+BzVI1yhHmj+maSgmO3RPNePeV5Gn254jCFOhhYzK0CZ8DLLCFja-IiT2NDTrujWo9vXSyt2\n"
//			+ "0TiCaue4IjwbaNzHrp9lR+5ntyg-riU-RIU6d1rNQkjI1nuA4B7vRBySdEykPLtZr1nVN-nwjz2u\n"
//			+ "BhizYsJevVg6-lt+RB29DJdVqjQA-Y5jvRqxaOCIL87RDBp1jZTCBQO-BF4AuDxN-gcvTXMcsc-t\n"
//			+ "JHn3Kaep6d7kq8eTHf-sGQcMc97tUg7Xvu2Wg6sTk5B5W+0npIfn6gElKLogxamThF2+XweZvWOc\n"
//			+ "Y2hfLARZB6Mp4cDe6xcpQm-k-CFxqsyHAiuB-Hyy\n"
//			+ "-----END SEALED ENVELOPE-----\n";

	// private String verificationEnvelope2 = "-----BEGIN SEALED CIPHER-----\n"
	// +
	// "qZqt5uMTSFSUYQBbgVllT+0XQxZatiS-NS2UaOSp2AL2M3voA0kEwumVcLHcBX7rOxsUckC3Xn66\n"
	// +
	// "QKWMRgFlUJpeZopoMt2LGdoNvBMaA60pqZeFx9e7-C91N2mX1Y1PH0Zia5bCckxi+XFF9JzKIctr\n"
	// + "WRa53VV5XRQ9N74EqTg=\n"
	// + "-----END SEALED CIPHER-----\n"
	// + "-----BEGIN SEALED ENVELOPE-----\n"
	// +
	// "AAAAgEk2PCx6I+BfJTFWNM2UKcTneHij6xCh3I8Ktrcnr4IIBw-5sBn8p1rc8gQ9YFBVndvQ5jFA\n"
	// +
	// "qq9M8fHcI9En657g56xN8FAqyeHZbwgocDAExCvuLrZtOMIVI5qDnEHKTXfv99Jl3VqeIaHEbtCx\n"
	// +
	// "T+m3IZS90ILPh2RDWZOW-iNSh4k-ebrtTYpB32jfXR3wEeXo+m5drUxW2c3ZmSU8V8Cv7Me2i2aF\n"
	// +
	// "CQSDwAgrSIDSKCEsnYu7L5-ZxNymJ1PGavbgWGYhyCq7KDDq4SkJFl2fAfEVlRYtxvX-A-1HYJ5v\n"
	// +
	// "s31Za1vIhAYU6CROoRE7rcXQBAETphC+bpolHmWazoMdTAC6ycTdRmtoHLmGQvZc+hzAXLNq8YU8\n"
	// +
	// "wN-YUnYHNmdohFm4ga7j+AUx40Re9fdZcow3D4lutBRqAYPA9wAk7yoFRSYZcqEY2K577vnxMLWr\n"
	// +
	// "yq3WP1Xdzbk7PpTzzsvcK0Kyj7Xm4zhz3s5fzJ8RUHKOgXvqJ34P3F0kVGJaaa0OdJ41A44bgJN4\n"
	// +
	// "f4fkjewpS5-uuJQ4tGHr+GUoyFZf2l7l2NWmSZc4xPZZbmHpr6q882xHpBLfN22DjeUjnmfI8aD9\n"
	// +
	// "BrHaut3RmlS4UkSlC4qNVMijwf1Js-VDo5S4GYlh6YwgWsFCO5M6v8D8Y8tAcwwGHKMCXg6rYv36\n"
	// +
	// "GLY-h1SmOWvMltxLKrVxJdMBG-LCtAN2yXS01EqhNBATzWu7R8pKM3gFYpPdG9nW7jL-AbrqRqMc\n"
	// +
	// "8KNM6xJY1Is0bv+9KMqfWIJC0yyVx8xGjmZ45MfPJndA-mGwrceNXXT2PlvFuNENI2ffTHq1g7wr\n"
	// +
	// "+VdI0LL3T4efmH1UIdlpi5rjYq6hPhVhvTTQHv3ZktZYmtDO89IdljHKFjhE3KUZr-d0+GRG394p\n"
	// +
	// "caEANTGSRIvdLhDJQbDFm2ISuT+3VNKJCTQZLoPt3Hee0z4EFepVA6ReDZDQjDHAHK+TfS4qiOJy\n"
	// +
	// "hrz1-xwixg+F6MuWoLhD2Am4xLeaxhjjB-KiKLNfjuh63xHjUPpbyJncOKt82X81lt-dNWP48Y7M\n"
	// +
	// "mIzk90CAQxANceEOjvnsqxXmPE0Znp1SFm1b5MVYEE1yDa0q1Iu+TIk451vvuPFRzSXpc7vSlN+v\n"
	// + "ZvqWHv5Zp0fHKsCVf0lAPulRbaSb\n"
	// + "-----END SEALED ENVELOPE-----\n";

//	private String verificationEnvelope2 = "-----BEGIN SEALED CIPHER-----\n"
//			+ "a8HJt2968uWC3Y5yo3OSh95tfDriXQPQp4cK4MrKe4LVykFTmDECM2kC9rSIbEkuRbbJWjqBhCFL\n"
//			+ "onyaa27eqKJtZvK2wsKNMsFRphAjMKX-yEIq+0X6NvVFxXdhiT3Nh94NCXUD8QmM-dYxJQNuh2fO\n"
//			+ "BjAsIZWMAPTt8LU43pY=\n"
//			+ "-----END SEALED CIPHER-----\n"
//			+ "-----BEGIN SEALED ENVELOPE-----\n"
//			+ "AAAAgF1tObrdN8AciMmqQAbdJK6dP8AsJ9z8HBBOdbGyXwP+4TXVRI-C1Yn7Z85JrmbfZEKHnoa+\n"
//			+ "EvpOAEyt12e5V2UCRy4KcBnZJvAMdAy8+0CMY8S6B12GWpGIXCghzNJaBVfiu52kqTUlbbYB0Auy\n"
//			+ "VsahRrvjVBlaTi3kvgJ8AzeFP1yhLBlEK9Zu0m6h2ZHjk-hAg7Ij1XD+plB9Wm4AkbAXPYW++398\n"
//			+ "Dx08YuT8wIfhGIBqoby1cydrHMD2kP+x6jgReXwgyVoYbt+jJsHhz0c+g4dXAsMC81Zv1ZNWN7sD\n"
//			+ "dOK-zwOCOFVbFf9vflnyfHNSu67b3vqesGFVElFQW+Pea1qmPS7RseYhvr3JIBWZ5PKmOnaXjxBx\n"
//			+ "8vr5ABz5QXX-YzWuLqmspeBmoCzhHHS1tT-phg9DYOA8voo6ulHGZppuMb-8uf5-Lmw6ymyLEjG-\n"
//			+ "Q5+FCDeGSEehxLHeiovHfitWJEJectgq-jROJ07QNIBO5y7bkWURx+hUq7At2jAYuoebFZaUhFZF\n"
//			+ "aWK8ePDGdIGdpMkQ40p32nriSR8YilBe3nsIFXiH3cWTvM1+KEFRBePqA0obORU4qu5M8TwWqlHh\n"
//			+ "0R5iuoKa5HPg+k1frs5dLxoh-hZpFI3M8Z2+FfVIoukqtkgzBWtVdUH5wpNkiaBlsQFLQcYe26dj\n"
//			+ "thL0R-kK3kUUxX7VZ4cMWfUyvbahRrUzhGnkUCBWhxcG9AN+m0Hji1Px9ZHJJlH3oURfHpxFAQm4\n"
//			+ "jxeBE5RACY7IgiAdSvJ4Kx5ralaMidmwSVBzQe7ANX8VmbABPREz5jLp5R8O7w7nf5bEEmuY+iJn\n"
//			+ "eOsMphqH+dL6PqjJrd60bZrOE5WbJHSFwUJwS9D8f1mlXoxGNf8sqQAyp1fG19-3aNDg-pNBAgDt\n"
//			+ "0yYSBouOS03gUjQp68LgDdZaRSclBuOQWtWkMo8vxrm5oE-xOjrOHYdQLI1A1XfAdqMtHKwGLFfD\n"
//			+ "jcFASMGoVrl8yJ6vyPnieFDPaOTOmEte6BlIHwlqJg5etQ78xx5ax17qltZzImj-hKYkqryr9usx\n"
//			+ "	w1iJgq9szENJZfAD4Ii7wfrV1PmgnVgOsxR7IUGi2sCqlpT5lMKAJCr3QhiU+86HybvUqzB0jbpW\n"
//			+ "	Bk0u8RpeBy3zlEKfHOKQJawk1J4ePxgr57-nZxSBH2RY21YsKA==\n"
//			+ "	-----END SEALED ENVELOPE-----\n";

	// -----BEGIN SEALED CIPHER-----
	// qZqt5uMTSFSUYQBbgVllT+0XQxZatiS-NS2UaOSp2AL2M3voA0kEwumVcLHcBX7rOxsUckC3Xn66
	// QKWMRgFlUJpeZopoMt2LGdoNvBMaA60pqZeFx9e7-C91N2mX1Y1PH0Zia5bCckxi+XFF9JzKIctr
	// WRa53VV5XRQ9N74EqTg=
	// -----END SEALED CIPHER-----
	// -----BEGIN SEALED ENVELOPE-----
	// AAAAgEk2PCx6I+BfJTFWNM2UKcTneHij6xCh3I8Ktrcnr4IIBw-5sBn8p1rc8gQ9YFBVndvQ5jFA
	// qq9M8fHcI9En657g56xN8FAqyeHZbwgocDAExCvuLrZtOMIVI5qDnEHKTXfv99Jl3VqeIaHEbtCx
	// T+m3IZS90ILPh2RDWZOW-iNSh4k-ebrtTYpB32jfXR3wEeXo+m5drUxW2c3ZmSU8V8Cv7Me2i2aF
	// CQSDwAgrSIDSKCEsnYu7L5-ZxNymJ1PGavbgWGYhyCq7KDDq4SkJFl2fAfEVlRYtxvX-A-1HYJ5v
	// s31Za1vIhAYU6CROoRE7rcXQBAETphC+bpolHmWazoMdTAC6ycTdRmtoHLmGQvZc+hzAXLNq8YU8
	// wN-YUnYHNmdohFm4ga7j+AUx40Re9fdZcow3D4lutBRqAYPA9wAk7yoFRSYZcqEY2K577vnxMLWr
	// yq3WP1Xdzbk7PpTzzsvcK0Kyj7Xm4zhz3s5fzJ8RUHKOgXvqJ34P3F0kVGJaaa0OdJ41A44bgJN4
	// f4fkjewpS5-uuJQ4tGHr+GUoyFZf2l7l2NWmSZc4xPZZbmHpr6q882xHpBLfN22DjeUjnmfI8aD9
	// BrHaut3RmlS4UkSlC4qNVMijwf1Js-VDo5S4GYlh6YwgWsFCO5M6v8D8Y8tAcwwGHKMCXg6rYv36
	// GLY-h1SmOWvMltxLKrVxJdMBG-LCtAN2yXS01EqhNBATzWu7R8pKM3gFYpPdG9nW7jL-AbrqRqMc
	// 8KNM6xJY1Is0bv+9KMqfWIJC0yyVx8xGjmZ45MfPJndA-mGwrceNXXT2PlvFuNENI2ffTHq1g7wr
	// +VdI0LL3T4efmH1UIdlpi5rjYq6hPhVhvTTQHv3ZktZYmtDO89IdljHKFjhE3KUZr-d0+GRG394p
	// caEANTGSRIvdLhDJQbDFm2ISuT+3VNKJCTQZLoPt3Hee0z4EFepVA6ReDZDQjDHAHK+TfS4qiOJy
	// hrz1-xwixg+F6MuWoLhD2Am4xLeaxhjjB-KiKLNfjuh63xHjUPpbyJncOKt82X81lt-dNWP48Y7M
	// mIzk90CAQxANceEOjvnsqxXmPE0Znp1SFm1b5MVYEE1yDa0q1Iu+TIk451vvuPFRzSXpc7vSlN+v
	// ZvqWHv5Zp0fHKsCVf0lAPulRbaSb
	// -----END SEALED ENVELOPE-----
//
//	/**
//	 * @param args
//	 */
//	public static void main(String[] args) {
//
//		AuthenEngine authen = new AuthenEngine();
//
//		HashSet<SE> empty = new HashSet<SE>();
//		
//		String[] envelopes;
////		envelopes = authen.authorize("sschrein", 1,
////				 "/alice/cern.ch/user/s/sschrein/testJDLFULL2.jdl", 440, "a00e6c3e-3cbb-11df-8620-0018fe730ae5",
////				empty, empty, "", 1,
////				"CERN");
////		
////		System.out.println("we received:");
////		System.out.println(Arrays.toString(envelopes));
//		
//		try {
//
//			authen.loadKeys();
//
//			String localfile = "/tmp/javatestgedoehns";
//			String localcopy = "/tmp/javatestgedoehns.copy";
//			
//			
//			String turl = "root://pcepalice11.cern.ch:1094//opt/xrootd_storage/xr1/13/36958/a891c798-e29c-11df-8143-001e0b24002f";
//			
////			String turl = "root://pcepalice11.cern.ch:1094//tmp/a891c798-e29c-11df-8143-001e0b24002f";
//
//			String readEnvelope = "turl="+ turl +
//			"&access=read&lfn=/pcepalice11/user/a/ali/testing.output.lasttryX&size=9&se=pcepalice11::CERN::XR1"+
//			"&guid=a891c798-e29c-11df-8143-001e0b24002f&md5=fb8e3a8f65d2bbfd9d4d6de6bef2519d&user=ali";
//			
//			String writeEnvelope = "turl="+ turl +
//			"&access=write&lfn=/pcepalice11/user/a/ali/testing.output.lasttryX&size=18&se=pcepalice11::CERN::XR1"+
//			"&guid=a891c798-e29c-11df-8143-001e0b24002f&md5=fb8e3a8f65d2bbfd9d4d6de6bef2519d&user=ali";
//			
//			String hashord = "turl-access-lfn-size-se-guid-md5-user";
//			
//
//
//			System.out.println("generated testVO: ");
//		
//			Xrootd_implementation xrootd = new Xrootd_implementation();
//			xrootd.xrdUpload(turl, authen.dryEngine(writeEnvelope, hashord), localfile, 18);
////			xrootd.xrdGet(turl, authen.dryEngine(readEnvelope, hashord), localcopy, 9);
//
////			
////			
////			System.out.println("xrdcpapmon  -DIFirstConnectMaxCnt 6 root://pcepalice11.cern.ch:1094//opt/xrootd_storage/xr1/14/58772/0fefd262-0148-11e0-be40-001e0b24002f"+
////					" /tmp/pcepalice11/cache/0fefd262-0148-11e0-be40-001e0b24002f.16591291647188"+
////					" -OS\\&"
////			+ 
////			authen.dryEngine(baseEnvelope, hashord));
//
//
//		} catch (GeneralSecurityException gexcept) {
//			System.out.println("General Securiry exception" + gexcept);
//		} catch (IOException ioexcept) {
//			System.out.println("IO exception" + ioexcept);
//		}
//		
//		
//
//
//	}
//
//	public static String accessString(String P_user, int access,
//			String P_options, String P_lfn, int size, String P_guid,
//			Set<SE> ses, Set<SE> exxSes, String P_qos, int qosCount,
//			String P_sitename) {
//
//		AuthenServer authen = new AuthenServer();
//
//		String ret = "";
//
//		try {
//
//			Set<XrootDEnvelope> envelopes = authen.createEnvelopePerlAliEnV218(
//					P_user, access, P_options, P_lfn, size, P_guid, ses,
//					exxSes, P_qos, qosCount, P_sitename);
//
//			authen.loadKeys();
//			EncryptedAuthzToken authz = new EncryptedAuthzToken(
//					authen.AuthenPrivKey, authen.SEPubKey);
//			System.out.println();
//			// System.out.println();
//			// System.out.println("loaded authz engine");
//
//			for (XrootDEnvelope env : envelopes) {
//				env.decorateEnvelope(authz);
//				// System.out.println("envelope ready: " +
//				// env.getPerlEnvelopeTicket().get("envelope"));
//				ret += "\n" + env.getPerlEnvelopeTicket().get("envelope")
//						+ "\n";
//
//			}
//		} catch (GeneralSecurityException gexcept) {
//			System.out.println("General Securiry exception" + gexcept);
//		} catch (IOException ioexcept) {
//			System.out.println("IO exception" + ioexcept);
//		}
//		return ret;
//	}
//
//	public static Hashtable<String, String>[] access(String P_user,
//			String P_access, String P_options, String P_lfn, String P_size,
//			String P_guid, String p_ses, String P_exxSes, String P_qos,
//			String P_qosCount, String P_sitename) {
//
//		System.out.println("we are invoked first: user: " + P_user + ", code: "
//				+ P_access + ", options: " + P_options + ", lfn: " + P_lfn
//				+ ", size: " + P_size + ", guid: " + P_guid);
//
//		int access = new Integer(P_access);
//		int size = new Integer(P_size);
//		int qosCount = new Integer(P_qosCount);
//
//		Set<SE> ses = new LinkedHashSet<SE>();
//
//		Set<SE> exxses = new LinkedHashSet<SE>();
//
//		System.out.println("we are invoked second: user: " + P_user
//				+ ", code: " + access + ", options: " + P_options + ", lfn: "
//				+ P_lfn + ", size: " + size + ", guid: " + P_guid);
//
//		Hashtable<String, String>[] envList = new Hashtable[1];
//
//		// Hashtable<String,String> returnEnvs = new Hashtable<String,String>();
//
//		//
//		// returnEnvs.put("se","eins");
//		// returnEnvs.put("access","zwei");
//		//
//		// envList[0] = returnEnvs;
//
//		AuthenServer authen = new AuthenServer();
//
//		try {
//
//			Set<XrootDEnvelope> envelopes = authen.createEnvelopePerlAliEnV218(
//					P_user, access, P_options, P_lfn, size, P_guid, ses,
//					exxses, P_qos, qosCount, P_sitename);
//
//			authen.loadKeys();
//			EncryptedAuthzToken authz = new EncryptedAuthzToken(
//					authen.AuthenPrivKey, authen.SEPubKey);
//			System.out.println();
//			// System.out.println();
//			// System.out.println("loaded authz engine");
//
//			for (XrootDEnvelope env : envelopes) {
//
//				env.decorateEnvelope(authz);
//				System.out.println("envelope ready: "
//						+ env.getPerlEnvelopeTicket().get("envelope"));
//
//				envList[0] = env.getPerlEnvelopeTicket();
//				// returnEnvs.add(env.getPerlEnvelopeTicket().get("envelope"));
//
//			}
//		} catch (GeneralSecurityException gexcept) {
//			System.out.println("General Securiry exception" + gexcept);
//		} catch (IOException ioexcept) {
//			System.out.println("IO exception" + ioexcept);
//		}
//		return envList;
//	}

////	public String[] authorize(String P_user, int access, 
//			String P_lfn, int size, String P_guid, Set<SE> ses, Set<SE> exxSes,
//			String P_qos, int qosCount, String P_sitename) {
//		
//		
//
//		System.out.println("we are invoked:  P_user: " + P_user + "\naccess: " + access 
//			+ "\nP_lfn: " + 	
//			 P_lfn + "\nsize: " + size + "\nP_guid: " +  P_guid 
//			 + "\nP_qos: " + P_qos + "\nqosCount: " +  qosCount + "\nP_sitename: " +  P_sitename);
//		
//		ArrayList<String> signedEnvelopes = null;
//		
//		AuthenServer authen = new AuthenServer();
//		try {
//
//			AliEnPrincipal user = UserFactory.getByUsername(P_user);
//
//			CatalogueAccess ca = AuthorizationFactory.requestAccess(user,
//					P_lfn, access);
//
//			XrootDEnvelopeDecorator
//					.loadXrootDEnvelopesForCatalogueAccess(ca, P_sitename,
//							P_qos, qosCount, ses, exxSes);
//
//			authen.loadKeys();
//			EncryptedAuthzToken authz = new EncryptedAuthzToken(
//					authen.AuthenPrivKey, authen.SEPubKey);
//				
//			signedEnvelopes = XrootDEnvelopeDecorator.signEnvelope(authen.AuthenPrivKey,
//					ca.getEnvelopes());

//			FOR (XROOTDENVELOPE ENV : CA.GETENVELOPES()) {
//				
//				
//				
//				ENV.DECORATEENVELOPE(AUTHZ);
//				// SYSTEM.OUT.PRINTLN("ENVELOPE READY: " +
//				// ENV.GETPERLENVELOPETICKET().GET("ENVELOPE"));
//				RET += "\N" + ENV.GETPERLENVELOPETICKET().GET("ENVELOPE")
//						+ "\N";
//
//			}
//		} catch (GeneralSecurityException gexcept) {
//			System.out.println("General Securiry exception" + gexcept);
//		} catch (IOException ioexcept) {
//			System.out.println("IO exception" + ioexcept);
//		}
//		return (String[]) signedEnvelopes.toArray(new String[0]);
//
//	}

	public List<String> authorizeEnvelope(AliEnPrincipal certOwner,
			String p_user, String p_dir, String access,
			HashMap<String, String> optionHash, String p_jobid) {

		boolean evenIfNotExists = false;
		AliEnPrincipal user = UserFactory.getByUsername(p_user);

		AccessType accessRequest = AccessType.NULL;

		if (access.startsWith("write")) {
			accessRequest = AccessType.WRITE;
			evenIfNotExists = true;
		} else if (access.equals("read")) {
			accessRequest = AccessType.READ;
		} else if (access.equals("delete")) {
			accessRequest = AccessType.DELETE;
		} else {
			System.out.println("illegal access type!");
			return null;
		}
		int jobid = new Integer(sanitizePerlString(p_jobid, true));

		int p_size = new Integer(sanitizePerlString(optionHash.get("size"),
				true));
		int p_qosCount = new Integer(sanitizePerlString(
				optionHash.get("writeQosCount"), true));
		String p_lfn = sanitizePerlString(optionHash.get("lfn"), false);
		if (!p_lfn.startsWith("//"))
			p_lfn = p_dir + p_lfn;
		String p_guid = sanitizePerlString(optionHash.get("guid"), false);
		String p_guidrequest = sanitizePerlString(
				optionHash.get("guidRequest"), false);
		String p_md5 = sanitizePerlString(optionHash.get("md5"), false);
		String p_qos = sanitizePerlString(optionHash.get("writeQos"), false);
		String p_pfn = sanitizePerlString(optionHash.get("pfn"), false);
		String p_links = sanitizePerlString(optionHash.get("links"), false);
		String p_site = sanitizePerlString(optionHash.get("site"), false);

		String[] splitWishedSE = sanitizePerlString(optionHash.get("wishedSE"),
				false).split(";");
		List<SE> ses = new ArrayList<SE>(splitWishedSE.length);
		for (String sename : Arrays.asList(splitWishedSE)) {
			SE se = SEUtils.getSE(sename);
			if (se != null){
				ses.add(se);
				System.out.println("An SE found: " + se.getName());
			}
		}

		String[] splitExcludeSE = sanitizePerlString(
				optionHash.get("excludeSE"), false).split(";");
		List<SE> exxSes = new ArrayList<SE>(splitExcludeSE.length);
		for (String sename : Arrays.asList(splitExcludeSE)) {
			SE se = SEUtils.getSE(sename);
			if (se != null){
				ses.add(se);
				System.out.println("An exSE found: " + se.getName());
			}
		}

		if ((ses.size() + p_qosCount) <= 0) {
			p_qos = "disk";
			p_qosCount = 2;
		}

		System.out.println("we are invoked:  user: " + p_user + "\naccess: "
				+ access + "\nlfn: " + p_lfn + "\nsize: " + p_size
				+ "\nrequestguid: " + p_guidrequest + "\nqos: " + p_qos
				+ "\nqosCount: " + p_qosCount + "\nsitename: " + p_site + "\nSEs: " + optionHash.get("wishedSE")
				 + "\nexSEs: " + optionHash.get("excludeSE")+"\n...\n");


		LFN lfn = null;
		GUID guid = null;
		if (GUIDUtils.isValidGUID(p_lfn)) {
			guid = GUIDUtils.getGUID(UUID.fromString(p_lfn), evenIfNotExists);
		} else {
			lfn = LFNUtils.getLFN(p_lfn, evenIfNotExists);
			if(lfn.guid==null) {
				guid = GUIDUtils.createGuid();
				lfn.guid = guid.guid;
			} else {
			    guid = GUIDUtils.getGUID(lfn.guid, evenIfNotExists );
			}
		}

		List<PFN> pfns = new ArrayList<PFN>(ses.size() + p_qosCount);
		
		LFN setArchiveAnchor = null;

		try {
			if (accessRequest == AccessType.WRITE) {

				// statis list of specified SEs
				for (SE se : ses) {
					pfns.add(BookingTable.bookForWriting(user, lfn, guid, null,
							jobid, se));
				}

				if (p_qosCount > 0) {
					ses.addAll(exxSes);
					List<SE> SEs = SEUtils.getClosestSEs(p_site,ses);
					final Iterator<SE> it = SEs.iterator();

					int counter = 0;
					while (counter < p_qosCount && it.hasNext()) {
						SE se = it.next();
						System.out.println("Trying to book writing on SE: " + se.getName());

							pfns.add(BookingTable.bookForWriting(user, lfn,
									guid, null, jobid, se));
							counter++;
					}

				}
			}
			if (accessRequest == AccessType.READ) {

				PFN readpfn = null;

				pfns = SEUtils.sortBySiteSpecifySEs(guid.getPFNs(), p_site,
						true, ses, exxSes);

				for (PFN pfn : pfns) {
					System.err.println(pfn);
					System.out.println("Asking read for " + user.getName()
							+ " to " + pfn.getPFN());
					String reason = AuthorizationFactory.fillAccess(user, pfn,
							AccessType.READ);

					if (reason != null) {
						System.err.println("Access refused because: " + reason);
						continue;
					}
					UUID archiveLinkedTo = pfn.retrieveArchiveLinkedGUID();
					if (archiveLinkedTo != null) {
						GUID archiveguid = GUIDUtils.getGUID(archiveLinkedTo,
								false);
						setArchiveAnchor = lfn;
						List<PFN> apfns = SEUtils.sortBySiteSpecifySEs(
								GUIDUtils.getGUID(
										pfn.retrieveArchiveLinkedGUID())
										.getPFNs(), p_site, true, ses, exxSes);
						if (!AuthorizationChecker.canRead(archiveguid, user)) {
							System.err
									.println("Access refused because: Not allowed to read sub-archive");
							continue;
						}

						for (PFN apfn : apfns) {
							reason = AuthorizationFactory.fillAccess(user,
									apfn, AccessType.READ);

							if (reason != null) {
								System.err.println("Access refused because: "
										+ reason);
								continue;
							}
							System.out
									.println("We have an evenlope candidate: "
											+ apfn.getPFN());
							readpfn = apfn;
							break;

						}
					} else {
						readpfn = pfn;
					}
					pfns.clear();
					pfns.add(readpfn);
					break;

				}
			}
		} catch (Exception e) {
			System.out.println("exception: " + e.toString());
		}

		List<String> envelopes = new ArrayList<String>(pfns.size());

		for (PFN pfn : pfns) {
			if (pfn.ticket.envelope == null) {
				System.err.println("Sorry ... Envelope is null!");
			} else {
				pfn.ticket.envelope.setArchiveAnchor(setArchiveAnchor);
				if (pfn.ticket.envelope.getEncryptedEnvelope() == null) {
					System.err
							.println("Sorry ... getInternalEnvelope is null!");
				} else {
						envelopes
								.add(pfn.ticket.envelope
												.getUnsignedEnvelope().replace(
														"&", "\\&")
												+ "\\&hashord=lfn-pfn-turl-md5-size-guid-zguid-se-access\\&signature=1234556\\&oldEnvelope="
												+ pfn.ticket.envelope
														.getEncryptedEnvelope());
						System.out.println("enc: " + pfn.ticket.envelope
								.getUnEncryptedEnvelope());
						System.out.println("sgn: " + pfn.ticket.envelope
								.getUnsignedEnvelope());
					}
				}
			}
		
		return envelopes;

	}

	public String sanitizePerlString(String maybeNull,boolean isInteger){
		if(maybeNull == null){
			if(isInteger)
				return "0";
			return "";
			}
		return maybeNull;
	}
	
	
	
	
	
	
//	public Set<XrootDEnvelope> createEnvelopePerlAliEnV218(String P_user,
//			int access, String P_options, String P_lfn, int size,
//			String P_guid, Set<SE> ses, Set<SE> exxSes, String P_qos,
//			int qosCount, String P_sitename) {
//
//		AliEnPrincipal user = UserFactory.getByUsername(P_user);
//		//
//		// LFN lfn = LFNUtils
//		// .getLFN(P_lfn);
//		//
//		// CatalogEntity requestedEntry;
//
//		CatalogueAccess ca = AuthorizationFactory.requestAccess(user, P_lfn,
//				access);
//
//		CatalogueAccessEnvelopeDecorator
//				.loadXrootDEnvelopesForCatalogueAccess(ca, P_sitename,
//						P_qos, qosCount, ses, exxSes);
//
//		return ca.getEnvelopes();
//
//	}

	
	
//	
//	/**
//	 * Create envelope
//	 * 
//	 * @param user
//	 * @param egal
//	 * @param envreq
//	 * @param lfn
//	 * @param staticSEs
//	 * @param size
//	 * @param noSEs
//	 * @param guid
//	 * @param site
//	 * @param qos
//	 * @param qosCount
//	 * @return the envelope
//	 */
//	public String createEnvelope(String user, String egal, String envreq,
//			String lfn, String staticSEs, String size, String noSEs,
//			String guid, String site, String qos, String qosCount) {
//
//		// for (int i = 0; i < celsius.length; i++) {
//		//
//		// try {
//		// System.out.println("getting" + celsius[i]);
//		// backer += ",";
//		// backer += celsius[i];
//		//
//		// } catch (java.lang.ArrayIndexOutOfBoundsException iob) {
//		// break;
//		// }
//		// }
//		System.out.println("i got: user: " + user + ", envreq: " + envreq
//				+ ", lfn: " + lfn + ",staticSEs: " + staticSEs);
//
//		// Hashtable<String, String> envelope = new Hashtable<String, String>();
//		// envelope.put("access", envreq);
//		// envelope.put("turl",
//		// "root://lpsc-se-dpm-server.in2p3.fr:1094//dpm/in2p3.fr/home/alice/06/13580/7c1e082e-e81f-11df-93de-001e0b24002f");
//		// envelope.put("pfn",
//		// "/dpm/in2p3.fr/home/alice/06/13580/7c1e082e-e81f-11df-93de-001e0b24002f");
//		// envelope.put("pfn", lfn);
//		// envelope.put("size", "1234");
//		// envelope.put("se", "pcepalice11::CERN::DPM");
//		// envelope.put("guid", "7c1e082e-e81f-11df-93de-001e0b24002f");
//		// envelope.put("md5", "4eef16773f59388963526254922bf5ef");
//		// envelope.put("envelepe",
//		// "111111111111111111000000000000000000000000000xxxxxxxxxxxxxxxxxxxxxxxxxx");
//		// envelope.put("signedEnvelope", envreq);
//		//
//		// String ticket = "<authz>\n  <file>" + "\n";
//		// ticket += "    <access>write-once</access>" + "\n";
//		// ticket +=
//		// "    <turl>root://lpsc-se-dpm-server.in2p3.fr:1094//dpm/in2p3.fr/home/alice/14/00977/087984d8-eadc-13df-8274-001e0b24002f</turl>"
//		// + "\n";
//		// ticket += "    <lfn>/pcepalice11/user/a/ali/ksksksk</lfn>" + "\n";
//		// ticket += "    <size>13593</size>" + "\n";
//		// ticket +=
//		// "    <pfn>/dpm/in2p3.fr/home/alice/14/00977/087984d8-eadc-13df-8274-001e0b24002f</pfn>"
//		// + "\n";
//		// ticket += "    <se>pcepalice11::CERN::DPM</se>" + "\n";
//		// ticket += "    <guid>087984d8-eadc-13df-8274-001e0b24002f</guid>" +
//		// "\n";
//		// ticket += "    <md5>4eef16773f59388963526254922bf5ef</md5>" + "\n";
//		// ticket += "  </file>\n</authz>\n";
//
//		String ticket = "<authz>\n  <file>" + "\n";
//		ticket += "    <access>write-once</access>" + "\n";
//		ticket += "    <turl>root://nanxrdmgr01.in2p3.fr:1094//01/10795/d3a4f56e-ec41-11df-ab69-33fa9fe34833</turl>"
//				+ "\n";
//		ticket += "    <lfn>/pcepalice11/user/a/ali/ksksksk</lfn>" + "\n";
//		ticket += "    <size>18295</size>" + "\n";
//		ticket += "    <pfn>/01/10795/d3a4f56e-ec41-11df-ab69-33fa9fe34833</pfn>"
//				+ "\n";
//		ticket += "    <se>pcepalice11::CERN::Suba</se>" + "\n";
//		ticket += "    <guid>d3a4f56e-ec41-11df-ab69-33fa9fe34833</guid>"
//				+ "\n";
//		ticket += "    <md5>4eef16773f59388963526254922bf5ef</md5>" + "\n";
//		ticket += "  </file>\n</authz>\n";
//
//		// envEngine = SealedEnvelope.InitializeEngine();
//
//		String encrTicket = "";
//
//		// envEngine = envEngine.TSealedEnvelope(lpriv.toCharArray(),
//		// lpub.toCharArray(), rpriv.toCharArray(), rpub.toCharArray(),
//		// cipher.toCharArray(),
//		// creator.toCharArray(), 0);
//		// TSealedEnvelope envEngine = new TSealedEnvelope();
//
//		// envEngine.Reset();
//		// envEngine.Initialize(2);
//
//		try {
//			loadKeys();
//			System.out.println("loaded keys");
//
//			EncryptedAuthzToken authz = new EncryptedAuthzToken(AuthenPrivKey,
//					SEPubKey);
//			System.out.println();
//			System.out.println();
//			System.out.println("loaded authz engine");
//
//			encrTicket = authz.encrypt(ticket);
//			System.out.println("ticket encryption finished.");
//
//			System.out.println();
//			System.out.println();
//			EncryptedAuthzToken deauthz = new EncryptedAuthzToken(encrTicket,
//					SEPrivKey, AuthenPubKey);
//			System.out.println("ROUND 3, own ticket: decrypting");
//
//			String plain = deauthz.decrypt();
//
//			System.out.println("ticket decrypted");
//			System.out.println("ticket was:" + plain);
//
//			// System.out.println();
//			// System.out.println();
//			// EncryptedAuthzToken verdeauthz = new
//			// EncryptedAuthzToken(verificationEnvelope, SEPrivKey,
//			// AuthenPubKey);
//			// System.out.println("ROUND 1: decrypting");
//			//
//			// String verplain = verdeauthz.decrypt();
//			//
//			// System.out.println("ticket decrypted");
//			// System.out.println("ticket was:" + verplain);
//
//			System.out.println();
//			System.out.println();
//			EncryptedAuthzToken verdeauthz2 = new EncryptedAuthzToken(
//					verificationEnvelope2, SEPrivKey, AuthenPubKey);
//			System.out.println("ROUND 2: decrypting");
//
//			String verplain2 = verdeauthz2.decrypt();
//
//			System.out.println("ticket decrypted");
//			System.out.println("ticket was:" + verplain2);
//
//			System.out.println();
//			System.out.println();
//			System.out.println();
//			System.out.println();
//
//		} catch (GeneralSecurityException gexcept) {
//			System.out.println("General Securiry exception" + gexcept);
//		}
//
//		catch (IOException ioexcept) {
//			System.out.println("IO exception" + ioexcept);
//		}
//
//		// encrTicket = TSealedEnvelope.encode(lpriv,lpub,rpriv,rpub,ticket);
//
//		// envEngine.encodeEnvelopePerl(ticket,0,"none");
//		// encrTicket = envEngine.GetEncodedEnvelope();
//
//		// Hashtable<int, String> alles = new Hashtable[1];
//		// alles[0] = envelope;
//
//		System.out.println("i encrypted ticket:" + encrTicket);
//
//		return encrTicket;
//	}
//
//	private void loadKeys() throws GeneralSecurityException, IOException {
//
////		System.out.println("loading keys...");
//		// String lprivfile = "/home/ron/authen_keys/lpriv.key";
//		// String lpubfile = "/home/ron/authen_keys/lpub.pem";
//		// String rprivfile = "/home/ron/authen_keys/rpriv.pem";
//		// String rpubfile = "/home/ron/authen_keys/rpub.pem";
//
//		Security.addProvider(new BouncyCastleProvider());
//
//		// KeyFactory keyFactory = KeyFactory.getInstance("RSA");
//
//	
//		File AuthenPrivfile = new File("/home/ron/authen_keys/AuthenPriv.der");
//		byte[] AuthenPriv = new byte[(int) AuthenPrivfile.length()];
//		FileInputStream fis = new FileInputStream(AuthenPrivfile);
//		fis.read(AuthenPriv);
//		fis.close();
//
//		PKCS8EncodedKeySpec AuthenPrivSpec = new PKCS8EncodedKeySpec(AuthenPriv);
//		System.out.print("loading AuthenPriv...");
//		this.AuthenPrivKey = (RSAPrivateKey) KeyFactory.getInstance("RSA")
//				.generatePrivate(AuthenPrivSpec);
//
//		File SEPrivfile = new File("/home/ron/authen_keys/SEPriv.der");
//		byte[] SEPriv = new byte[(int) SEPrivfile.length()];
//		fis = new FileInputStream(SEPrivfile);
//		fis.read(SEPriv);
//		fis.close();
//
//		PKCS8EncodedKeySpec SEPrivSpec = new PKCS8EncodedKeySpec(SEPriv);
////		System.out.println("loading SEPriv...");
//		this.SEPrivKey = (RSAPrivateKey) KeyFactory.getInstance("RSA")
//				.generatePrivate(SEPrivSpec);
//
//		//
//		//
//
//		// byte[] lpriv = (new BufferedReader(new
//		// FileReader(lprivfile))).toString().getBytes();
//		// byte[] lpub = (new BufferedReader(new
//		// FileReader(lpubfile))).toString().getBytes();
//		// byte[] rpriv = (new BufferedReader(new
//		// FileReader(rprivfile))).toString().getBytes();
//		// byte[] rpub = (new BufferedReader(new
//		// FileReader(rpubfile))).toString().getBytes();
//
//		CertificateFactory certFact = CertificateFactory.getInstance("X.509");
//
//		File AuthenPubfile = new File("/home/ron/authen_keys/AuthenPub.crt");
////		System.out.println("loading SEPub...");
//		X509Certificate AuthenPub = (X509Certificate) certFact
//				.generateCertificate(new BufferedInputStream(
//						new FileInputStream(AuthenPubfile)));
//		this.AuthenPubKey = (RSAPublicKey) AuthenPub.getPublicKey();
//
//		File SEPubfile = new File("/home/ron/authen_keys/SEPub.crt");
////		System.out.println("loading rpub...");
//
//		X509Certificate SEPub = (X509Certificate) certFact
//				.generateCertificate(new BufferedInputStream(
//						new FileInputStream(SEPubfile)));
//		this.SEPubKey = (RSAPublicKey) SEPub.getPublicKey();
//
//		// this.lpub = (RSAPublicKey) ((X509CertificateObject) new PEMReader(new
//		// BufferedReader(new FileReader(lpub))).readObject());
//
//		// this.lpub = (RSAPublicKey) ((KeyPair) new PEMReader(new
//		// BufferedReader(
//		// new FileReader(lpub))).readObject()).getPublic();
//
//		// this.rpriv = (RSAPrivateKey) ((X509CertificateObject) new PEMReader(
//		// new BufferedReader(new FileReader(rpriv))).readObject());
//		//
//		// this.rpub = (RSAPublicKey) ((X509CertificateObject) new PEMReader(new
//		// BufferedReader(
//		// new FileReader(rpub))).readObject());
//
//		System.out.println("done!");
//
//	}
//
//	private String dryEngine(String baseEnvelope, String hashord) throws NoSuchAlgorithmException, InvalidKeyException, SignatureException {
//		
//		
//		long issued = System.currentTimeMillis() / 1000L;
//		long expires = issued + 86400;
//
//		String toBeSigned = baseEnvelope
//				+ "&issuer=JAuthenX&issued=" + issued + "&expires="
//				+ expires + "&hashord=" + hashord + "-issuer-issued-expires-hashord";
//				
//		Signature signer = Signature.getInstance("SHA384withRSA");
//		signer.initSign(AuthenPrivKey);
//		signer.update(toBeSigned.getBytes());
//		
//
//		byte[] rawsignature = new byte[1024];
//		rawsignature = signer.sign();
//
//		toBeSigned = toBeSigned + "&signature="
//				+ String.valueOf(Base64.encode(rawsignature));
//		return toBeSigned;
//	}
}

	