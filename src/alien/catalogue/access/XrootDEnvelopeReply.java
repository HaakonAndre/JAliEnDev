package alien.catalogue.access;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lazyj.Format;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.se.SE;
import alien.se.SEUtils;

/**
 * @author ron
 * 
 */
public class XrootDEnvelopeReply implements Serializable {


	/**
	 * 
	 */
	private static final long serialVersionUID = 213489317746462701L;

	/**
	 * pfn of the file on the SE (proto:://hostdns:port//storagepath)
	 */
	public final PFN pfn;

	/**
	 * Signed envelope
	 */
	protected String signedEnvelope;

	/**
	 * Create a signed only envelope in order to verify it
	 * 
	 * @param envelope
	 */
	public XrootDEnvelopeReply(String envelope) {

		StringTokenizer st = new StringTokenizer(envelope, "\\&");
		String pfn = "";
		String turl = "";
		String guid = "";
		String se = "";
		int size = 0;
		String md5 = "";

		while (st.hasMoreTokens()) {
			String tok = st.nextToken();

			int idx = tok.indexOf('=');

			if (idx >= 0) {
				String key = tok.substring(0, idx);
				String value = tok.substring(idx + 1);

				if ("path".equals(key))
					pfn = value;
				else if ("size".equals(key))
					size = Integer.parseInt(value);
				else if ("md5".equals(key))
					md5 = value;
				else if ("se".equals(key))
					se = value;
			}
		}
		
		final SE rSE = SEUtils.getSE(se);
		
		System.out.println("pfn: " + pfn + " guid: " + guid + " size: " + size + " md5: " + md5
				+ " se: " + se);
		

		final GUID g = GUIDUtils.getGUID(UUID.fromString(pfn.substring(pfn.lastIndexOf('/')+1)), true);
		g.md5 = md5;
		g.size = size;
		
		if (rSE!=null && rSE.seioDaemons!=null && rSE.seioDaemons.length()>0)
			pfn = rSE.seioDaemons + "/" + pfn;
		
		System.out.println("pfn: " + pfn + " guid: " + guid + " size: " + size + " md5: " + md5);
				System.out.println( " se: " + rSE.seName);

		this.pfn = new PFN(pfn, g, SEUtils.getSE(se));

		signedEnvelope = envelope;
	}

	
}
