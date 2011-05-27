package alien.catalogue.access;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import sun.security.provider.JavaKeyStore.CaseExactJKS;

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
public class XrootDEnvelope implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1024787790575833398L;

	/**
	 * Format
	 */
	public static final String hashord = "turl-access-lfn-guid-se-size-md5";
	
	/**
	 * the access ticket this envelope belongs to
	 */
	public AccessType type = null;
	
	/**
	 * pfn of the file on the SE (proto:://hostdns:port//storagepath)
	 */
	public final PFN pfn;
	
	/**
	 * A LFN that is pointing to this envelope's GUID/PFN us as a guid:// archive link
	 */
	private LFN archiveAnchorLFN;
	
	
	/**
	 * Signed envelope
	 */
	protected String signedEnvelope; 
	
	/**
	 * Encrypted envelope
	 */
	protected String encryptedEnvelope;

	/**
	 * @param type
	 * @param pfn 
	 */
	public XrootDEnvelope(final AccessType type, final PFN pfn){
		this.type = type;
		this.pfn = pfn;
	}
	
	/**
	 * Create a signed only envelope in order to verify it
	 * 
	 * @param envelope
	 */
	public XrootDEnvelope(String envelope){
		
		StringTokenizer st = new StringTokenizer(envelope, "\\&");
		String pfn = "";
		String lfn = "";
		String guid = "";
		String se = "";
		int size = 0;
		String md5 = "";
		
	     while (st.hasMoreTokens()) {
	    	 String tok = st.nextToken();
	    	 
	    	 int idx = tok.indexOf('=');
	    	 
	    	 if (idx>=0){
	    		 String key = tok.substring(0, idx);
	    		 String value = tok.substring(idx+1);
	    	 
	    		 if("access".equals(key)) 
	    				if (value.startsWith("write")) {
	    					type = AccessType.WRITE;
	    				} else if (value.equals("read")) {
	    					type = AccessType.READ;
	    				} else if (value.equals("delete")) {
	    					type = AccessType.DELETE;
	    				} else {
	    					System.err.println("illegal access type!");
	    					type=null;
	    					this.pfn=null;
	    				}
	    		 else if ("pfn".equals(key))
	    			 pfn = value;
	    		 else if ("lfn".equals(key))
	    			 lfn = value;
	    		 else if ("guid".equals(key))
	    			 guid = value;
	    		 else if ("size".equals(key))
	    			 size = Integer.parseInt(value);
	    		 else if ("md5".equals(key))
	    			 md5 = value;
	    		 else if ("se".equals(key))
	    			 se = value;
	    	 }
	     }
	    GUID g = GUIDUtils.getGUID(UUID.fromString(guid), true);
	    g.md5 = md5;
	    g.size = size;
	    
		this.pfn = new PFN(pfn,g,SEUtils.getSE(se));
		
		signedEnvelope = envelope;
	}

	/**
	 * Set the LFN that is pointing to this envelope's GUID/PFN us as a guid:// archive link
	 * @param anchor Anchor LFN
     */	
	public void setArchiveAnchor(final LFN anchor){
		archiveAnchorLFN = anchor;
	}

	/**
	 * @return envelope xml
	 */
	public String getUnEncryptedEnvelope() {

		final String access = type.toString().replace("write", "write-once");

		final String[] pfnsplit = pfn.getPFN().split("//");

		final GUID guid = pfn.getGuid();

		final Set<LFN> lfns = guid.getLFNs();

		final SE se = SEUtils.getSE(pfn.seNumber);

		String ret = "<authz>\n  <file>\n" + "    <access>" + access + "</access>\n";

		String turl = pfn.getPFN();
		if (archiveAnchorLFN != null)
			turl += "#" + archiveAnchorLFN.getFileName();

		ret += "    <turl>" + Format.escHtml(turl) + "</turl>\n";
		
		LFN refLFN = null;
		GUID refGUID = guid;
		
		if (archiveAnchorLFN != null){
			refGUID = GUIDUtils.getGUID(archiveAnchorLFN.guid);
			refLFN = archiveAnchorLFN;
		}
		else if (lfns!=null && lfns.size()>0)
			refLFN = lfns.iterator().next();
		
		if (refLFN != null)
			ret += "    <lfn>" + Format.escHtml(refLFN.getCanonicalName()) + "</lfn>\n";
		else
			ret += "    <lfn>/NOLFN</lfn>\n";
					
		ret += "    <size>" + refGUID.size + "</size>" + "\n" + 
			   "    <guid>" + Format.escHtml(refGUID.getName().toUpperCase()) + "</guid>\n" + 
			   "    <md5>"	+ Format.escHtml(refGUID.md5) + "</md5>\n"+
			   "    <pfn>" + Format.escHtml("/" + pfnsplit[2]) + "</pfn>\n" + 
		       "    <se>" + Format.escHtml(se.getName())+ "</se>\n" + 
		       "  </file>\n</authz>\n";

		return ret;
	}

	/**
	 * Splitter of PFNs
	 */
	public static final Pattern PFN_EXTRACT = Pattern.compile("^\\w+://([\\w-]+(\\.[\\w-]+)*(:\\d+))?/(.*)$");
	
	/**
	 * @return URL of the storage. This is passed as argument to xrdcp and in most cases it is the PFN but for 
	 * 			DCACHE it is a special path ...
	 */
	public String getTransactionURL() {
		final SE se = SEUtils.getSE(pfn.seNumber);
		
		if (se == null)
			return null;
		
		if (se.seName.indexOf("DCACHE") > 0){
			final GUID guid = pfn.getGuid();
			
			final Set<LFN> lfns = guid.getLFNs();
			
			if (lfns!=null && lfns.size()>0)
				return se.seioDaemons + "/" + lfns.iterator().next().getCanonicalName();
			
			return se.seioDaemons + "//NOLFN";
		}
		
		final Matcher m = PFN_EXTRACT.matcher(pfn.pfn);
		
		if (m.matches()){
			if(archiveAnchorLFN!=null)
				return se.seioDaemons + "/" + m.group(4) + "#" + archiveAnchorLFN.getFileName();
			return se.seioDaemons + "/" + m.group(4);
		}
		if(archiveAnchorLFN!=null)
			return pfn.pfn + "#" + archiveAnchorLFN.getFileName();
		return pfn.pfn;
	}

	/**
	 * @return url envelope
	 */
	public String getUnsignedEnvelope() {
				
		final GUID guid = pfn.getGuid();

		
		final Set<LFN> lfns = guid.getLFNs();
		
		final SE se = SEUtils.getSE(pfn.seNumber);
		
		String ret = "turl=" + pfn.getPFN();
		if(archiveAnchorLFN!=null)
			ret = ret + "#" + archiveAnchorLFN.getFileName();
		
		System.out.println("Decorating: " + pfn.getPFN());
		if(archiveAnchorLFN!=null)
		System.out.println("Archive Link: " + archiveAnchorLFN.getFileName());
		
		
		ret +=  "&access=" + type.toString();
		
		String lfn = "/NOLFN";
		
		if(archiveAnchorLFN!=null)
			lfn = archiveAnchorLFN.getCanonicalName();
		else if (lfns!=null && lfns.size()>0)
			lfn = lfns.iterator().next().getCanonicalName();		
		
		ret += "&lfn=" + lfn;
			
		if(archiveAnchorLFN==null){
			ret += "&guid=" + guid.getName() +
			"&size=" + guid.size + "&md5="+ guid.md5;

		} else {
			GUID archiveAnchorGUID = GUIDUtils.getGUID(archiveAnchorLFN.guid);
			ret += "&zguid=" + guid.getName() +
			"&guid=" + archiveAnchorGUID.getName() +
			"&size=" + archiveAnchorGUID.size + "&md5="+ archiveAnchorGUID.md5;

		}
		
		ret += "&se=" + se.getName();
		
		ret = addXURLForSpecialSEs(ret,lfn);
		
		return ret;
	}
	
	
	private String addXURLForSpecialSEs(String ret, String lfn) {

		SE se = SEUtils.getSE(pfn.seNumber);

		// $se =~ /dcache/i
		// $se =~ /alice::((RAL)|(CNAF))::castor/i
		// $se =~ /alice::RAL::castor2_test/i
		if ((se.seName.toLowerCase()).contains("dcache"))
			return ret + "&xurl=" + se.seioDaemons + "/" + lfn;
		return ret;
	}
	
	/**
	 * @param signedEnvelope
	 */
	public void setSignedEnvelope(String signedEnvelope){
		this.signedEnvelope = signedEnvelope;
	}
	
	/**
	 * @return the signed envelope
	 */
	public String getSignedEnvelope(){
		return signedEnvelope;
	}
	
	/**
	 * @param encryptedEnvelope
	 */
	public void setEncryptedEnvelope(String encryptedEnvelope){
		this.encryptedEnvelope = encryptedEnvelope;
	}
	
	/**
	 * @return encrypted envelope
	 */
	public String getEncryptedEnvelope(){
		return encryptedEnvelope;
	}
}
