package alien.shell.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.StringTokenizer;

import alien.api.catalogue.CatalogueApiUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.se.SEUtils;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandaccess extends JAliEnBaseCommand {

	/**
	 * access request type: read,write,delete...
	 */
	private AccessType accessRequest = AccessType.NULL;

	/**
	 * access request lfnOrGUID
	 */
	private String lfnOrGIUD;

	/**
	 * access request SEs (preferred/constraint SEs)
	 */
	private ArrayList<String> ses = new ArrayList<String>(0);

	/**
	 * access file size requested
	 */
	private int size = 0;

	/**
	 * access request excluded SEs
	 */
	private ArrayList<String> exxses = new ArrayList<String>(0);

	/**
	 * access request SE selector
	 */
	private int sesel = 0;

	/**
	 * access GUID defined for a new LFN specified file on a write request
	 */
	private String requireguid;

	/**
	 * access request site
	 */
	private String site = "";

	/**
	 * access QoS type requested
	 */
	private String qosType;

	/**
	 * access QoS count of replicas requested
	 */
	private int qosCount;

	/**
	 * return pfns;
	 */
	private List<PFN> pfns = null;

	/**
	 * execute the access
	 */
	public void execute() {

		LFN lfn = null;
		GUID guid = null;
		boolean evenIfNotExists = false;

		if (accessRequest.equals(AccessType.WRITE))
			evenIfNotExists = true;

		if (GUIDUtils.isValidGUID(lfnOrGIUD)) {
			guid = CatalogueApiUtils.getGUID(lfnOrGIUD, evenIfNotExists);
			if(guid == null){
				out.printErrln("Not able to retrieve GUID from Catalogue [error in processing].");
				return;
			}
		} else {
			lfn = CatalogueApiUtils.getLFN(lfnOrGIUD, evenIfNotExists);
			if(lfn == null){
				out.printErrln("Not able to retrieve LFN from Catalogue [error in processing].");
				return;
			}
			if (lfn.guid == null) {
				if ("".equals(requireguid))
					guid = GUIDUtils.createGuid();
				else
					guid = CatalogueApiUtils.getGUID(requireguid, true);
				lfn.guid = guid.guid;
				guid.lfnCache = new LinkedHashSet<LFN>(1);
				guid.lfnCache.add(lfn);
				guid.size = size;
			} else {
				guid = GUIDUtils.getGUID(lfn.guid, evenIfNotExists);
			}
		}
		
		System.out.println("LFN/GUID prepared, let's ask for access.");

		if (accessRequest == AccessType.WRITE) {
			if (lfn != null)
				pfns = CatalogueApiUtils.getPFNsToWrite(commander.user, site,
						lfn, ses, exxses, qosType, qosCount);
			else if (guid != null)
				pfns = CatalogueApiUtils.getPFNsToWrite(commander.user, site,
						guid, ses, exxses, qosType, qosCount);
			else
				return;
		}
		else if (accessRequest == AccessType.READ) {
			if (lfn != null){
				System.out.println("Going to ask for user: " + commander.user.getName());
				System.out.println("Going to ask for site: " + site);
				System.out.println("Going to ask for lfn: " + lfn);
				System.out.println("Going to ask for ses: " + ses);
				System.out.println("Going to ask for exxses: " + exxses);
				pfns = CatalogueApiUtils.getPFNsToRead(commander.user, site,
						lfn, ses, exxses);
			}
			else if (guid != null)
				pfns = CatalogueApiUtils.getPFNsToRead(commander.user, site,
						guid, ses, exxses);
			else{
				out.printErrln("Not able to get envelopes [error in processing].");
			}
		}
		else {
			out.printErrln("Unknown access type [error in processing].");
		}
		
		if (out.isRootPrinter())
			out.setReturnArgs(deserializeForRoot());
	}

	/**
	 * printout the help info
	 */
	public void printHelp() {
		// ignore
	}

	/**
	 * get cannot run without arguments
	 * 
	 * @return <code>false</code>
	 */
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * set command's silence trigger
	 */
	public void silent() {
		// ignore
	}

	/**
	 * serialize return values for gapi/root
	 * 
	 * @return serialized return
	 */
	public String deserializeForRoot() {

		System.out.println("Preparing ROOT encoding...");

		String ret = "";
		if (pfns!= null && !pfns.isEmpty()) {
			
			String col = RootPrintWriter.columnseparator;
			String desc = RootPrintWriter.fielddescriptor;
			String sep = RootPrintWriter.fieldseparator;

//			String col = "<columnseparator>";
//			String desc = "<fielddescriptor>";
//			String sep = "<fieldseparator>";

			System.out.println("Let's see the envelopes we have ...");
			
			for (PFN pfn : pfns) {
				ret += col;
				
				String envelope = pfn.ticket.envelope.getSignedEnvelope();
				if (!"alice::cern::setest".equals(SEUtils.getSE(pfn.seNumber).getName().toLowerCase()))
					if (SEUtils.getSE(pfn.seNumber).needsEncryptedEnvelope)
						envelope += "&envelope="
								+ pfn.ticket.envelope.getEncryptedEnvelope();
				
				System.out.println("encrypted envelope is: " + pfn.ticket.envelope.getEncryptedEnvelope());

				final StringTokenizer st = new StringTokenizer(
						envelope, "&");
				while(st.hasMoreTokens()){
					String t = st.nextToken();
					String key = t.substring(0, t.indexOf('='));
					String val = t.substring(t.indexOf('=')+1);
					
					if(("turl").equals(key)){
						ret += desc + "url" + sep + val;
						final StringTokenizer tpfn = new StringTokenizer(
								val, "////");
						tpfn.nextToken();
						tpfn.nextToken();
						String ttpfn = "/" + tpfn.nextToken();
						while(tpfn.hasMoreTokens())
							ttpfn += "/" + tpfn.nextToken();
						
						ret += desc + "pfn" + sep + ttpfn;
					}
					if(("lfn").equals(key))
						ret += desc + key + sep + "/alice/cern.ch/user/s/sschrein/whoamI_copyX.jdl";
					else if(("guid").equals(key))
						ret += desc + key + sep + val.toUpperCase();
					else 
						ret += desc + key + sep + val;
					
				}
				ret += desc + "nSEs" + sep + pfns.get(0).getGuid().getPFNs().size();
				ret += desc + "user" + sep + commander.user.getName();
			}
			return ret;
		}
		return super.deserializeForRoot();
	}

	//
	// <stdoutindicator><columnseparator><fieldseparator>Aug 24 18:20:13 info
	// Authorize: STARTING envelope creation: -z read
	// /alice/simulation/2008/v4-15-Release/Ideal/FMD/Align/Data/Run0_999999999_v1_s0.root
	// 0 0 0 NIHAM
	// <columnseparator>.<fieldseparator>Aug 24 18:20:13 info Nothing from
	// cache, going in: sesel=1,ses=
	// <columnseparator>.<fieldseparator>Aug 24 18:20:13 info whereis said:
	// ALICE::CERN::SE ALICE::CNAF::SE ALICE::FZK::SE ALICE::Legnaro::SE
	// Alice::NIHAM::FILE ALICE::KFKI::SE
	// <columnseparator>.<fieldseparator>Aug 24 18:20:13 info sql query: SELECT
	// seName from (SELECT DISTINCT b.seName as seName, a.rank FROM SERanks a
	// right JOIN SE b on (a.seNumber=b.seNumber and a.sitename=?) WHERE
	// (b.seExclusiveRead is NULL or b.seExclusiveRead = '' or b.seExclusiveRead
	// LIKE concat ('%,' , concat(? , ',%')) ) and upper(b.seName)=upper(?) or
	// upper(b.seName)=upper(?) or upper(b.seName)=upper(?) or
	// upper(b.seName)=upper(?) or upper(b.seName)=upper(?) or
	// upper(b.seName)=upper(?) ORDER BY coalesce(a.rank,1000) ASC ) d;, values:
	// CERN sschrein ALICE::CERN::SE ALICE::CNAF::SE ALICE::FZK::SE
	// ALICE::Legnaro::SE Alice::NIHAM::FILE ALICE::KFKI::SE
	// <columnseparator>.<fieldseparator>Aug 24 18:20:13 info We sorted inside
	// ses<>0^sesel>1, outcome:
	// sesel=1,ses=ALICE::CERN::SE,nSEs=6,sorted=ALICE::CERN::SE,ALICE::CNAF::SE,ALICE::KFKI::SE,ALICE::Legnaro::SE,Alice::NIHAM::FILE,ALICE::FZK::SE
	
	// <stderrindicator><columnseparator><fieldseparator>

	// <outputindicator>
	// <columnseparator><fielddescriptor>hashord
	// <fieldseparator>turl-access-lfn-size-se-guid-md5-user-issuer-issued-expires-hashord
	// <fielddescriptor>issuer<fieldseparator>Authen.v2-19.112
	// <fielddescriptor>lfn<fieldseparator>/alice/simulation/2008/v4-15-Release/Ideal/FMD/Align/Data/Run0_999999999_v1_s0.root
	// <fielddescriptor>pfn<fieldseparator>/03/54199/d04cfa3a-801c-11dd-ad0f-001e0b4e02fc
	// <fielddescriptor>size<fieldseparator>4139
	// <fielddescriptor>user<fieldseparator>sschrein
	// <fielddescriptor>url<fieldseparator>root://pcaliense04.cern.ch:1095//03/54199/d04cfa3a-801c-11dd-ad0f-001e0b4e02fc
	// <fielddescriptor>envelope<fieldseparator>-----BEGIN
	// SEALED CIPHER-----
	// EBVdGI6ulcJR6fqDMqhUb5xf0bkC0WsJChx4Q7vIGcC8CZqMuP+bpXLXIBjLI+IEg0HSU+Tcnoqu
	// 3Y+YnckQo4yC0EtysscaD9lROWcWQoJpzd0Mnem+X6hzGePnLzK1Ozp5L6ZQtFeGWxdcfVTxty0+
	// 6E8RP49LYNwow8HX31c=
	// -----END SEALED CIPHER-----
	// -----BEGIN SEALED ENVELOPE-----
	// AAAAgHq+cg-wxP3RLZOCkYReUZAEPjCBzlSRkx-rJK01WO6pDrvUpuBWDyvlTJfYbJv-WHXZpB4x
	// jWvsuPXqhUw49fmDl3iSXUPLGMBhckfbheY75yRN1bFTlqYfpM8+mS3U73-5sJYn9e6GgFbXo089
	// 8t+2HchICIrBIiRaDmaS1H0TwIAEWosqEkGj6dP+vq7b0P4kdrgNOL-Hkf9AGBftF9Wr2-J-eanQ
	// LrTCyPsqelcYPX6bK8tKyiLbgix2G5-N+XryJMY6p-oO6Fg5IG-RhUVFk6tOhf2g5uQori-PICnM
	// Pl4zDgJWHrxM+Vq6E0hX80BN646ubgKOq5qIP2+I5YyTp8BFuRxchIABzkVu8WqUEjG8o0DPqXUd
	// +4umnp9OYxDYkJJ6AS2Z2rw47WhZVGwDgKuPc6uDQu1lBkCU8HZm4GiM2lC+DEq9H+BenQ2AiqEy
	// flQGweFbzATLvs2ZZS2LOhqlJi6cMTh9FNSBIyzxKlTyWqQCBqq0jkbGZHzhsytVzLGnSe1NrIQM
	// p+vlrC6Hhv1eUv9SJH3nGs9mXJr8uiNE+9OgzXZzuzyf7fXOoAbECUASGM6ul2AjQ8QB01EkGJGU
	// 1oJnvsbrEbdxg3oeZgSbjv9whjDy5op2HfyjMCSoCthkszcwvBMr2YrtEj5NWDNTnLafBbi1KwfF
	// z8gNxxqihkQJTZ0fZITkmZkvAYuf4O66m4nPQ2cDSWMY2JAJ6DAn3aMz6Xv2rTOgLcr4zGC2t-xB
	// CHV12PiVhtG6CZL0kqfzEoJF6nMsslwr-HNBH6LK6vdrrB0S9MnvY4vlxrRCbcTbHUGBK63F61aM
	// Bscn3fLpEdxzllRNDVR91I+6Ly-aBYIyhM7WBkPOLg5VfrOUEsxt6IukE4q8fzCMgl7k1epWMzZi
	// MZRlFk6rhI1f3ebtBwsyW31e99Pny86zsQD8DBxeklO1eCdCZrB9H3IPEyfrIsZUAGbjaDY0tsB+
	// apQGJVjyBOYslWkShmD1zI59FeoP5sfWi0Dydms5BcYssbjkajpjMFaF7PaNaZMvkzZnQXs-nVOs
	// XU-vMxotkTlgSvekyEw=
	// -----END SEALED ENVELOPE-----
	// <fielddescriptor>guid<fieldseparator>D04CFA3A-801C-11DD-AD0F-001E0B4E02FC
	// <fielddescriptor>access<fieldseparator>read
	// <fielddescriptor>nSEs<fieldseparator>6
	// <fielddescriptor>se<fieldseparator>ALICE::CERN::SE
	// <fielddescriptor>signature<fieldseparator>i0/bETKqxQdZg4is3NhWOUO1BQjvAFT+4a15P5J9A8L4htvYqOE/Z9yV2/O+VhmBQblYo4+0mg06ulEZk0MzAZABCnL9j0sRFtHDHd3/vbmtZrVX37+SoU4LbaXjJvnhuRG4847/rvwTCccvRmRus2E2X8hHF51Ds/Va0D3gTcM=
	//<fielddescriptor>issued<fieldseparator>1314200567
	//<fielddescriptor>md5<fieldseparator>0
	//<fielddescriptor>expires<fieldseparator>1314286967
	//<outputterminator><columnseparator>
	//<fielddescriptor>pwd<fieldseparator>/alice/cern.ch/user/s/sschrein/<streamend>
	//

	/**
	 * Constructor needed for the command factory in commander
	 * 
	 * @param commander
	 * @param out
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandaccess(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) {
		super(commander, out, alArguments);

		// defaults
		site = commander.site;

		java.util.ListIterator<String> arg = alArguments.listIterator();

		// my $options = shift;
		// my $access = (shift || 0);
		// my $lfn = (shift || 0);
		// my $se = (shift || "");
		// my $size = (shift || 0);
		// my $sesel = (shift || 1);
		// my @accessOptions = @_;
		// my $extguid = (shift || 0);
		// my $sitename = (shift || 0);
		// my $writeQos = (shift || 0);

		if (arg.hasNext()) {
			String access = arg.next();
			System.out.println("Getting access: " + access);

			if (access.startsWith("write")) {
				accessRequest = AccessType.WRITE;
			} else if (access.equals("read")) {
				accessRequest = AccessType.READ;
			} else if (access.equals("delete")) {
				accessRequest = AccessType.DELETE;
			}

			if (!accessRequest.equals(AccessType.NULL)) {
				
				if (arg.hasNext()) {
					lfnOrGIUD = arg.next();
					System.out.println("Getting lfnOrGUID: " + lfnOrGIUD);

					if (arg.hasNext()) {
						final StringTokenizer st = new StringTokenizer(
								arg.next(), ";");
						ses = new ArrayList<String>(st.countTokens());
						while (st.hasMoreElements())
							ses.add(st.nextToken());
					}
					if (arg.hasNext())
						try {
							size = Integer.parseInt(arg.next());
						} catch (NumberFormatException e) {
							// ignore
						}

					if (arg.hasNext())
						try {
							sesel = Integer.parseInt(arg.next());
						} catch (NumberFormatException e) {
							final StringTokenizer st = new StringTokenizer(
									arg.next(), ";");
							exxses = new ArrayList<String>(st.countTokens());
							while (st.hasMoreElements())
								exxses.add(st.nextToken());
						}

					if (arg.hasNext()) {
						String rguid = arg.next();
						if (GUIDUtils.isValidGUID(rguid))
							requireguid = rguid;
					}

					if (arg.hasNext())
						site = arg.next();

					if (arg.hasNext()) {

						final StringTokenizer st = new StringTokenizer(
								arg.next(), "=");
						if (st.countTokens() == 2) {
							qosType = st.nextToken();
							try {
								qosCount = Integer.parseInt(st.nextToken());

							} catch (NumberFormatException e) {
								qosType = "";
							}
						}

					}
				} else
					out.printErrln("No LFN or GUID specified [error in request].");
			} else
				out.printErrln("Illegal Request type specified [error in request].");

		} else
			out.printErrln("No Request type specified [error in request].");

	}
}
