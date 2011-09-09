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
	private String lfnOrGIUD = "";

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
	private String requireguid = "";

	/**
	 * access request site
	 */
	private String site = "";

	/**
	 * access QoS type requested
	 */
	private String qosType = "";

	/**
	 * access QoS count of replicas requested
	 */
	private int qosCount = 0;
	
	/**
	 * access md5 requested
	 */
	private String md5 = "00000000000000000000000000000000";

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
			if (guid == null) {
				out.printErrln("Not able to retrieve GUID from Catalogue [error in processing].");
				return;
			}
			guid.size = size;
			guid.md5 = md5;
		} else {
			lfn = CatalogueApiUtils.getLFN(lfnOrGIUD, evenIfNotExists);
			if (lfn == null) {
				out.printErrln("Not able to retrieve LFN from Catalogue [error in processing].");
				return;
			}
			if (lfn.guid == null) {
				if ("".equals(requireguid)){
					guid = GUIDUtils.createGuid();
					
				}
				else
					guid = CatalogueApiUtils.getGUID(requireguid,
							evenIfNotExists);
				lfn.guid = guid.guid;
				guid.lfnCache = new LinkedHashSet<LFN>(1);
				guid.lfnCache.add(lfn);
				guid.size = size;
				guid.md5 = md5;
			} else {
				guid = GUIDUtils.getGUID(lfn.guid, evenIfNotExists);
			}
		}

		if (accessRequest == AccessType.WRITE) {
			
			if(ses.isEmpty() && (("").equals(qosType) || (qosCount==0))){
				qosType="disk";
				qosCount=1;
			}

//			if (lfn != null)
//				pfns = CatalogueApiUtils.getPFNsToWrite(commander.user, site,
//						lfn, ses, exxses, qosType, qosCount);
//			else
				if (guid != null)
				pfns = CatalogueApiUtils.getPFNsToWrite(commander.user, site,
						guid, ses, exxses, qosType, qosCount);
			else
				out.printErrln("Not able to get request LFN/GUID [error in processing].");
		} else if (accessRequest == AccessType.READ) {
			if (lfn != null)
				pfns = CatalogueApiUtils.getPFNsToRead(commander.user, site,
						lfn, ses, exxses);
			else if (guid != null)
				pfns = CatalogueApiUtils.getPFNsToRead(commander.user, site,
						guid, ses, exxses);
			else
				out.printErrln("Not able to get request LFN/GUID [error in processing].");
		} else
			out.printErrln("Unknown access type [error in processing].");

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

		String ret = "";
		if (pfns != null && !pfns.isEmpty()) {

			String col = RootPrintWriter.columnseparator;
			String desc = RootPrintWriter.fielddescriptor;
			String sep = RootPrintWriter.fieldseparator;

			for (PFN pfn : pfns) {
				ret += col;

				String envelope = pfn.ticket.envelope.getSignedEnvelope();
				
				if (!"alice::cern::setest".equals(CatalogueApiUtils.getSE(pfn.seNumber)
						.getName().toLowerCase()))
					if (SEUtils.getSE(pfn.seNumber).needsEncryptedEnvelope)
						envelope += "&envelope="
								+ pfn.ticket.envelope.getEncryptedEnvelope();

				final StringTokenizer st = new StringTokenizer(envelope, "&");
				while (st.hasMoreTokens()) {
					String t = st.nextToken();
					String key = t.substring(0, t.indexOf('='));
					String val = t.substring(t.indexOf('=') + 1);

					if (("turl").equals(key)) {
						ret += desc + "url" + sep + val;
						final StringTokenizer tpfn = new StringTokenizer(val,
								"////");
						tpfn.nextToken();
						tpfn.nextToken();
						String ttpfn = "/" + tpfn.nextToken();
						while (tpfn.hasMoreTokens())
							ttpfn += "/" + tpfn.nextToken();

						ret += desc + "pfn" + sep + ttpfn;
					}
					// if(("lfn").equals(key))
					// ret += desc + key + sep +
					// "/alice/cern.ch/user/s/sschrein/whoamI_copyX.jdl";
					// else if(("guid").equals(key))
					// ret += desc + key + sep + val.toUpperCase();
					// else
					ret += desc + key + sep + val;

				}
				ret += desc + "nSEs" + sep
						+ pfns.get(0).getGuid().getPFNs().size();
				ret += desc + "user" + sep + commander.user.getName();
			}
			return ret;
		}
		out.printErrln("We didn't get any envelopes [error in processing].");
		return super.deserializeForRoot();
	}

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

		// defaults, not generally ok, e.g. not for ROOT
		site = commander.site;

		java.util.ListIterator<String> arg = alArguments.listIterator();

		if (arg.hasNext()) {
			String access = arg.next();
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

					if (arg.hasNext()) {
						final StringTokenizer st = new StringTokenizer(
								arg.next(), ";");
						ses = new ArrayList<String>();
						while (st.hasMoreElements()){
							String se = st.nextToken();
							if(!"IGNORE".equals(se) && !"0".equals(se))
								ses.add(se);
						}
					}
					if (arg.hasNext())
						try {
							size = Integer.parseInt(arg.next());
						} catch (NumberFormatException e) {
							// ignore
						}

					if (arg.hasNext()){
						String ssel = arg.next();
						try {
							sesel = Integer.parseInt(ssel);
						} catch (NumberFormatException e) {
							final StringTokenizer st = new StringTokenizer(
									ssel, ";");
							exxses = new ArrayList<String>();
							while (st.hasMoreElements()){
								String ex = st.nextToken();
								if(!"IGNORE".equals(ex) && !"0".equals(ex))
									exxses.add(ex);
							}
						}
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
