package alien.perl.commands;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import lazyj.Log;

import alien.catalogue.BookingTable;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.catalogue.access.XrootDEnvelope;
import alien.catalogue.access.XrootDEnvelopeReply;
import alien.io.xrootd.envelopes.XrootDEnvelopeSigner;
import alien.se.SE;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;
import alien.user.UserFactory;

/**
 * @author Alina Grigoras
 * @since May 25, 2011
 * implements AliEn "authorize" command 
 * */
public class AlienCommandauthorize extends AlienCommand {
	/**
	 * @param p AliEn principal received from the https request
	 * @param al SOAP arguments received from the http request 
	 * @throws Exception
	 */
	public AlienCommandauthorize(final AliEnPrincipal p, final ArrayList<Object> al) throws Exception {
		super(p, al);
	}

	/**
	 * @param p AliEn principal received from the https request
	 * @param sUsername username received from SOAP request. It can be a different user than the user received through the AliEn principal (the user can su into a different user)
	 * @param sCurrentDirectory current directory when the command was issued 
	 * @param sCommand the command requested by the user
	 * @param iDebugLevel 
	 * @param alArguments the arguments to the commnad, can be null or empty
	 * @throws Exception
	 */
	public AlienCommandauthorize (final AliEnPrincipal p, final String sUsername, final String sCurrentDirectory, final String sCommand, final int iDebugLevel, final List<?> alArguments) throws Exception {
		super(p, sUsername, sCurrentDirectory, sCommand, iDebugLevel,alArguments);
	}

	/**
	 * execute authorize command <br>
	 * the list of arguments 
	 * 		<ul>
	 * 			<li>first argument must contain the access string: 
	 * 				write/read/mirror/register/delete/registerenvs 
	 * 			</li>
	 * 			<li>second argument can be an array list if access is "registerenvs" or a map for the rest </li>
	 * 			<li>third arguments appears only in the case of map and it is the job id </li>
	 * 		</ul>
	 * @return 	 the response is a map with the keys: 
	 * 		<ul>
	 * 			<li>rcvalues - the actual values used by the command </li> 
	 * 			<li>rcmessages - the printed logs </li>
	 * 		</ul>
	 */
	@Override
	public HashMap<String, ArrayList<String>> executeCommand() throws Exception{
		Log.log(Log.FINER, "Entering authorize command");
		
		HashMap<String, ArrayList<String>> hmReturn = new HashMap<String, ArrayList<String>>();

		ArrayList<String> alrcValues;
		ArrayList<String> alrcMessages = new ArrayList<String>();
		
		boolean bDebug = false;

		alrcMessages.add("This is just a simple log\n");

		//we need to have at least 2 parameters
		if(this.alArguments != null && this.alArguments.size() >= 2){
			//first argument must be access string
			String sAccess = (String) this.alArguments.get(0);
			Log.log(Log.FINER, "Authorize access = "+sAccess);
			
			int iJobId = 0;
			int envelopeCount = this.alArguments.size()-1;

			if(this.alArguments.size() >= 2){
				try {
					if(this.alArguments.get(this.alArguments.size()-1) instanceof String){
						iJobId = Integer.parseInt((String) this.alArguments.get(this.alArguments.size()-1));
						envelopeCount--;
					}
				} 
				catch(NumberFormatException e){
					// nothing to do, then the jobID is just not set by Perl ...
				}
			}
			
			Log.log(Log.FINER, "Authorize Job id = "+iJobId);

			if("registerenvs".equals(sAccess)){
				ArrayList<String> alInfo = new ArrayList<String>(this.alArguments.size());
				for(int i=1; i<= envelopeCount;i++)
					alInfo.add((String) this.alArguments.get(i));
			
				alrcValues = (ArrayList<String>) registerEnvelope(this.pAlienUser, this.sUsername, this.sCurrentDirectory , sAccess, alInfo, iJobId,this.iDebug);
		
			}
			else{
				@SuppressWarnings("unchecked")
				HashMap<String, String> hmInfo = (HashMap<String, String>) this.alArguments.get(1);
				

				alrcValues = (ArrayList<String>) authorizeEnvelope(this.pAlienUser, this.sUsername, this.sCurrentDirectory , sAccess, hmInfo, iJobId, this.iDebug);
				alrcMessages.addAll(alrcValues);
			}

		}
		else{
			throw new Exception("Invalid authorize command arguments");		
		}

		if(!bDebug) alrcMessages.clear();

		hmReturn.put("rcvalues", alrcValues);
		hmReturn.put("rcmessages", alrcMessages);

		Log.log(Log.FINER, "Existing authorize command");
		
		return hmReturn;
	}

	

	/**
	 * @param user 
	 * @param p_user
	 * @param p_dir
	 * @param access
	 * @param envelopes
	 * @param jobid 
	 * @param debugLevel 
	 * @return the list of envelopes
	 */
	public static List<String> registerEnvelope(final AliEnPrincipal user, final String p_user,
			final String p_dir, final String access, final ArrayList<String> envelopes,
			final int jobid, final int debugLevel) {

		AliEnPrincipal effectiveUser = user;
		
		if (effectiveUser.canBecome(p_user))
			effectiveUser= UserFactory.getByUsername(p_user);
		else {
			System.err.println("You [" + effectiveUser.getName()
					+ "] have not the rights to become " + p_user);
			return null;
		}

		ArrayList<String> retenv = new ArrayList<String>(envelopes.size());

		for (String env : envelopes) {
			
			System.out.println("We received an envelope for registration: " + env);

			try {
				
				if (XrootDEnvelopeSigner.verifyEnvelope(env, true)) {
					XrootDEnvelope xenv = new XrootDEnvelope(env);
					System.out.println("Self Signature VERIFIED! : "
							+ xenv.pfn.pfn);
					if (BookingTable.commit(effectiveUser,
							BookingTable.getBookedPFN(xenv.pfn.pfn))) {
						System.out.println("Successfully moved " + xenv.pfn.pfn
								+ " to the Catalogue");

						retenv.add(env);
					}

				} else if (XrootDEnvelopeSigner.verifyEnvelope(env, false)) {
					XrootDEnvelopeReply xenv = new XrootDEnvelopeReply(env);
					System.out.println("SE Signature VERIFIED! : "
							+ xenv.pfn.pfn);
					if (BookingTable.commit(effectiveUser,
							BookingTable.getBookedPFN(xenv.pfn.pfn))) {
						System.out.println("Successfully moved " + xenv.pfn.pfn
								+ " to the Catalogue");
						retenv.add(env);
					}

				} else {
					System.out.println("COULD NOT VERIFY ANY SIGNATURE!");
				}

			} catch (SignatureException e) {
				System.err.println("Sorry ... Could not sign the envelope!");
			} catch (InvalidKeyException e) {
				System.err.println("Sorry ... Could not sign the envelope!");
			} catch (NoSuchAlgorithmException e) {
				System.err.println("Sorry ... Could not sign the envelope!");
			} catch (IOException e) {
				System.err.println("Sorry ... Error getting the PFN!");
			}
		}

		return retenv;
	}

	/**
	 * @param user
	 * @param p_user
	 * @param p_dir
	 * @param access
	 * @param optionHash
	 * @param jobid 
	 * @param debugLevel 
	 * @return the list of envelopes
	 */
	public static List<String> authorizeEnvelope(final AliEnPrincipal user, final String p_user,
			final String p_dir, final String access, final Map<String, String> optionHash,
			final int jobid, final int debugLevel) {

		AliEnPrincipal effectiveUser = user;
		
		System.out.println();
		System.out.println("JAuthen SOAP request for authorize...");

		boolean evenIfNotExists = false;
		if (effectiveUser.canBecome(p_user))
			effectiveUser = UserFactory.getByUsername(p_user);
		else {
			System.err.println("You [" + effectiveUser.getName()
					+ "] have not the rights to become " + p_user);
			return null;
		}

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

		int p_size = Integer.parseInt(sanitizePerlString(
				optionHash.get("size"), true));
		int p_qosCount = Integer.parseInt(sanitizePerlString(
				optionHash.get("writeQosCount"), true));

		String p_lfn = sanitizePerlString(optionHash.get("lfn"), false);
		if (!p_lfn.startsWith("/"))
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
			if (se != null) {
				ses.add(se);
				System.out.println("An SE found: " + se.getName());
			}
		}

		String[] splitExcludeSE = sanitizePerlString(
				optionHash.get("excludeSE"), false).split(";");
		List<SE> exxSes = new ArrayList<SE>(splitExcludeSE.length);
		for (String sename : Arrays.asList(splitExcludeSE)) {
			SE se = SEUtils.getSE(sename);
			if (se != null) {
				exxSes.add(se);
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
				+ "\nqosCount: " + p_qosCount + "\nsitename: " + p_site
				+ "\nSEs: " + optionHash.get("wishedSE") + "\nexSEs: "
				+ optionHash.get("excludeSE") + "\n...\n");

		LFN lfn = null;
		GUID guid = null;
		if (GUIDUtils.isValidGUID(p_lfn)) {
			guid = GUIDUtils.getGUID(UUID.fromString(p_lfn), evenIfNotExists);
		} else {
			lfn = LFNUtils.getLFN(p_lfn, evenIfNotExists);
			if (lfn.guid == null) {
				if ("".equals(p_guidrequest))
					guid = GUIDUtils.createGuid();
				else
					guid = GUIDUtils.getGUID(UUID.fromString(p_guidrequest),
							true);
				lfn.guid = guid.guid;
				guid.lfnCache = new LinkedHashSet<LFN>(1);
				guid.lfnCache.add(lfn);
				guid.size = p_size;
				guid.md5 = p_md5;
			} else {
				guid = GUIDUtils.getGUID(lfn.guid, evenIfNotExists);
			}
		}

		List<PFN> pfns = new ArrayList<PFN>(ses.size() + p_qosCount);

		LFN setArchiveAnchor = null;

		try {
			if (accessRequest == AccessType.WRITE) {

				// statis list of specified SEs
				for (SE se : ses) {
					System.out.println("Trying to book writing on static SE: "
							+ se.getName());

					if (!se.canWrite(effectiveUser)) {
						System.err
								.println("You are not allowed to write to this SE.");
						continue;
					}

					try {
						pfns.add(BookingTable.bookForWriting(effectiveUser, lfn, guid,
								null, jobid, se));
					} catch (Exception e) {
						System.out.println("Error for the request on "
								+ se.getName() + ", message: " + e);
					}
				}

				if (p_qosCount > 0) {
					ses.addAll(exxSes);
					List<SE> SEs = SEUtils.getClosestSEs(p_site, ses, true);
					final Iterator<SE> it = SEs.iterator();

					int counter = 0;
					while (counter < p_qosCount && it.hasNext()) {
						SE se = it.next();

						if (!se.canWrite(effectiveUser))
							continue;

						System.out
								.println("Trying to book writing on discoverd SE: "
										+ se.getName());
						try {
							pfns.add(BookingTable.bookForWriting(effectiveUser, lfn,
									guid, null, jobid, se));
						} catch (Exception e) {
							System.out.println("Error for the request on "
									+ se.getName() + ", message: " + e);
							continue;
						}
						counter++;
					}

				}
			}
			if (accessRequest == AccessType.READ) {

				PFN readpfn = null;

				pfns = SEUtils.sortBySiteSpecifySEs(guid.getPFNs(), p_site, true, ses, exxSes, false);

				for (PFN pfn : pfns) {
					System.err.println(pfn);
					System.out.println("Asking read for " + effectiveUser.getName() + " to " + pfn.getPFN());
					String reason = AuthorizationFactory.fillAccess(effectiveUser, pfn, AccessType.READ);

					if (reason != null) {
						System.err.println("Access refused because: " + reason);
						continue;
					}
					UUID archiveLinkedTo = pfn.retrieveArchiveLinkedGUID();
					if (archiveLinkedTo != null) {
						GUID archiveguid = GUIDUtils.getGUID(archiveLinkedTo,
								false);
						setArchiveAnchor = lfn;
						List<PFN> apfns = SEUtils.sortBySiteSpecifySEs(GUIDUtils.getGUID(pfn.retrieveArchiveLinkedGUID()).getPFNs(), p_site, true, ses, exxSes, false);
						if (!AuthorizationChecker.canRead(archiveguid, effectiveUser)) {
							System.err
									.println("Access refused because: Not allowed to read sub-archive");
							continue;
						}

						for (PFN apfn : apfns) {
							reason = AuthorizationFactory.fillAccess(effectiveUser, apfn, AccessType.READ);

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
				try {
					// we need to both encrypt and sign, the later is not
					// automatic
					XrootDEnvelopeSigner.signEnvelope(pfn.ticket.envelope);
				} catch (SignatureException e) {
					System.err
							.println("Sorry ... Could not sign the envelope!");
				} catch (InvalidKeyException e) {
					System.err
							.println("Sorry ... Could not sign the envelope!");
				} catch (NoSuchAlgorithmException e) {
					System.err
							.println("Sorry ... Could not sign the envelope!");
				}
				String addEnv = pfn.ticket.envelope.getSignedEnvelope().replace("&", "\\&");

				// drop the following once LDAP schema is updated and version
				// number properly on
				if (!"alice::cern::setest".equals(SEUtils.getSE(pfn.seNumber).getName().toLowerCase())) {
					if (SEUtils.getSE(pfn.seNumber).needsEncryptedEnvelope) {
						addEnv += "\\&oldEnvelope="
								+ pfn.ticket.envelope.getEncryptedEnvelope();
						System.out.println("Creating ticket (encrypted): "
								+ pfn.ticket.envelope.getUnEncryptedEnvelope());
					}
				}
				envelopes.add(addEnv);

			}
		}

		return envelopes;

	}

	/**
	 * @param maybeNull
	 * @param isInteger
	 * @return a non-null value, defaulting to "0" if isInteger
	 */
	public static String sanitizePerlString(String maybeNull, boolean isInteger) {
		if (maybeNull == null) {
			if (isInteger)
				return "0";
			return "";
		} else if (("0".equals(maybeNull) && !isInteger))
			return "";
		return maybeNull;
	}

	
	
}
