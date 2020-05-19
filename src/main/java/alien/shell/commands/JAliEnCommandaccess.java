package alien.shell.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.logging.Level;

import alien.catalogue.CatalogEntity;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.XrootDEnvelope;
import alien.se.SE;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

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
	private String lfnName = "";

	/**
	 * access request site
	 */

	private int referenceCount = 0;

	private final List<String> ses = new ArrayList<>();
	private final List<String> exses = new ArrayList<>();

	private final HashMap<String, Integer> qos = new HashMap<>();

	/**
	 * return pfns;
	 */
	private List<PFN> pfns = null;

	/**
	 * For write envelopes the size, if known, can be embedded in the booking request
	 */
	private long size = -1;

	/**
	 * For write envelopes the MD5 checksum, if known, can be embedded in the booking request
	 */
	private String md5sum = null;

	/**
	 * For write envelopes you can also pass the job ID, when applicable
	 */
	private long jobId = -1;

	/**
	 * execute the access
	 */
	@Override
	public void run() {
		boolean evenIfNotExists = false;

		if (accessRequest.equals(AccessType.WRITE)) {
			logger.log(Level.INFO, "Access called for a write operation");
			evenIfNotExists = true;
		}

		// obtaining LFN information for read or a new LFN for write
		final LFN lfn = lfnName.startsWith("/") ? commander.c_api.getLFN(lfnName, evenIfNotExists) : null;

		final CatalogEntity referenceEntity;

		if (lfn == null) {
			GUID referenceGUID = null;

			if (accessRequest == AccessType.READ && GUIDUtils.isValidGUID(lfnName)) {
				referenceGUID = commander.c_api.getGUID(lfnName);
			}

			if (referenceGUID == null) {
				logger.log(Level.INFO, "Not able to retrieve LFN from catalogue");
				commander.setReturnCode(ErrNo.ENOENT, lfnName);
				return;
			}

			referenceEntity = referenceGUID;
		}
		else
			referenceEntity = lfn;

		if (accessRequest == AccessType.WRITE) {
			final GUID guid;
			if (!lfn.exists || lfn.guid == null) {
				guid = GUIDUtils.createGuid(commander.user);

				if (size >= 0)
					guid.size = size;

				if (md5sum != null)
					guid.md5 = md5sum;

				if (jobId >= 0)
					lfn.jobid = jobId;

				lfn.guid = guid.guid;
				lfn.size = guid.size;
				lfn.md5 = guid.md5;
			}
			else {
				// check if the details match the existing entry, if they were provided
				if ((size >= 0 && lfn.size != size) || (md5sum != null && lfn.md5 != null && !md5sum.equalsIgnoreCase(lfn.md5)) || (jobId >= 0 && lfn.jobid >= 0 && jobId != lfn.jobid)) {
					commander.setReturnCode(ErrNo.EINVAL, "You seem to want to write a different file from the existing one in the catalogue");
					return;
				}

				guid = commander.c_api.getGUID(lfn.guid.toString(), evenIfNotExists, false);

				if (guid == null) {
					commander.setReturnCode(ErrNo.EUCLEAN, "Could not retrieve the GUID entry for the existing file");
					return;
				}

				if (md5sum != null) {
					// could the existing entries be enhanced?
					if (lfn.md5 == null)
						lfn.md5 = md5sum;

					if (guid.md5 == null)
						guid.md5 = md5sum;
				}

				if (jobId >= 0 && lfn.jobid < 0)
					lfn.jobid = jobId;
			}

			guid.addKnownLFN(lfn);

			pfns = commander.c_api.getPFNsToWrite(lfn, guid, ses, exses, qos);
		}
		else if (accessRequest == AccessType.READ) {
			logger.log(Level.INFO, "Access called for a read operation");
			pfns = commander.c_api.getPFNsToRead(referenceEntity, ses, exses);
		}
		else {
			logger.log(Level.SEVERE, "Unknown access type");
			commander.setReturnCode(ErrNo.EINVAL, accessRequest.toString());
			return;
		}

		if (pfns == null || pfns.isEmpty()) {
			logger.log(Level.SEVERE, "No PFNs for this LFN");
			commander.setReturnCode(ErrNo.EBADFD, "No PFNs for this LFN");
			return;
		}

		for (final PFN pfn : pfns) {
			commander.outNextResult();
			commander.printOutln(pfn.pfn);
			final SE se = commander.c_api.getSE(pfn.seNumber);

			if (se != null) {
				commander.printOutln("SE: " + se.seName + " (" + (se.needsEncryptedEnvelope ? "needs" : "doesn't need") + " encrypted envelopes)");

				if (pfn.ticket != null) {
					final XrootDEnvelope env = pfn.ticket.envelope;

					if (!"alice::cern::setest".equals(se.getName().toLowerCase()))
						if (se.needsEncryptedEnvelope) {
							commander.printOut("envelope", env.getEncryptedEnvelope());
							commander.printOutln("Encrypted envelope:\n" + env.getEncryptedEnvelope());
						}
						else {
							commander.printOut("envelope", env.getSignedEnvelope());
							commander.printOutln("Signed envelope:\n" + env.getSignedEnvelope());
						}

					// If archive member access requested, add it's filename as anchor
					if (pfn.ticket.envelope.getArchiveAnchor() != null) {
						commander.printOut("url", pfn.ticket.envelope.getTransactionURL() + "#" + pfn.ticket.envelope.getArchiveAnchor().getFileName());
					}
					else {
						commander.printOut("url", pfn.ticket.envelope.getTransactionURL());
					}

					commander.printOut("guid", pfn.getGuid().getName());
					commander.printOut("se", pfn.getSE().getName());
					commander.printOut("tags", pfn.getSE().qos.toString());
					commander.printOut("nSEs", String.valueOf(pfns.size()));
					commander.printOut("md5", referenceEntity.getMD5());
					commander.printOut("size", String.valueOf(referenceEntity.getSize()));
					commander.printOutln();
				}
			}
		}
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("access", "[options] <read|write> <lfn> [<specs>]"));
		commander.printOutln(helpOption("-s", "for write requests, size of the file to be uploaded, when known"));
		commander.printOutln(helpOption("-m", "for write requests, MD5 checksum of the file to be uploaded, when known"));
		commander.printOutln(helpOption("-j", "for write requests, the job ID that created these files, when applicable"));
		commander.printOutln();
	}

	/**
	 * get cannot run without arguments
	 *
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * serialize return values for gapi/root
	 *
	 * @return serialized return
	 */

	/**
	 * Constructor needed for the command factory in commander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandaccess(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);

		try {
			final OptionParser parser = new OptionParser();

			parser.accepts("s").withRequiredArg().ofType(Long.class);
			parser.accepts("j").withRequiredArg().ofType(Long.class);
			parser.accepts("m").withRequiredArg();

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			final java.util.ListIterator<String> arg = optionToString(options.nonOptionArguments()).listIterator();

			if (arg.hasNext()) {
				final String access = arg.next();
				logger.log(Level.INFO, "Access = " + access);
				if (access.startsWith("write")) {
					logger.log(Level.INFO, "We got write accesss");
					accessRequest = AccessType.WRITE;
				}
				else if (access.equals("read")) {
					logger.log(Level.INFO, "We got read accesss");
					accessRequest = AccessType.READ;
				}
				else
					logger.log(Level.INFO, "We got unknown accesss request: " + access);

				if (!accessRequest.equals(AccessType.NULL) && (arg.hasNext())) {
					lfnName = arg.next();

					if (arg.hasNext()) {
						final StringTokenizer st = new StringTokenizer(arg.next(), ",");
						while (st.hasMoreElements()) {

							final String spec = st.nextToken();
							if (spec.contains("::")) {
								if (spec.indexOf("::") != spec.lastIndexOf("::"))
									if (spec.startsWith("!")) // an exSE spec
										exses.add(spec.toUpperCase().substring(1));
									else {// an SE spec
										ses.add(spec.toUpperCase());
										referenceCount++;
									}
							}
							else if (spec.contains(":"))
								try {
									final int c = Integer.parseInt(spec.substring(spec.indexOf(':') + 1));
									if (c > 0) {
										qos.put(spec.substring(0, spec.indexOf(':')), Integer.valueOf(c));
										referenceCount = referenceCount + c;
									}
									else
										throw new JAliEnCommandException("The number replicas has to be stricly positive");

								}
								catch (final Exception e) {
									throw new JAliEnCommandException("Exception parsing the QoS string", e);
								}
							else if (!spec.equals(""))
								throw new JAliEnCommandException();
						}
					}
				}
				else
					commander.setReturnCode(ErrNo.EINVAL, "Invalid access type requested: " + access);
			}

			if (options.has("s"))
				size = ((Long) options.valueOf("s")).longValue();

			if (options.has("m"))
				md5sum = options.valueOf("m").toString();

			if (options.has("j"))
				jobId = ((Long) options.valueOf("j")).longValue();
		}
		catch (final OptionException e) {
			printHelp();
			throw e;
		}
	}
}
