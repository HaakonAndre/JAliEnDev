package alien.shell.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUIDUtils;

/**
 * @author psvirin
 * 
 */
public class JAliEnCommandmirror extends JAliEnBaseCommand {
	private boolean keepSamePath;
	private boolean useLFNasGuid;
	private boolean checkFileIsPresentOnDest;
	private boolean transferWholeArchive;
	private String collection;
	private Integer masterTransferId;
	private Integer attempts;
	private String lfn;
	private String dstSE;

	private int referenceCount = 0;
	private final List<String> ses = new ArrayList<>();
	private final List<String> exses = new ArrayList<>();
	private final HashMap<String, Integer> qos = new HashMap<>();

	/**
	 * @param commander
	 * @param out
	 * @param alArguments
	 */
	public JAliEnCommandmirror(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) {
		super(commander, out, alArguments);

		try {
			final OptionParser parser = new OptionParser();
			parser.accepts("try").withRequiredArg();
			parser.accepts("S").withRequiredArg();
			parser.accepts("g");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			final List<String> lfns = optionToString(options.nonOptionArguments());
			if (lfns == null || lfns.size() == 0)
				return;
			this.lfn = lfns.get(0);

			useLFNasGuid = options.has("g");
			checkFileIsPresentOnDest = options.has("u");
			transferWholeArchive = options.has("r");

			if (options.has("try"))
				attempts = Integer.valueOf(options.valueOf("try").toString());
			else
				attempts = Integer.valueOf(1);

			if (options.has("S") && options.hasArgument("S")) {
				if ((String) options.valueOf("S") != null) {
					final StringTokenizer st = new StringTokenizer((String) options.valueOf("S"), ",");
					while (st.hasMoreElements()) {
						final String spec = st.nextToken();
						if (spec.contains("::")) {
							if (spec.indexOf("::") != spec.lastIndexOf("::"))
								// SE
								// spec
								if (spec.startsWith("!"))
									exses.add(spec.substring(1).toUpperCase());
								else {// an SE spec
									ses.add(spec.toUpperCase());
									referenceCount++;
								}
						} else if (spec.contains(":"))
							try {
								final int c = Integer.parseInt(spec.substring(spec.indexOf(':') + 1));
								if (c > 0) {
									qos.put(spec.substring(0, spec.indexOf(':')), Integer.valueOf(c));
									referenceCount = referenceCount + c;
								} else
									throw new JAliEnCommandException();
							} catch (final Exception e) {
								throw new JAliEnCommandException();
							}
						else if (!spec.equals(""))
							throw new JAliEnCommandException();
					}
				}
			} else {
				if (lfns.size() != 2)
					throw new JAliEnCommandException();
				this.dstSE = lfns.get(1);
			}
		} catch (final OptionException e) {
			printHelp();
			throw e;
		}
	}

	@Override
	public void run() {
		if (this.useLFNasGuid && !GUIDUtils.isValidGUID(this.lfn)) {
			out.printErrln("Invalid GUID was specified");
			return;
		}

		if (this.ses.size() == 0 && this.dstSE != null && this.dstSE.length() != 0)
			this.ses.add(this.dstSE);

		if (this.ses.size() != 0 || this.qos.size() != 0) {
			HashMap<String, Integer> results;
			try {
				if (!this.useLFNasGuid)
					lfn = FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDir().getCanonicalName(), lfn);
				results = commander.c_api.mirrorLFN(lfn, this.ses, this.exses, this.qos, this.useLFNasGuid, this.attempts);
				for (final String s : results.keySet()) {
					String result_string;
					final Integer result = results.get(s);

					if (result != null) {
						result_string = JAliEnCommandmirror.Errcode2Text(result.intValue());
						if (result.intValue() > 0)
							out.printOutln(s + ": transfer scheduled");
						else
							out.printErrln(s + ": " + result_string);
					} else
						out.printErrln(s + ": unexpected error");
				}
			} catch (final IllegalArgumentException e) {
				out.printErrln(e.getMessage());
			}
		}
	}

	protected static String Errcode2Text(final int error) {
		String text = null;
		switch (error) {
		case 0:
			text = "file already exists on SE";
			break;
		case -256:
			text = "problem getting LFN";
			break;
		case -320:
			text = "LFN name empty";
			break;
		case -330:
			text = "LFN name empty";
			break;
		case -350:
			text = "other problem";
			break;
		case -255:
			text = "no destination SE name";
			break;
		case -254:
			text = "unable to connect to SE";
			break;
		case -253:
			text = "empty SE list";
			break;
		case -1:
			text = "wrong mirror parameters";
			break;
		case -2:
			text = "database connection missing";
			break;
		case -3:
			text = "cannot locate real pfns";
			break;
		case -4:
			text = "DB query failed";
			break;
		case -5:
			text = "DB query didn't generate a transfer ID";
			break;
		case -6:
			text = "cannot locate the archive LFN to mirror";
			break;
		}
		return text;
	}

	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln("mirror Copies a file into another SE");
		out.printOutln(" Usage:");
		out.printOutln("	mirror [-g] [-try <number>] [-S [se[,se2[,!se3[,qos:count]]]]] <lfn> [<SE>]");
		out.printOutln("                 -g:      Use the lfn as a guid");
		out.printOutln("                 -S:     specifies the destination SEs to be used");
		out.printOutln("                 -try <NumOfAttempts>     Specifies the number of attempts to try and mirror the file");
		out.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}
}