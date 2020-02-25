package alien.shell.commands;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.GetTagValues;
import alien.api.catalogue.GetTags;
import alien.catalogue.FileSystemUtils;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandshowTagValue extends JAliEnBaseCommand {

	private ArrayList<String> alPaths = null;
	private Set<String> theseTagsOnly = null;
	private Set<String> theseColumnsOnly = null;

	@Override
	public void run() {
		boolean first = true;

		for (final String eachFileName : alPaths) {
			final String absolutePath = FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), eachFileName);
			final List<String> sources = FileSystemUtils.expandPathWildCards(absolutePath, commander.user);

			for (final String file : sources) {
				if (!first)
					commander.printOutln();

				commander.printOutln(file);
				first = false;

				Set<String> tags = theseTagsOnly;

				if (tags == null || tags.size() == 0) {
					try {
						final GetTags availableTags = Dispatcher.execute(new GetTags(commander.user, file));

						tags = availableTags.getTags();

						if (tags == null || tags.size() == 0) {
							commander.printErrln("No tags for " + file);
							continue;
						}
					}
					catch (ServerException e) {
						final String error = "Could not get the list of tags for " + file + ": " + e.getMessage();

						commander.setReturnCode(1, error);
						commander.printErrln(error);

						return;
					}
				}

				for (final String tag : tags) {
					commander.printOutln("  " + tag);

					try {
						final GetTagValues tagValues = Dispatcher.execute(new GetTagValues(commander.user, file, tag, theseColumnsOnly));

						for (final Map.Entry<String, String> tagEntry : tagValues.getTagValues().entrySet()) {
							commander.printOutln("    " + tagEntry.getKey() + "=" + tagEntry.getValue());

							commander.printOut("fileName", file);
							commander.printOut("tagName", tag);

							commander.printOut("column", tagEntry.getKey());
							commander.printOut("value", tagEntry.getValue());
						}
					}
					catch (ServerException e) {
						final String error = "Could not get the columns for " + file + " for tag " + tag + ": " + e.getMessage();

						commander.setReturnCode(2, error);
						commander.printErrln(error);

						return;
					}
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
		commander.printOutln(helpUsage("showtagValue", "[flags] <filename> [<filename>...]"));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-t", "restrict to this tag only (default is to return all available tags)"));
		commander.printOutln(helpOption("-c", "restrict to these (comma separated) columns"));
		commander.printOutln();
	}

	/**
	 * cat cannot run without arguments
	 *
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * Constructor needed for the command factory in JAliEnCOMMander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandshowTagValue(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		try {
			final OptionParser parser = new OptionParser();

			parser.accepts("t").withRequiredArg();
			parser.accepts("c").withRequiredArg();

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			alPaths = new ArrayList<>(options.nonOptionArguments().size());
			alPaths.addAll(optionToString(options.nonOptionArguments()));

			if (options.has("t")) {
				StringTokenizer st = new StringTokenizer(options.valueOf("t").toString(), ",;");

				theseTagsOnly = new HashSet<>();

				while (st.hasMoreTokens()) {
					theseTagsOnly.add(st.nextToken());
				}
			}

			if (options.has("c")) {
				StringTokenizer st = new StringTokenizer(options.valueOf("c").toString(), ",;");

				theseColumnsOnly = new HashSet<>();

				while (st.hasMoreTokens()) {
					theseColumnsOnly.add(st.nextToken());
				}
			}
		}
		catch (final OptionException e) {
			printHelp();
			throw e;
		}
	}
}
