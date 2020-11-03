package alien.shell.commands;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import alien.se.SE;
import alien.shell.ShellColor;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.Format;

/**
 * @author costing
 *
 */
public class JAliEnCommandlistSEs extends JAliEnBaseCommand {
	private List<String> sesToQuery = new ArrayList<>();
	private final List<Set<String>> requestQosList = new ArrayList<>();

	@Override
	public void run() {
		final List<SE> results = commander.c_api.getSEs(sesToQuery);

		Collections.sort(results);

		final List<SE> filteredSEs = new ArrayList<>(results.size());

		int maxQosLength = 0;
		int maxSENameLength = 0;

		for (final SE se : results) {
			if (!se.seName.contains("::"))
				continue;

			if (requestQosList.size() > 0) {
				boolean any = false;

				for (final Set<String> qosSet : requestQosList)
					if (se.qos.containsAll(qosSet)) {
						any = true;
						break;
					}

				if (!any)
					continue;
			}

			int qosLen = 0;

			for (final String q : se.qos)
				qosLen += q.length();

			if (se.qos.size() > 1)
				qosLen += (se.qos.size() - 1) * 2;

			maxQosLength = Math.max(maxQosLength, qosLen);

			maxSENameLength = Math.max(maxSENameLength, se.seName.length());

			filteredSEs.add(se);
		}

		commander.printOutln(padRight(" ", maxSENameLength) + "\t\t                Capacity\t  \t\t\t\t\tDemote");
		commander.printOutln(padLeft("SE name", maxSENameLength) + "\t ID\t   Total  \t    Used  \t    Free      File count\t   Read   Write\t" + padRight("QoS", maxQosLength) + "\t  Endpoint URL");

		for (final SE se : filteredSEs) {
			String qos = "";

			int len = 0;

			for (final String q : se.qos) {
				if (qos.length() > 0) {
					len += 2;
					qos += ", ";
				}

				len += q.length();

				switch (q) {
					case "disk":
						qos += ShellColor.jobStateGreen() + q + ShellColor.reset();
						break;
					case "tape":
						qos += ShellColor.jobStateBlue() + q + ShellColor.reset();
						break;
					case "legooutput":
					case "legoinput":
						qos += ShellColor.jobStateYellow() + q + ShellColor.reset();
						break;
					default:
						qos += ShellColor.jobStateRed() + q + ShellColor.reset();
				}
			}

			for (; len < maxQosLength; len++)
				qos += " ";

			final long totalSpace = se.size * 1024;
			final long usedSpace = se.seUsedSpace;
			final long freeSpace = usedSpace <= totalSpace ? totalSpace - usedSpace : 0;

			commander.printOutln(String.format("%1$" + maxSENameLength + "s", se.originalName) + "\t" + String.format("%3d", Integer.valueOf(se.seNumber)) + "\t" + padLeft(Format.size(totalSpace), 8)
					+ "\t" + padLeft(Format.size(usedSpace), 8) + "\t" + padLeft(Format.size(freeSpace), 8)
					+ String.format("%16d", Long.valueOf(se.seNumFiles))
					+ "\t" + String.format("% .4f", Double.valueOf(se.demoteRead)) + " "
					+ String.format("% .4f", Double.valueOf(se.demoteWrite)) + "\t" + qos + "\t  " + se.generateProtocol());

			commander.printOut("seName", se.originalName);
			commander.printOut("seNumber", String.valueOf(se.seNumber));
			commander.printOut("totalSpace", String.valueOf(totalSpace));
			commander.printOut("usedSpace", String.valueOf(usedSpace));
			commander.printOut("freeSpace", String.valueOf(freeSpace));
			commander.printOut("fileCount", String.valueOf(se.seNumFiles));
			commander.printOut("demoteRead", String.valueOf(se.demoteRead));
			commander.printOut("demoteWrite", String.valueOf(se.demoteWrite));
			commander.printOut("qos", String.join(",", se.qos));
			commander.printOut("endpointUrl", se.generateProtocol());
			commander.outNextResult();
		}
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln("listSEs: print all (or a subset) of the defined SEs with their details");
		commander.printOutln(helpUsage("listSEs", "[-qos filter,by,qos] [SE name] [SE name] ..."));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-qos", "filter the SEs by the given QoS classes. Comma separate entries for 'AND', pass multiple -qos options for an 'OR'"));
	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

	/**
	 * @param commander
	 * @param alArguments
	 * @throws OptionException
	 */
	public JAliEnCommandlistSEs(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		final OptionParser parser = new OptionParser();
		parser.accepts("qos").withRequiredArg();

		final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

		if (options.has("qos")) {
			for (final Object qosObj : options.valuesOf("qos")) {
				final StringTokenizer st = new StringTokenizer(qosObj.toString(), " ,;");

				final Set<String> set = new HashSet<>();

				while (st.hasMoreTokens())
					set.add(st.nextToken());

				if (set.size() > 0)
					requestQosList.add(set);
			}
		}

		sesToQuery = optionToString(options.nonOptionArguments());
	}
}
