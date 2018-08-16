package alien.shell.commands;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import alien.se.SE;
import alien.shell.ShellColor;
import joptsimple.OptionException;
import lazyj.Format;

/**
 * @author costing
 *
 */
public class JAliEnCommandlistSEs extends JAliEnBaseCommand {
	private List<String> sesToQuery = new ArrayList<>();

	@Override
	public void run() {
		final List<SE> results = commander.c_api.getSEs(sesToQuery);

		commander.printOutln("                                        \t\t                Capacity\t  \t\t\tDemote");
		commander.printOutln("                        SE name         \t ID\t   Total  \t    Used  \t    Free  \t   Read   Write\t               QoS  \t  Endpoint URL");

		Collections.sort(results);

		for (final SE se : results) {
			if (!se.seName.contains("::"))
				continue;

			String qos = "";

			int len = 0;

			for (String q : se.qos) {
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
					qos += ShellColor.jobStateYellow() + q + ShellColor.reset();
					break;
				default:
					qos += ShellColor.jobStateRed() + q + ShellColor.reset();
				}
			}

			for (; len <= 20; len++)
				qos += " ";

			long totalSpace = se.size * 1024;
			long usedSpace = se.seUsedSpace;
			long freeSpace = usedSpace <= totalSpace ? totalSpace - usedSpace : 0;

			commander.printOutln(String.format("%1$" + 40 + "s", se.originalName) + "\t" + String.format("%3d", Integer.valueOf(se.seNumber)) + "\t" + padLeft(Format.size(totalSpace), 8) + "\t"
					+ padLeft(Format.size(usedSpace), 8) + "\t" + padLeft(Format.size(freeSpace), 8) + "\t" + String.format("% .4f", Double.valueOf(se.demoteRead)) + " "
					+ String.format("% .4f", Double.valueOf(se.demoteWrite)) + "\t" + qos + "\t  " + se.seioDaemons);
		}

		commander.printOutln();
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln("listSEs: print all (or a subset) of the defined SEs with their details");
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

		if (alArguments != null)
			for (final String arg : alArguments)
				if (arg.indexOf("::") >= 0)
					sesToQuery.add(arg);
	}
}
