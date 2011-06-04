package alien.shell.commands;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import lazyj.Log;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.io.Transfer;
import alien.io.protocols.Protocol;
import alien.perl.commands.AlienTime;
import alien.ui.api.CatalogueApiUtils;
import alien.user.AliEnPrincipal;

public class JAliEnCommandcat extends JAliEnCommand {

	/**
	 * ls command arguments : -help/l/a
	 */
	private static ArrayList<String> lsArguments = new ArrayList<String>();

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils
			.getLogger(GUIDUtils.class.getCanonicalName());

	/**
	 * marker for -help argument
	 */
	private boolean bHelp = false;

	private String site = null;

	private String lfnOrGuid = null;

	/**
	 * @param p
	 *            AliEn principal received from https request
	 * @param al
	 *            all arguments received from SOAP request, contains user,
	 *            current directory and command
	 * @throws Exception
	 */
	public JAliEnCommandcat(AliEnPrincipal p, LFN currentDir, String site,
			final ArrayList<String> al) throws Exception {
		super(p, currentDir, al);
		this.site = site;
	}

	public void executeCommand() throws Exception {

		final Iterator<String> it = alArguments.iterator();

		while (it.hasNext()) {
			String arg = it.next();
			if ("-h".equals(arg) || "-help".equals(arg))
				bHelp = true;
		}

		// if (args[1].equals("-h"))
		// bHelp = true;

		if (!bHelp) {

				ArrayList<String> args = new ArrayList<String>();
				args.add("-s");
				args.addAll(alArguments);
				JAliEnCommandget get = new JAliEnCommandget(principal,
						currentDirectory, site, args);
				get.executeCommand();
				File out = get.getOutputFile();
				if (out != null && out.isFile() && out.canRead()) {
					FileInputStream fstream = new FileInputStream(out);

					DataInputStream in = new DataInputStream(fstream);
					BufferedReader br = new BufferedReader(
							new InputStreamReader(in));
					String strLine;

					while ((strLine = br.readLine()) != null) {

						System.out.println(strLine);
					}
				

			}
		} else {
			System.out.println(AlienTime.getStamp() + "Usage: cat  ... ");
			System.out.println("		-g : get file by GUID");
			System.out.println("		-s : se,se2,!se3,se4,!se5");
			System.out.println("		-o : outputfilename");

		}
	}

}
