package alien.shell.commands;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

import joptsimple.OptionException;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.user.AuthorizationChecker;

/**
 * @author ron
 * @since Oct 27, 2011
 */
public class JAliEnCommandrm extends JAliEnBaseCommand {

	@Override
	public void run() {

		for (String path : alArguments) {

			LFN file = commander.c_api.getLFN(FileSystemUtils
					.getAbsolutePath(commander.user
							.getName(), commander
							.getCurrentDir()
							.getCanonicalName(), path));

			if (file!=null && file.exists) {
				if (!file.isDirectory()) {
					if (AuthorizationChecker.canWrite(file, commander.user)) {
						
						delFile(file.getCanonicalName());

//						if (!commander.c_api
//								.removeLFN(dir.getCanonicalName())) {
//							out.printErrln("Could not remove LFN: "
//									+ path);
//							out.printErrln("Sorry, this command is not implemented yet.");
//						}

					} else {
						if (!isSilent())
							out.printErrln("Permission denied on LFN: ["
									+ path + "]");
					}

				} else {
					if (!isSilent())
						out.printErrln("LFN is a directory: [" + path + "]");
				}
			} else {
				if (!isSilent())
					out.printErrln("No such file or directory: [" + path + "]");
			}
		}
	}
	
	
	/**
	 * @param file 
	 * @return mv file to garbage_bin
	 */
	public boolean delFile(final String file) {
		
		final String garbageBin = "~/garbage_bin/";
				
		ArrayList<String> args = new ArrayList<String>(2);
		args.addAll(alArguments);
		args.add(garbageBin + file.substring(file.lastIndexOf('/')+1) + "_" + (System.currentTimeMillis() / 1000L));
		
		JAliEnCommandmv mv;
		try {
			mv = (JAliEnCommandmv) JAliEnCOMMander.getCommand("mv",
					new Object[] { commander, out, args });
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		mv.silent();
		
		try {
			
			mv.start();
			while (mv.isAlive()) {
				Thread.sleep(1);
				}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {

		out.printOutln();
		out.printOutln(helpUsage("rm",
				" <LFN> [<LFN>[,<LFN>]]"));
		out.printOutln(helpStartOptions());
		out.printOutln(helpOption("-silent","execute command silently"));
		out.printOutln();
	}

	/**
	 * mkdir cannot run without arguments
	 * 
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * Constructor needed for the command factory in commander
	 * 
	 * @param commander
	 * @param out
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandrm(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);

	}
}
