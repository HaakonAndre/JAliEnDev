package alien.shell.commands;

import java.util.ArrayList;
import java.util.logging.Logger;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.config.ConfigUtils;
import alien.ui.api.CatalogueApiUtils;
import alien.user.AliEnPrincipal;
import alien.user.UsersHelper;

public class JAliEnCommandcd extends JAliEnCommand {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils
			.getLogger(GUIDUtils.class.getCanonicalName());

	/**
	 * @param al
	 *            all arguments received from SOAP request, contains user,
	 *            current directory and command
	 * @throws Exception
	 */
	public JAliEnCommandcd(AliEnPrincipal p, LFN currentDir,
			final ArrayList<String> al) throws Exception {
		super(p, currentDir, al);
	}

	public LFN changeDir() {
		executeCommand();
		return currentDirectory;
	}

	public void executeCommand() {

		LFN  newDir = null;
		
		if (alArguments.size() > 0) 
				newDir = CatalogueApiUtils.getLFN(FileSystemUtils.getAbsolutePath(principal.getName(),
						currentDirectory.getCanonicalName(), alArguments.get(0)));
		else
			newDir = CatalogueApiUtils.getLFN(UsersHelper
					.getHomeDir(principal.getName()));

		if(newDir!=null)
			currentDirectory = newDir;

	}

}
