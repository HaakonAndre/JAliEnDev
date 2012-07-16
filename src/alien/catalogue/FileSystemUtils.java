package alien.catalogue;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.FindfromString;
import alien.api.catalogue.LFNfromString;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;
import alien.user.UsersHelper;

/**
 * @author ron
 * @since Mai 28, 2011
 * @author sraje (Shikhar Raje, IIIT Hyderabad)
 * @since Modified July 5, 2012
 */
@SuppressWarnings("unused")
public final class FileSystemUtils {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils
			.getLogger(CatalogueUtils.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory
			.getMonitor(CatalogueUtils.class.getCanonicalName());

	
	/**
	 * @param user
	 * @param directory
	 * @return the LFN
	 */
	public static LFN createCatalogueDirectory(final AliEnPrincipal user, final String directory) {
		
		String path = FileSystemUtils.getAbsolutePath(user.getName(), null, directory);
		
		if(path.endsWith("/"))
			path = path.substring(0,path.length()-1);

		final LFN lfn = LFNUtils.getLFN(path, true);
		LFN parent = lfn.getParentDir();
		
		if(!path.endsWith("/"))
			path += "/";

		if (AuthorizationChecker.canRead(lfn, user)
				&& AuthorizationChecker.canWrite(lfn, user)) {

			if (!lfn.exists) {

				if(!lfn.lfn.endsWith("/"))
					lfn.lfn += "/";
				
				lfn.size = 0;
				lfn.owner = user.getName();
				lfn.gowner = user.getName();
				lfn.type = 'd';
				lfn.perm="755";
				
				parent = LFNUtils.ensureDir(parent);
				
				if (parent==null){
					logger.log(Level.WARNING, "Parent dir for new directory ["+path+"]  is null for "+lfn.getCanonicalName());
					return null;
				}
				
				lfn.parentDir = parent;
				
				final IndexTableEntry ite = CatalogueUtils.getClosestMatch(path);
				if (ite==null){
					logger.log(Level.WARNING, "Insertion for new directory ["+path+"] failed, ite null: " + lfn.getCanonicalName());
					return null;
				}
				lfn.indexTableEntry = ite;
				
			    lfn.dir = parent.entryId;
								
				boolean inserted = LFNUtils.insertLFN(lfn);

				if (!inserted) {
					logger.log(Level.WARNING,
							"New directory ["+path+"] creation failed. Could not insert this LFN in the catalog : " + lfn);
					return null;
				}
				return LFNUtils.getLFN(path, true);
			}
			
			return lfn;
		}
		
		logger.log(Level.WARNING, "New directory ["+path+"] creation failed. Authorization failed.");
		
		return null;
	}
	
	@SuppressWarnings("null")
	public List<String> expandPathWildCards(final String sourcename, AliEnPrincipal user, String role)
	{
		List<String> result = null;
		String basename = "";
		String token = null;
		StringTokenizer st = new StringTokenizer(sourcename, "/");
		while(st.hasMoreTokens())
		{
			token = st.nextToken();
			int star = token.indexOf('*');
			int question = token.indexOf('?');
			if(star > 0 || question > 0)
			{
				List<String> valid_filenames = null;
				try
				{
					valid_filenames = Dispatcher.execute(new FindfromString(user, role, basename, token.substring(0, (star > question ? star:question)), 8)).getFileNames();//Alternative: We can call getLFNs from here, and then run the for loop to get the canonicals here. Advantage: Less serverside strain.
				}
				catch (ServerException se)
				{
					return null;
				}
				for(String valid_filename : valid_filenames)
					result.addAll(expandPathWildCards(basename + "/" + valid_filename + sourcename.substring((basename + "/" + token).length() + sourcename.indexOf(basename + "/" + token)), user, role));
			}
			else
				basename += token;
		}
		return result;
	}
	
//	public List<LFN> expandPathWildcards(final LFN source, String sourcename, AliEnPrincipal user, String role, String criteria)
//	{
//		List<LFN> result = null;
//		String[] components = sourcename.split("*");
//		String basename = components[0];
//		for(int i = 1; i < components.length; i++)
//		{
//			String component = components[i];
//			LFN temp = (new LFNfromString(user, role, component, false)).getLFN();
//			if(temp.isDirectory())
//			{
//				result.addAll(expandPathWildcards(temp, component, user, role, criteria));
//			}
//		}
//		return null;
//	}
	
	
	/**
	 * @param user
	 * @param path
	 * @param createNonExistentParents 
	 * @return the LFN
	 */
	public static LFN createCatalogueDirectory(AliEnPrincipal user,
			String path, boolean createNonExistentParents) {
		
		if(createNonExistentParents){
			String goDown = path;
			if(goDown.endsWith("/"))
				goDown = goDown.substring(0,goDown.length()-1);
			ArrayList<String> parents = new ArrayList<String>();
			parents.add(goDown);
			while(goDown.lastIndexOf('/')!=0){
				goDown = goDown.substring(0,goDown.lastIndexOf('/'));
				parents.add(goDown);
			}
			
			LinkedList<String> toDo = new LinkedList<String>();
			for(String parent: parents)
				if(LFNUtils.getLFN(parent)==null)
					toDo.add(parent);
			LFN ret = null;
			while(!toDo.isEmpty()){
				ret = createCatalogueDirectory(user,toDo.getLast());
				toDo.removeLast();
			}
			return ret;
		}
		
		return createCatalogueDirectory(user,path);
	}

	/**
	 * Get the absolute path, currentDir can be <code>null</code> then currentDir is set to user's home
	 * 
	 * @param user
	 * @param currentDirectory
	 * @param cataloguePath
	 * @return absolute path, or <code>null</code> if none could be found
	 */
	public static String getAbsolutePath(final String user, final String currentDirectory, final String cataloguePath) {
		String currentDir = currentDirectory!=null ? currentDirectory : UsersHelper.getHomeDir(user); 
		
		String path = cataloguePath;
		
		if (path.indexOf('~') == 0)
			path = UsersHelper.getHomeDir(user)
					+ path.substring(1, path.length());

		if (path.indexOf('/') != 0)
			path = currentDir + path;

		if (path.contains("//")) {
			path = path.replace("///", "/");
			path = path.replace("//", "/");
		}

		if (path.endsWith("/") && path.length() != 1)
			path = path.substring(0, path.lastIndexOf('/'));

		while (path.contains("/./"))
			path = path.replace("/./", "/");

		while (path.contains("/..")) {
			int pos = path.indexOf("/..") - 1;
			String newpath = path.substring(0, pos);
			newpath = newpath.substring(0, newpath.lastIndexOf('/'));
			if (path.length() > (pos + 3))
				path = newpath + "/" + path.substring(pos + 4);
			else
				path = newpath;
		}

		if (path.endsWith("/."))
			path = path.substring(0, path.length() - 1);

		if (path.endsWith("/.."))
			path = path.substring(0, currentDir.lastIndexOf('/'));

		return path;
	}

	private static final String[] translation = new String[] { "---", "--x",
			"-w-", "-wx", "r--", "r-x", "rw-", "rwx" };

	/**
	 * Get the type+perm string for LFN
	 * 
	 * @param lfn
	 * @return type+perm String e.g. -rwxrwxr-x or drwxr-xr-x
	 */
	public static String getFormatedTypeAndPerm(final LFN lfn) {
		final StringBuilder ret = new StringBuilder(10);

		if (lfn.type != 'f')
			ret.append(lfn.type);
		else
			ret.append('-');

		for (int pos = 0; pos < 3; pos++) {
			ret.append(translation[lfn.perm.charAt(pos) - '0']);
		}

		return ret.toString();
	}
}
