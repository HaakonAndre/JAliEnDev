package alien.catalogue;

import java.util.ArrayList;
import java.util.Collection;
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

	/**
	 * @param sourcename
	 * @param user
	 * @param role
	 * @return the matching lfns from the catalogue
	 */
	public static List<String> expandPathWildCards(final String sourcename, final AliEnPrincipal user, final String role){
		final List<String> result = new ArrayList<String>();
		
		final int idxStar = sourcename.indexOf('*');
		final int idxQM   = sourcename.indexOf('?');
		
		if (idxStar<0 && idxQM<0){
			result.add(sourcename);
			return result;
		}
		
		final int minIdx = idxStar>=0 ? (idxQM>=0 ? Math.min(idxStar, idxQM) : idxStar) : idxQM;
		
		final int lastIdx = sourcename.lastIndexOf('/', minIdx);
		
		final String path;
		final String pattern;
		
		if (lastIdx<0){
			path = "/";
			pattern = sourcename;
		}
		else{
			path = sourcename.substring(0, lastIdx+1);
			pattern = sourcename.substring(lastIdx+1);
		}
		
		System.err.println("Path: "+path+"\nPattern: "+pattern);
		
		try{
			final FindfromString ret = Dispatcher.execute(new FindfromString(user, role, path, pattern+"$", LFNUtils.FIND_REGEXP | LFNUtils.FIND_INCLUDE_DIRS));
			
			if (ret!=null){
				final Collection<LFN> lfns = ret.getLFNs();
			
				if (lfns!=null){
					for (final LFN l: lfns){
						result.add(l.getCanonicalName());
					}
				}
			}
		}
		catch (ServerException se){
			return null;
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
