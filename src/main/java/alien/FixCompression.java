package alien;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.tomcat.util.http.fileupload.FileUtils;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.CatalogueApiUtils;
import alien.api.catalogue.PFNforReadOrDel;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.XmlCollection;
import alien.catalogue.access.AccessType;
import alien.io.protocols.Factory;
import alien.shell.commands.JAliEnCOMMander;
import alien.site.OutputEntry;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author yuw
 *
 */
public class FixCompression {

	private static JAliEnCOMMander commander = null;
	private static Random rand = new Random(System.currentTimeMillis());

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(final String[] args) throws Exception {

		if (args.length > 0) {
			try {
				commander = JAliEnCOMMander.getInstance();
			}
			catch (final ExceptionInInitializerError | NullPointerException e) {
				System.err.println("Failed to get a JAliEnCOMMander instance. Abort");
				e.printStackTrace();
				return;
			}

			final OptionParser parser = new OptionParser();
			parser.accepts("list").withRequiredArg(); // Like "/alice/sim/2017/LHC17e5/TrackRefs0.xml"
			final OptionSet options = parser.parse(args);

			// Read archive members names from file
			final String collectionName = (String) options.valueOf("list");

			final LFN collectionLFN = commander.c_api.getLFN(collectionName);
			if (collectionLFN == null || !collectionLFN.exists) {
				System.err.println("Failed to get collection from " + collectionName);
				return;
			}

			final XmlCollection xmlCollection = new XmlCollection(collectionLFN);

			Iterator<LFN> xmlEntries = xmlCollection.iterator();
			System.out.println("We will process next files:");
			while (xmlEntries.hasNext()) {
				System.out.println("- " + xmlEntries.next().getCanonicalName());
			}
			System.out.println();

			xmlCollection.parallelStream().forEach((x) -> fixArchiveCompression(x.getCanonicalName()));

			// xmlEntries = xmlCollection.iterator();
			// while (xmlEntries.hasNext()) {
			// fixArchiveCompression(xmlEntries.next().getCanonicalName());
			// }
			System.out.println();
			System.out.println("All files processed. Exiting");

			final File validation = new File("validation_error.message");
			if (validation.length() == 0)
				validation.delete();
		}
	}

	private static void fixArchiveCompression(final String xmlEntry) {
		// Use this for debugging
		// final ByteArrayOutputStream out = new ByteArrayOutputStream();
		// commander.setLine(new JSONPrintWriter(out), null);

		// Parse the parent directory from the entry, e.g.:
		// /alice/sim/2018/LHC18e1a/246053/075/BKG/TrackRefs.root ->
		// /alice/sim/2018/LHC18e1a/246053/075
		String parentdir = xmlEntry.substring(0, xmlEntry.lastIndexOf("/"));

		final String lastStringToken = parentdir.substring(parentdir.lastIndexOf("/") + 1);
		if (!lastStringToken.matches("^\\d+$")) {
			parentdir = parentdir.substring(0, parentdir.lastIndexOf("/"));
		}

		System.out.println();
		System.out.println("[" + new Date() + "] Processing " + parentdir + "/root_archive.zip");

		// Check if this directory has been already processed
		if (commander.c_api.getLFN(parentdir + "/.fixed") != null) {
			System.err.println("[" + new Date() + "] " + parentdir + "/root_archive.zip" + ": Already fixed");
			return;
		}

		final String registerPath = parentdir + "/registertemp";

		final LFN remoteArchiveLFN = commander.c_api.getLFN(parentdir + "/root_archive.zip");
		if (remoteArchiveLFN == null || !remoteArchiveLFN.exists) {
			System.err.println("[" + new Date() + "] " + parentdir + "/root_archive.zip" + ": Archive not found in parent dir. Abort");
			return;
		}

		String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

		String basedir = new String(System.getProperty("user.dir") + System.getProperty("file.separator") + "fixcompression" + System.getProperty("file.separator")
				+ rand.ints(10, 0, chars.length()).mapToObj(i -> "" + chars.charAt(i)).collect(Collectors.joining()));
		final File workingdir = new File(basedir);
		if (!workingdir.exists()) {
			workingdir.mkdirs();
		}

		// Main procedure
		//
		try (PrintWriter validation = new PrintWriter(new FileOutputStream("validation_error.message", true))) {

			final List<PFN> remoteArchivePFN = Dispatcher.execute(new PFNforReadOrDel(commander.getUser(), commander.getSite(), AccessType.DELETE, remoteArchiveLFN, null, null)).getPFNs();
			if (remoteArchivePFN.size() == 0) {
				System.err.println("[" + new Date() + "] " + parentdir + "/root_archive.zip" + ": Archive is orphaned");
				validation.println("Orphaned archive " + parentdir + "/root_archive.zip");
				return;
			}

			final String remoteArchive = remoteArchiveLFN.getCanonicalName();
			final String archiveName = remoteArchiveLFN.getFileName();
			final long jobID = remoteArchiveLFN.jobid;

			final List<LFN> remoteArchiveMembers = commander.c_api.getArchiveMembers(remoteArchive);
			if (remoteArchiveMembers == null || remoteArchiveMembers.isEmpty()) {
				System.err.println("[" + new Date() + "] Failed to get members of the remote archive: " + remoteArchive);
				validation.println("Failed to get members of " + remoteArchive);
				return;
			}

			final File localArchive = new File(basedir + System.getProperty("file.separator") + archiveName);
			if (localArchive.exists()) {
				localArchive.delete();
			}

			// Download the archive from the Grid
			//
			System.out.println("[" + new Date() + "] Downloading the archive from the Grid");
			commander.c_api.downloadFile(remoteArchive, localArchive, "-silent");
			if (!localArchive.exists()) {
				System.err.println("[" + new Date() + "] " + remoteArchive + ": Failed to download remote archive");
				validation.println("Download failed " + remoteArchive);
				return;
			}

			// Unpack to local directory and zip again without member file
			//
			System.out.println("[" + new Date() + "] Unpacking to local directory");
			if (!unzip(archiveName, basedir)) {
				System.err.println("[" + new Date() + "] " + remoteArchive + ": Failed to extract files from archive: " + basedir + System.getProperty("file.separator") + archiveName);
				validation.println("Extraction failed " + remoteArchive);
				return;
			}
			localArchive.delete();

			final ArrayList<String> listOfFiles = getFileListing(basedir + System.getProperty("file.separator") + "extracted");

			System.out.println("[" + new Date() + "] Zipping the new archive");
			final OutputEntry entry = new OutputEntry(archiveName, listOfFiles, "", Long.valueOf(jobID));
			entry.createZip(basedir + System.getProperty("file.separator") + "extracted");

			final String newArchiveFullPath = registerPath + "/" + archiveName;

			// Upload the new archive to the Grid
			//
			System.out.println("[" + new Date() + "] Uploading the new archive to the Grid: " + newArchiveFullPath);

			final File newArchive = new File(basedir + System.getProperty("file.separator") + "extracted" + System.getProperty("file.separator") + archiveName);

			LFN newArchiveLFN = commander.c_api.getLFN(newArchiveFullPath);
			if (newArchiveLFN != null) {
				// Delete registertemp/root_archive.zip if there is any
				commander.c_api.removeLFN(newArchiveFullPath);
			}

			commander.c_api.uploadFile(newArchive, newArchiveFullPath, "-w", "-S", "disk:1"); // Create only one replica

			newArchiveLFN = commander.c_api.getLFN(newArchiveFullPath);
			if (newArchiveLFN == null || !newArchiveLFN.exists) {
				System.err.println("[" + new Date() + "] " + remoteArchive + ": Failed to upload archive " + newArchiveFullPath);
				validation.println("Upload failed " + newArchiveFullPath);
				return;
			}

			// Register files in the catalogue
			//

			// Create subdirs (like BKG/)
			for (final String file : listOfFiles) {
				if (file.contains("/")) {
					if (commander.c_api.createCatalogueDirectory(registerPath + "/" + file.substring(0, file.lastIndexOf("/")), true) == null) {
						System.err.println("[" + new Date() + "] " + remoteArchive + ": Failed to create new directory " + registerPath + "/" + file.substring(0, file.lastIndexOf("/")));
						validation.println("Mkdir failed " + registerPath + "/" + file.substring(0, file.lastIndexOf("/")));

						// Delete all newly created entries and directories
						if (registerPath.length() > 20) // Safety check
							commander.c_api.removeLFN(registerPath, true);
						return;
					}
				}
			}

			System.out.println("[" + new Date() + "] Registering files in the catalogue");
			CatalogueApiUtils.registerEntry(entry, registerPath + "/", commander.getUser());

			for (final String file : listOfFiles) {
				final LFN entryLFN = commander.c_api.getLFN(registerPath + "/" + file);
				if (entryLFN == null || !entryLFN.exists) {
					System.err.println("[" + new Date() + "] " + remoteArchive + ": Failed to register entry " + registerPath + "/" + file);
					validation.println("Register failed " + registerPath + "/" + file);

					// Delete all newly created entries and directories
					if (registerPath.length() > 20) // Safety check
						commander.c_api.removeLFN(registerPath, true);
					return;
				}
			}

			// Delete the members links of old archive
			//
			System.out.println("[" + new Date() + "] Deleting the members links of old archive");
			for (final LFN member : remoteArchiveMembers) {
				System.out.println("[" + new Date() + "] Deleting " + member.getCanonicalName());
				if (!commander.c_api.removeLFN(member.getCanonicalName())) {
					System.err.println("[" + new Date() + "] " + remoteArchive + ": Failed to delete old archive member " + member.getCanonicalName());
					validation.println("Archive member deletion failed " + remoteArchive);

					// Delete all newly created entries and directories
					if (registerPath.length() > 20) // Safety check
						commander.c_api.removeLFN(registerPath, true);
					return;
				}
			}

			// Delete old remote archive
			//
			System.out.println("[" + new Date() + "] Deleting old remote archive");

			// Remove lfn of the old archive
			if (!commander.c_api.removeLFN(remoteArchive)) {
				System.err.println("[" + new Date() + "] " + remoteArchive + ": Failed to delete old archive");
				validation.println("Archive deletion failed " + remoteArchive);

				// Delete all newly created entries and directories
				if (registerPath.length() > 20) // Safety check
					commander.c_api.removeLFN(registerPath, true);
				return;
			}

			// Remove physical replicas of the old archive
			final Iterator<PFN> it = remoteArchivePFN.iterator();
			while (it.hasNext()) {
				final PFN pfn = it.next();

				try {
					System.out.println("[" + new Date() + "] Deleting copy at " + pfn.getSE().getName());
					if (!Factory.xrootd.delete(pfn)) {
						System.err.println("[" + new Date() + "] " + remoteArchive + ": Could not delete " + pfn.pfn);
					}
				}
				catch (final IOException e) {
					e.printStackTrace();
				}
			}

			// Create file marker to leave trace
			if (commander.c_api.touchLFN(parentdir + "/" + ".fixed") == null) {
				System.err.println("[" + new Date() + "] " + remoteArchive + ": Could not create .fixed marker");
			}

			// Rename uploaded archive
			//
			System.out.println("[" + new Date() + "] Renaming uploaded archive");
			if (commander.c_api.moveLFN(registerPath + "/" + archiveName, remoteArchive) == null) {
				System.err.println("[" + new Date() + "] " + remoteArchive + ": Failed to rename the archive " + registerPath + "/" + archiveName);

				// Check if there is another copy of the same file in parentdir
				final LFN registerArchive = commander.c_api.getLFN(registerPath + "/" + archiveName);
				final LFN parentArchive = commander.c_api.getLFN(remoteArchive);
				if (parentArchive != null && parentArchive.guid.equals(registerArchive.guid)) {
					commander.c_api.removeLFN(registerArchive.getCanonicalName(), false, false);
				}
				else {
					validation.println("Renaming failed " + registerPath + "/" + archiveName);
					return;
				}
			}

			// Rename new archive members
			for (final String file : listOfFiles) {
				if (commander.c_api.moveLFN(registerPath + "/" + file, parentdir + "/" + file) == null) {
					System.err.println("[" + new Date() + "] " + remoteArchive + ": Failed to rename archive member " + parentdir + "/" + file);

					// Check if there is another copy of the same file in parentdir
					final LFN registerMember = commander.c_api.getLFN(registerPath + "/" + file);
					final LFN parentMember = commander.c_api.getLFN(parentdir + "/" + file);
					if (parentMember != null && parentMember.guid.equals(registerMember.guid)) {
						commander.c_api.removeLFN(registerMember.getCanonicalName(), false, false);
					}
					else {
						validation.println("Renaming failed " + parentdir + "/" + file);
						return;
					}
				}
			}

			if (registerPath.length() > 20) // Safety check
				commander.c_api.removeLFN(registerPath, true);

			System.out.println("[" + new Date() + "] " + "Fixed " + remoteArchive);

			// Clean up local files
			newArchive.delete();

		}
		catch (final IOException e1) {
			e1.printStackTrace();
		}
		catch (final ServerException e1) {
			System.err.println("[" + new Date() + "] " + parentdir + "/root_archive.zip" + ": Could not get PFN. Abort");
			e1.printStackTrace();
		}

		try {
			FileUtils.deleteDirectory(workingdir);
			System.out.println();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Size of the buffer to read/write data
	 */
	private static final int BUFFER_SIZE = 4096;

	/**
	 * Extracts a zip file specified by the zipFilePath to a directory specified by
	 * destDirectory (will be created if does not exists)
	 * 
	 * @param jobID
	 * @throws IOException
	 */
	private static boolean unzip(final String archiveName, final String basedir) throws IOException {

		final File destDir = new File(basedir + System.getProperty("file.separator") + "extracted");
		if (!destDir.exists()) {
			destDir.mkdir();
		}
		else {
			// Clean up temp directory if there are files in it
			final String[] destDirEntries = destDir.list();
			if (destDirEntries != null)
				for (final String s : destDirEntries) {
					final File currentFile = new File(destDir.getPath(), s);
					currentFile.delete();
				}
		}

		// Start unpacking the archive
		try (ZipInputStream zipIn = new ZipInputStream(new FileInputStream(basedir + System.getProperty("file.separator") + archiveName))) {
			ZipEntry entry = zipIn.getNextEntry();
			// iterates over entries in the zip file
			while (entry != null) {
				final String filePath = destDir.getAbsolutePath() + System.getProperty("file.separator") + entry.getName();
				if (!entry.isDirectory()) {
					// If the entry is a file, extract it
					String path = filePath.substring(0, filePath.lastIndexOf("/"));
					final File dir = new File(path);
					dir.mkdirs();

					extractFile(zipIn, filePath);
				}
				else {
					// If the entry is a directory, make the directory
					final File dir = new File(filePath);
					dir.mkdir();
				}
				System.out.println("[" + new Date() + "] - extracted " + entry.getName());
				zipIn.closeEntry();
				entry = zipIn.getNextEntry();
			}

			zipIn.close();
			return true;
		}
		catch (final FileNotFoundException e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Extracts a zip entry (file entry)
	 * 
	 * @param zipIn
	 * @param filePath
	 * @throws IOException
	 */
	private static void extractFile(final ZipInputStream zipIn, final String filePath) throws IOException {
		try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath))) {
			byte[] bytesIn = new byte[BUFFER_SIZE];
			int read = 0;
			while ((read = zipIn.read(bytesIn)) != -1) {
				bos.write(bytesIn, 0, read);
			}
			bos.close();
		}
	}

	/**
	 * Get list of files in a directory recursively while saving relative paths
	 * 
	 * @param folderName
	 *            folder to look inside
	 */
	private static ArrayList<String> getFileListing(final String folderName) {
		final File folder = new File(folderName);
		final ArrayList<String> listOfFiles = new ArrayList<>();
		final File[] listing = folder.listFiles();
		if (listing == null) {
			System.err.println("[" + new Date() + "] Failed to get list of files in local folder. Break");
			return null;
		}
		for (final File file : listing) {
			if (file.isDirectory()) {
				try {
					for (final String child : getFileListing(file.getCanonicalPath())) {
						listOfFiles.add(file.getName() + "/" + child);
					}
				}
				catch (final IOException e) {
					e.printStackTrace();
				}
			}
			else
				listOfFiles.add(file.getName());
		}
		return listOfFiles;
	}
}
