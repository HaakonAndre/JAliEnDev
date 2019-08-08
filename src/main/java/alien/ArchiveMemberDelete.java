package alien;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

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
public class ArchiveMemberDelete {

	private static JAliEnCOMMander commander = null;

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
			parser.accepts("list").withRequiredArg(); // Like "collection.xml"
			final OptionSet options = parser.parse(args);

			// Read archive members names from file
			final String collectionName = (String) options.valueOf("list");

			final File collectionFile = new File(collectionName);
			if (!collectionFile.exists()) {
				System.err.println("Couldn't open the collection! File " + collectionName + " doesn't exist");
				return;
			}
			final XmlCollection xmlCollection = new XmlCollection(collectionFile);

			Iterator<LFN> xmlEntries = xmlCollection.iterator();
			System.out.println("We will process next files:");
			while (xmlEntries.hasNext()) {
				System.out.println("- " + xmlEntries.next().getCanonicalName());
			}
			System.out.println();

			xmlEntries = xmlCollection.iterator();
			while (xmlEntries.hasNext()) {
				deleteArchiveMember(xmlEntries.next().getCanonicalName());
			}
			System.out.println();
			System.out.println("All files processed. Exiting");

			final Path validation = Path.of("validation_error.message");
			if (Files.size(validation) == 0)
				cleanUpLocal(validation);
		}
	}

	private static void deleteArchiveMember(final String xmlEntry) {
		// Use this for debugging
		// final ByteArrayOutputStream out = new ByteArrayOutputStream();
		// commander.setLine(new JSONPrintWriter(out), null);
		System.out.println();
		System.out.println("[" + new Date() + "] Processing " + xmlEntry);

		// Parse the parent directory from the entry, e.g.:
		// /alice/sim/2018/LHC18e1a/246053/075/BKG/TrackRefs.root ->
		// /alice/sim/2018/LHC18e1a/246053/075
		String parentdir = xmlEntry.substring(0, xmlEntry.lastIndexOf("/"));
		final String lastStringToken = parentdir.substring(parentdir.lastIndexOf("/") + 1);
		if (!lastStringToken.matches("^\\d+.\\d+$")) {
			parentdir = parentdir.substring(0, parentdir.lastIndexOf("/"));
		}

		final String registerPath = parentdir + "/registertemp";

		LFN remoteLFN = null;
		try {
			remoteLFN = commander.c_api.getLFN(xmlEntry);
		}
		catch (final NullPointerException e) {
			// Connection may be refused
			System.err.println("[" + new Date() + "] Something went wrong. Abort.");
			e.printStackTrace();
			return;
		}

		// Clean up previous iterations
		//
		if (remoteLFN == null || !remoteLFN.exists) {
			// TrackRefs was deleted, registertemp contains valid files
			System.err.println("[" + new Date() + "] " + xmlEntry + ": LFN doesn't exist, checking registertemp");

			if (commander.c_api.getLFN(registerPath) != null) {
				// Move everything from registertemp to parent
				System.out.println("[" + new Date() + "] " + "registertemp found, moving out its content");
				for (final LFN file : commander.c_api.getLFNs(registerPath)) {
					if (file == null || !file.exists) {
						System.err.println("[" + new Date() + "] " + "Failed to get directory listing for " + registerPath + ". Abort.");
						return;
					}

					// Check if there is another copy of the same file in parentdir
					final LFN registerMember = commander.c_api.getLFN(registerPath + "/" + file.getFileName());
					final LFN parentMember = commander.c_api.getLFN(parentdir + "/" + file.getFileName());
					if (parentMember != null) {
						if (parentMember.guid.equals(registerMember.guid))
							commander.c_api.removeLFN(parentMember.getCanonicalName(), false, false);
						else
							commander.c_api.removeLFN(parentMember.getCanonicalName());
					}

					System.out.println("[" + new Date() + "] " + "Moving " + registerPath + "/" + file.getFileName());
					if (commander.c_api.moveLFN(registerPath + "/" + file.getFileName(), parentdir + "/" + file.getFileName()) == null) {
						System.err.println("[" + new Date() + "] " + "Failed to move " + file.getFileName() + ". Abort.");
						return;
					}
				}

				// Delete registertemp dir since all files are moved
				if (registerPath.length() > 20) // Safety check
					commander.c_api.removeLFN(registerPath, true);
			}
			else {
				if (!commander.c_api.find(parentdir, ".deleted", 0).isEmpty()) {
					System.out.println("[" + new Date() + "] " + "registertemp is not there, all DONE");
				}
				else {
					System.out.println("[" + new Date() + "] " + "registertemp is not there, but " + parentdir + "/.deleted NOT FOUND. Abort.");
				}
			}
			return;
		}
		// Else
		// TrackRefs was not deleted, remove invalid files from registertemp
		if (commander.c_api.getLFN(registerPath) != null) {
			// Delete registertemp dir since it can be corrupted
			if (registerPath.length() > 20) // Safety check
				commander.c_api.removeLFN(registerPath, true);
		}
		else {
			System.out.println("[" + new Date() + "] " + "registertemp is not there, continue with the main procedure");
		}

		// Continue basic checks
		// Check if we are able to get PFN list
		List<PFN> remotePFN = null;
		try {
			remotePFN = Dispatcher.execute(new PFNforReadOrDel(commander.getUser(), commander.getSite(), AccessType.DELETE, remoteLFN, null, null)).getPFNs();
		}
		catch (final ServerException e1) {
			System.err.println("[" + new Date() + "] " + xmlEntry + ": Could not get PFN. Abort");
			e1.printStackTrace();
			return;
		}

		// If not - the file is orphaned
		if (remotePFN == null || remotePFN.size() == 0) {
			System.err.println("[" + new Date() + "] " + xmlEntry + ": Can't get PFNs for this file. Abort");
			return;
		}

		final String remoteFile = remoteLFN.getCanonicalName();
		final long remoteFileSize = remoteLFN.getSize();

		final LFN remoteArchiveLFN = commander.c_api.getRealLFN(remoteFile);
		if (remoteArchiveLFN == null || !remoteArchiveLFN.exists) {
			System.err.println("[" + new Date() + "] " + remoteFile + ": Archive not found in parent dir. Abort");
			return;
		}

		// If the file is not a member of any archives, just delete it
		if (remoteLFN.equals(remoteArchiveLFN)) {
			System.out.println("[" + new Date() + "] " + remoteFile + " is a real file, we'll simply delete it");

			// Speed up things by calling xrootd delete directly
			final Iterator<PFN> it = remotePFN.iterator();
			while (it.hasNext()) {
				PFN pfn = it.next();

				try {
					if (!Factory.xrootd.delete(pfn)) {
						System.err.println("[" + new Date() + "] " + remoteFile + ": Could not delete " + pfn.pfn);
					}
				}
				catch (final IOException e) {
					e.printStackTrace();
				}
			}

			commander.c_api.removeLFN(remoteFile);
			commander.c_api.touchLFN(parentdir + "/" + ".deleted" + remoteFileSize);

			System.out.println("[" + new Date() + "] " + remoteFile);
			System.out.println("[" + new Date() + "] Reclaimed " + remoteFileSize + " bytes of disk space");
			return;
		}

		// Main procedure
		//
		try (PrintWriter validation = new PrintWriter(new FileOutputStream("validation_error.message", true))) {

			final List<PFN> remoteArchivePFN = Dispatcher.execute(new PFNforReadOrDel(commander.getUser(), commander.getSite(), AccessType.DELETE, remoteArchiveLFN, null, null)).getPFNs();
			if (remoteArchivePFN.size() == 0) {
				System.err.println("[" + new Date() + "] " + remoteFile + ": Archive is orphaned");
				validation.println("Orphaned archive " + remoteFile);
				return;
			}

			final String remoteArchive = remoteArchiveLFN.getCanonicalName();
			final String archiveName = remoteArchiveLFN.getFileName();
			final String memberName = remoteLFN.getFileName();
			final long jobID = remoteArchiveLFN.jobid;

			final List<LFN> remoteArchiveMembers = commander.c_api.getArchiveMembers(remoteArchive);
			if (remoteArchiveMembers == null || remoteArchiveMembers.isEmpty()) {
				System.err.println("[" + new Date() + "] Failed to get members of the remote archive: " + remoteArchive);
				validation.println("Failed to get members of " + remoteArchive);
				return;
			}
			if (remoteArchiveMembers.size() == 1) {
				// RemoteLFN is the only file in remoteArchive
				// No point in downloading it, just remove file and archive

				System.out.println("[" + new Date() + "] Deleting remote file");
				commander.c_api.removeLFN(remoteFile);

				System.out.println("[" + new Date() + "] Deleting old remote archive");

				// Remove physical replicas of the old archive
				final Iterator<PFN> it = remoteArchivePFN.iterator();
				while (it.hasNext()) {
					final PFN pfn = it.next();

					try {
						if (!Factory.xrootd.delete(pfn)) {
							System.err.println("[" + new Date() + "] " + remoteFile + ": Could not delete " + pfn.pfn);
						}
					}
					catch (final IOException e) {
						e.printStackTrace();
					}
				}

				// Remove lfn of the old archive
				commander.c_api.removeLFN(remoteArchive);

				// Create file marker to leave trace
				commander.c_api.touchLFN(parentdir + "/" + ".deleted" + remoteArchiveLFN.getSize());

				System.out.println("[" + new Date() + "] " + memberName + " was " + remoteLFN.getSize() + " bytes");
				System.out.println("[" + new Date() + "] " + "Old archive was " + remoteArchiveLFN.getSize() + " bytes");
				System.out.println("[" + new Date() + "] " + "Reclaimed " + remoteArchiveLFN.getSize() + " bytes of disk space");

				return;
			}

			final File localArchive = new File(System.getProperty("user.dir") + System.getProperty("file.separator") + archiveName);
			if (Files.exists(localArchive.toPath()))
				cleanUpLocal(localArchive.toPath());

			// Download the archive from the Grid
			//
			System.out.println("[" + new Date() + "] Downloading the archive from the Grid");
			commander.c_api.downloadFile(remoteArchive, localArchive, "-silent");
			if (!Files.exists(localArchive.toPath())) {
				System.err.println("[" + new Date() + "] " + remoteFile + ": Failed to download remote archive " + remoteArchive);
				validation.println("Download failed " + remoteArchive);
				return;
			}

			// Unpack to local directory and zip again without member file
			//
			System.out.println("[" + new Date() + "] Unpacking to local directory");
			if (!unzip(archiveName, memberName)) {
				System.err.println(
						"[" + new Date() + "] " + remoteFile + ": Failed to extract files from archive: " + System.getProperty("user.dir") + System.getProperty("file.separator") + archiveName);
				validation.println("Extraction failed " + remoteArchive);
				return;
			}
			cleanUpLocal(localArchive.toPath());

			final ArrayList<String> listOfFiles = getFileListing(Path.of(System.getProperty("user.dir"), "extracted"));

			System.out.println("[" + new Date() + "] Zipping the new archive");
			final OutputEntry entry = new OutputEntry(archiveName, listOfFiles, "", Long.valueOf(jobID));
			entry.createZip(System.getProperty("user.dir") + System.getProperty("file.separator") + "extracted");

			final String newArchiveFullPath = registerPath + "/" + archiveName;

			// Upload the new archive to the Grid
			//
			final File newArchive = new File(System.getProperty("user.dir") + System.getProperty("file.separator") + "extracted" + System.getProperty("file.separator") + archiveName);

			while (commander.c_api.getLFN(newArchiveFullPath) != null) {
				// Delete registertemp/root_archive.zip if there is any
				System.out.println("[" + new Date() + "] Deleting corrupted " + newArchiveFullPath);
				commander.c_api.removeLFN(newArchiveFullPath);
			}

			System.out.println("[" + new Date() + "] Uploading the new archive to the Grid: " + newArchiveFullPath);
			commander.c_api.uploadFile(newArchive, newArchiveFullPath, "-w", "-S", "disk:1"); // Create only one replica

			final LFN newArchiveLFN = commander.c_api.getLFN(newArchiveFullPath);
			if (newArchiveLFN == null || !newArchiveLFN.exists) {
				System.err.println("[" + new Date() + "] " + remoteFile + ": Failed to upload archive " + newArchiveFullPath);
				validation.println("Upload failed " + newArchiveFullPath);
				return;
			}

			// Register files in the catalogue
			//

			// Create subdirs (like BKG/)
			for (final String file : listOfFiles) {
				if (file.contains("/")) {
					if (commander.c_api.createCatalogueDirectory(registerPath + "/" + file.substring(0, file.lastIndexOf("/")), true) == null) {
						System.err.println("[" + new Date() + "] " + remoteFile + ": Failed to create new directory " + registerPath + "/" + file.substring(0, file.lastIndexOf("/")));
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
					System.err.println("[" + new Date() + "] " + remoteFile + ": Failed to register entry " + registerPath + "/" + file);
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
					System.err.println("[" + new Date() + "] " + remoteFile + ": Failed to delete old archive member " + member.getCanonicalName());
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
				System.err.println("[" + new Date() + "] " + remoteFile + ": Failed to delete old archive " + remoteArchive);
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

				if (pfn == null) {
					System.err.println("One of the PFNs in the removeArchivePFN is null");
					continue;
				}

				try {
					System.out.println("[" + new Date() + "] Deleting pfn: " + pfn.pfn);
					if (!Factory.xrootd.delete(pfn)) {
						System.err.println("[" + new Date() + "] " + remoteFile + ": Could not delete " + pfn.pfn);
					}
				}
				catch (final IOException e) {
					e.printStackTrace();
				}
			}

			// Create file marker to leave trace
			if (commander.c_api.touchLFN(parentdir + "/" + ".deleted" + (remoteArchiveLFN.getSize() - newArchive.length())) == null) {
				System.err.println("[" + new Date() + "] " + remoteFile + ": Could not create .deleted marker");
			}

			// Rename uploaded archive
			//
			System.out.println("[" + new Date() + "] Renaming uploaded archive");
			if (commander.c_api.moveLFN(registerPath + "/" + archiveName, remoteArchive) == null) {
				System.err.println("[" + new Date() + "] " + remoteFile + ": Failed to rename the archive " + registerPath + "/" + archiveName);

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
					System.err.println("[" + new Date() + "] " + remoteFile + ": Failed to rename archive member " + parentdir + "/" + file);

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

			System.out.println("[" + new Date() + "] " + memberName + " was " + remoteLFN.getSize() + " bytes");
			System.out.println("[" + new Date() + "] " + "Old archive was " + remoteArchiveLFN.getSize() + " bytes");
			System.out.println("[" + new Date() + "] " + "New archive is " + newArchive.length() + " bytes");
			System.out.println("[" + new Date() + "] " + "Reclaimed " + (remoteArchiveLFN.getSize() - newArchive.length()) + " bytes of disk space");

			// Clean up local files
			cleanUpLocal(Path.of(System.getProperty("user.dir"), "extracted"));
			cleanUpLocal(newArchive.toPath());
		}
		catch (final IOException e1) {
			System.err.println("[" + new Date() + "] " + remoteFile + ": I/O exception. Abort");
			e1.printStackTrace();
		}
		catch (final ServerException e1) {
			System.err.println("[" + new Date() + "] " + remoteFile + ": Could not get PFN. Abort");
			e1.printStackTrace();
		}
		catch (final OutOfMemoryError e1) {
			System.err.println("[" + new Date() + "] " + "Out of memory. Abort");
			e1.printStackTrace();
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
	private static boolean unzip(final String archiveName, final String memberName) throws IOException {

		final Path destDir = Path.of(System.getProperty("user.dir"), "extracted");
		if (!Files.exists(destDir)) {
			Files.createDirectories(destDir);
		}
		else {
			// Clean up temp directory if there are files in it
			try (Stream<Path> destDirEntries = Files.list(destDir)) {
				destDirEntries.forEach(ArchiveMemberDelete::cleanUpLocal);
			}
		}

		// Start unpacking the archive
		try (ZipInputStream zipIn = new ZipInputStream(new FileInputStream(System.getProperty("user.dir") + System.getProperty("file.separator") + archiveName))) {
			ZipEntry entry = zipIn.getNextEntry();
			// iterates over entries in the zip file
			while (entry != null) {
				if (entry.getName().contains(memberName)) {
					// Skip this file
					zipIn.closeEntry();
					entry = zipIn.getNextEntry();
					continue;
				}
				if (entry.getName().contains("AliESDfriends.root")) {
					// Also skip AliESDfriends.root
					zipIn.closeEntry();
					entry = zipIn.getNextEntry();
					continue;
				}

				final Path filePath = Path.of(destDir.toString(), entry.getName());
				if (!entry.isDirectory()) {
					// If the entry is a file, extract it
					Files.createDirectories(filePath.getParent());
					extractFile(zipIn, filePath.toString());
				}
				else {
					// If the entry is a directory, make the directory
					Files.createDirectories(filePath);
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
	private static ArrayList<String> getFileListing(final Path folderName) {
		try (Stream<Path> paths = Files.walk(folderName)) {
			if (paths.count() == 0)
				System.err.println("[" + new Date() + "] Failed to get list of files in local folder: " + folderName);

			return paths.map(path -> folderName.relativize(path).toString()).collect(Collectors.toCollection(ArrayList<String>::new));
		}
		catch (final IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Delete local file using <code>java.nio.Files#delete</code>, catch any <code>IOException</code>
	 * 
	 * @param path
	 *            path of the local file or directory to delete
	 */
	private static void cleanUpLocal(final Path path) {
		try {
			Files.delete(path);
		}
		catch (final IOException e) {
			e.printStackTrace();
		}
	}
}
