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
	private final static JAliEnCOMMander commander = JAliEnCOMMander.getInstance();
	private static String archiveName;
	private static String memberName;

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(final String[] args) throws Exception {
		if (args.length > 0) {
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

			final File validation = new File("validation_error.message");
			if (validation.length() == 0)
				validation.delete();
		}
	}

	private static void deleteArchiveMember(final String xmlEntry) {
		// Use this for debugging
		// final ByteArrayOutputStream out = new ByteArrayOutputStream();
		// commander.setLine(new JSONPrintWriter(out), null);
		System.out.println();

		LFN remoteLFN = null;
		try {
			remoteLFN = commander.c_api.getLFN(xmlEntry);
		} catch (NullPointerException e) {
			System.err.println("[" + new Date() + "] Something went wrong. Abort.");
			e.printStackTrace();
			return;
		}

		if (remoteLFN == null || !remoteLFN.exists) {
			if (!commander.c_api.find(xmlEntry.substring(0, xmlEntry.lastIndexOf("/")), ".deleted", 0).isEmpty())
				System.out.println("[" + new Date() + "]  " + xmlEntry + ": already deleted, continue.");
			else
				System.err.println("[" + new Date() + "] " + xmlEntry + ": LFN doesn't exist. Abort.");

			return;
		}

		final String remoteFile = remoteLFN.getCanonicalName();
		final String remotePath = remoteLFN.getParentName();
		final long remoteFileSize = remoteLFN.getSize();

		System.out.println("[" + new Date() + "] Processing " + remoteFile);

		// If the file is not a member of any archives, just delete it
		if (remoteLFN.equals(commander.c_api.getRealLFN(remoteFile))) {
			System.out.println("[" + new Date() + "] " + remoteFile + " is a real file, we'll simply delete it");

			// Speed up things by calling xrootd delete directly
			try {
				final List<PFN> remotePFN = Dispatcher.execute(new PFNforReadOrDel(commander.getUser(), commander.getSite(), AccessType.DELETE, remoteLFN, null, null)).getPFNs();

				final Iterator<PFN> it = remotePFN.iterator();
				while (it.hasNext()) {
					PFN pfn = it.next();

					try {
						if (!Factory.xrootd.delete(pfn)) {
							System.err.println("[" + new Date() + "] " + remoteFile + ": Could not delete " + pfn.pfn);
						}
					} catch (final IOException e) {
						e.printStackTrace();
					}
				}
			} catch (final ServerException e) {
				System.err.println("[" + new Date() + "] " + remoteFile + ": Could not get PFN");
				e.printStackTrace();
			}

			commander.c_api.removeLFN(remoteFile);
			commander.c_api.touchLFN(remotePath + System.getProperty("file.separator") + ".deleted" + remoteFileSize);

			System.out.println("[" + new Date() + "] " + remoteFile);
			System.out.println("[" + new Date() + "] Reclaimed " + remoteFileSize + " bytes of disk space");
			return;
		}

		// Main procedure
		//
		try (final PrintWriter validation = new PrintWriter(new FileOutputStream("validation_error.message", true))) {
			// Archive variables
			final LFN remoteArchiveLFN = commander.c_api.getRealLFN(remoteFile);
			if (remoteArchiveLFN == null || !remoteArchiveLFN.exists) {
				System.err.println("[" + new Date() + "] " + remoteFile + ": Archive not found in parent dir");
				validation.println("File not found");
				return;
			}

			final String remoteArchive = remoteArchiveLFN.getCanonicalName();
			archiveName = remoteArchiveLFN.getFileName();
			memberName = remoteLFN.getFileName();
			final long jobID = remoteArchiveLFN.jobid;

			final List<LFN> remoteArchiveMembers = commander.c_api.getArchiveMembers(remoteArchive);
			if (remoteArchiveMembers.size() == 1) {
				// RemoteLFN is the only file in remoteArchive
				// No point in downloading it, just remove file and archive

				System.out.println("[" + new Date() + "] Deleting remote file");
				commander.c_api.removeLFN(remoteFile);

				System.out.println("[" + new Date() + "] Deleting old remote archive");

				// Remove physical replicas of the old archive
				try {
					final List<PFN> remotePFN = Dispatcher.execute(new PFNforReadOrDel(commander.getUser(), commander.getSite(), AccessType.DELETE, remoteArchiveLFN, null, null)).getPFNs();

					final Iterator<PFN> it = remotePFN.iterator();
					while (it.hasNext()) {
						final PFN pfn = it.next();

						try {
							if (!Factory.xrootd.delete(pfn)) {
								System.err.println("[" + new Date() + "] " + remoteFile + ": Could not delete " + pfn.pfn);
							}
						} catch (final IOException e) {
							e.printStackTrace();
						}
					}
				} catch (final ServerException e) {
					System.err.println("[" + new Date() + "] " + remoteFile + ": Could not get PFN for " + remoteArchive);
					e.printStackTrace();
				}

				// Remove lfn of the old archive
				commander.c_api.removeLFN(remoteArchive);

				// Create file marker to leave trace
				commander.c_api.touchLFN(remotePath + System.getProperty("file.separator") + ".deleted" + remoteArchiveLFN.getSize());

				System.out.println("[" + new Date() + "] " + remoteFile);
				System.out.println("[" + new Date() + "] " + memberName + " was " + remoteLFN.getSize() + " bytes");
				System.out.println("[" + new Date() + "] " + "Old archive was " + remoteArchiveLFN.getSize() + " bytes");
				System.out.println("[" + new Date() + "] " + "Reclaimed " + remoteArchiveLFN.getSize() + " bytes of disk space");

				return;
			}

			final File localArchive = new File(System.getProperty("user.dir") + System.getProperty("file.separator") + archiveName);
			if (localArchive.exists()) {
				localArchive.delete();
			}

			// Download the archive from the Grid
			//
			System.out.println("[" + new Date() + "] Downloading the archive from the Grid");
			commander.c_api.downloadFile(remoteArchive, localArchive, "-silent");
			if (!localArchive.exists()) {
				System.err.println("[" + new Date() + "] " + remoteFile + ": Failed to download remote archive " + remoteArchive);
				validation.println("Download failed");
				return;
			}

			// Unpack to local directory and zip again without member file
			//
			System.out.println("[" + new Date() + "] Unpacking to local directory");
			if (!unzip()) {
				System.err.println(
						"[" + new Date() + "] " + remoteFile + ": Failed to extract files from archive: " + System.getProperty("user.dir") + System.getProperty("file.separator") + archiveName);
				validation.println("Extraction failed");
				return;
			}
			localArchive.delete();

			final File folder = new File(System.getProperty("user.dir") + System.getProperty("file.separator") + "extracted");
			final ArrayList<String> listOfFiles = new ArrayList<>();
			final File[] listing = folder.listFiles();
			if (listing == null) {
				System.err.println("[" + new Date() + "] " + remoteFile + ": Failed to get list of files in local folder. Break");
				return;
			}
			for (final File file : listing)
				listOfFiles.add(file.getName());

			System.out.println("[" + new Date() + "] Zipping the new archive");
			final OutputEntry entry = new OutputEntry(archiveName, listOfFiles, "", Long.valueOf(jobID));
			entry.createZip(System.getProperty("user.dir") + System.getProperty("file.separator") + "extracted");

			// Upload the new archive to the Grid
			//
			System.out.println("[" + new Date() + "] Uploading the new archive to the Grid");

			// Create exactly the same number of replicas as the original archive had
			final int nreplicas = commander.c_api.getPFNsToRead(remoteArchiveLFN, null, null).size();
			final File newArchive = new File(System.getProperty("user.dir") + System.getProperty("file.separator") + "extracted" + System.getProperty("file.separator") + archiveName);
			commander.c_api.uploadFile(newArchive, remoteArchive + ".new", "-w", "-S", "disk:" + nreplicas);

			if (commander.c_api.getLFN(remoteArchive + ".new") == null || !commander.c_api.getLFN(remoteArchive + ".new").exists) {
				System.err.println("[" + new Date() + "] " + remoteFile + ": Failed to upload archive " + remoteArchive + ".new");
				validation.println("Upload failed");
				return;
			}

			// Delete the members links of old archive
			//
			System.out.println("[" + new Date() + "] Deleting the members links of old archive");
			for (final LFN member : remoteArchiveMembers) {
				commander.c_api.removeLFN(member.getCanonicalName());
			}

			// Delete old remote archive
			//
			System.out.println("[" + new Date() + "] Deleting old remote archive");

			// Remove physical replicas of the old archive
			try {
				final List<PFN> remotePFN = Dispatcher.execute(new PFNforReadOrDel(commander.getUser(), commander.getSite(), AccessType.DELETE, remoteArchiveLFN, null, null)).getPFNs();

				final Iterator<PFN> it = remotePFN.iterator();
				while (it.hasNext()) {
					final PFN pfn = it.next();

					try {
						if (!Factory.xrootd.delete(pfn)) {
							System.err.println("[" + new Date() + "] " + remoteFile + ": Could not delete " + pfn.pfn);
						}
					} catch (final IOException e) {
						e.printStackTrace();
					}
				}
			} catch (final ServerException e) {
				System.err.println("[" + new Date() + "] " + remoteFile + ": Could not get PFN for " + remoteArchive);
				e.printStackTrace();
			}

			// Remove lfn of the old archive
			commander.c_api.removeLFN(remoteArchive);

			// Rename uploaded archive
			//
			System.out.println("[" + new Date() + "] Renaming uploaded archive");
			commander.c_api.moveLFN(remoteArchive + ".new", remoteArchive);

			if (commander.c_api.getLFN(remoteArchive) == null || !commander.c_api.getLFN(remoteArchive).exists) {
				System.err.println("[" + new Date() + "] " + remoteFile + ": Failed to rename the archive " + remoteArchive);
				validation.println("Rename failed");
				return;
			}

			// Register files in the catalogue
			//
			System.out.println("[" + new Date() + "] Registering files in the catalogue");
			CatalogueApiUtils.registerEntry(entry, remotePath + System.getProperty("file.separator"), commander.getUser());

			for (final String file : listOfFiles) {
				if (commander.c_api.getLFN(remotePath + System.getProperty("file.separator") + file) == null
						|| !commander.c_api.getLFN(remotePath + System.getProperty("file.separator") + file).exists) {
					System.err.println("[" + new Date() + "] " + remoteFile + ": Failed to register entry " + remotePath + file);
					validation.println("Register failed");
					return;
				}
			}

			// Set jobID for the new archive LFN
			commander.c_api.getLFN(remoteArchive).jobid = jobID;

			// Create file marker to leave trace
			commander.c_api.touchLFN(remotePath + System.getProperty("file.separator") + ".deleted" + (remoteArchiveLFN.getSize() - newArchive.length()));

			System.out.println("[" + new Date() + "] " + remoteFile);
			System.out.println("[" + new Date() + "] " + memberName + " was " + remoteLFN.getSize() + " bytes");
			System.out.println("[" + new Date() + "] " + "Old archive was " + remoteArchiveLFN.getSize() + " bytes");
			System.out.println("[" + new Date() + "] " + "New archive is " + newArchive.length() + " bytes");
			System.out.println("[" + new Date() + "] " + "Gained " + (remoteArchiveLFN.getSize() - newArchive.length()) + " bytes of disk space");

			// Clean up
			final File destDir = new File(System.getProperty("user.dir") + System.getProperty("file.separator") + "extracted");
			if (!destDir.exists()) {
				return;
			}
			final String[] destDirEntries = destDir.list();
			if (destDirEntries != null)
				for (final String s : destDirEntries) {
					final File currentFile = new File(destDir.getPath(), s);
					currentFile.delete();
				}
			destDir.delete();
			newArchive.delete();

		} catch (IOException e1) {
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
	private static boolean unzip() throws IOException {

		final File destDir = new File(System.getProperty("user.dir") + System.getProperty("file.separator") + "extracted");
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
		try (final ZipInputStream zipIn = new ZipInputStream(new FileInputStream(System.getProperty("user.dir") + System.getProperty("file.separator") + archiveName))) {
			ZipEntry entry = zipIn.getNextEntry();
			// iterates over entries in the zip file
			while (entry != null) {
				if (entry.getName().contains(memberName)) {
					// Skip this file
					zipIn.closeEntry();
					entry = zipIn.getNextEntry();
					continue;
				}

				final String filePath = destDir.getAbsolutePath() + System.getProperty("file.separator") + entry.getName();
				if (!entry.isDirectory()) {
					// if the entry is a file, extracts it
					extractFile(zipIn, filePath);
				}
				else {
					// if the entry is a directory, make the directory
					final File dir = new File(filePath);
					dir.mkdir();
				}
				System.out.println("[" + new Date() + "] - extracted " + entry.getName());
				zipIn.closeEntry();
				entry = zipIn.getNextEntry();
			}

			zipIn.close();
			return true;
		} catch (@SuppressWarnings("unused") FileNotFoundException e) {
			System.err.println("No such file: " + System.getProperty("user.dir") + System.getProperty("file.separator") + archiveName);
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
	private static void extractFile(ZipInputStream zipIn, final String filePath) throws IOException {
		try (final BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath))) {
			byte[] bytesIn = new byte[BUFFER_SIZE];
			int read = 0;
			while ((read = zipIn.read(bytesIn)) != -1) {
				bos.write(bytesIn, 0, read);
			}
			bos.close();
		}
	}
}
