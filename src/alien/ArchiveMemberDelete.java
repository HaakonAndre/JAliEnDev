package alien;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
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

			// Read directory names from file
			final String collectionName = (String) options.valueOf("list");

			File collectionFile = new File(collectionName);
			if (!collectionFile.exists()) {
				System.err.println("Couldn't open the collection! File " + collectionName + " doesn't exist");
				return;
			}
			XmlCollection xmlCollection = new XmlCollection(collectionFile);

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
		}
	}

	private static void deleteArchiveMember(final String xmlEntry) {
		// TODO delete archive with 1 member
		// TODO create sh run script > stdout.log
		// TODO send messages for validation script
		LFN remoteLFN = commander.c_api.getLFN(xmlEntry);

		if (!remoteLFN.exists) {
			System.err.println("LFN doesn't exist: " + remoteLFN.getCanonicalName());
			return;
		}

		final String remoteFile = remoteLFN.getCanonicalName();

		System.out.println("Processing " + remoteFile);

		// If the file is not a member of any archives, just delete it
		if (remoteLFN.isReal()) {
			System.out.println("\t" + remoteFile + "is a real file, we'll simply delete it");

			// Speed up things by calling xrootd delete directly
			try {
				List<PFN> remotePFN = Dispatcher.execute(new PFNforReadOrDel(commander.getUser(), commander.getSite(),
						AccessType.DELETE, remoteLFN, null, null)).getPFNs();

				Iterator<PFN> it = remotePFN.iterator();
				while (it.hasNext()) {
					PFN pfn = it.next();

					try {
						if (!Factory.xrootd.delete(pfn)) {
							System.err.println("\tCould not delete: " + pfn.pfn);
						}
					} catch (final IOException e) {
						e.printStackTrace();
					}
				}
			} catch (final ServerException e) {
				System.err.println("\tCould not get PFN for: " + remoteFile);
				e.printStackTrace();
			}

			commander.c_api.removeLFN(remoteFile);
			System.out.println(remoteFile + " done\n");
			return;
		}

		try {
			final LFN remoteArchiveLFN = commander.c_api.getRealLFN(remoteFile);
			final String remotePath = remoteArchiveLFN.getParentName();
			final String remoteArchive = remoteArchiveLFN.getCanonicalName();
			archiveName = remoteArchiveLFN.getFileName();
			memberName = remoteLFN.getFileName();
			final long jobID = remoteArchiveLFN.jobid;

			if (!remoteLFN.exists) {
				System.err.println("\tThere is no " + archiveName + " in " + remotePath);
				return;
			}

			// Use this for debugging
			// final ByteArrayOutputStream out = new ByteArrayOutputStream();
			// commander.setLine(new JSONPrintWriter(out), null);

			File localArchive = new File(
					System.getProperty("user.dir") + System.getProperty("file.separator") + jobID + archiveName);
			if (localArchive.exists()) {
				localArchive.delete();
			}

			// Download the archive from the Grid
			//
			System.out.println("\tDownloading the archive from the Grid");
			commander.c_api.downloadFile(remoteArchive, localArchive, "-silent");
			if (!localArchive.exists()) {
				System.err.println("\tFailed to download remote archive " + remoteArchive);
				return;
			}

			// Unpack to local directory and zip again without member file
			//
			System.out.println("\tUnpacking to local directory");
			if (!unzip(jobID)) {
				System.err.println("\tFailed to extract files from archive: " + System.getProperty("user.dir")
						+ System.getProperty("file.separator") + jobID + archiveName);
				return;
			}

			File folder = new File(System.getProperty("user.dir") + System.getProperty("file.separator") + jobID);
			ArrayList<String> listOfFiles = new ArrayList<>();
			for (File file : folder.listFiles())
				listOfFiles.add(file.getName());

			System.out.println("\tZipping the new archive");
			OutputEntry entry = new OutputEntry(archiveName, listOfFiles, "", Long.valueOf(jobID));
			entry.createZip(System.getProperty("user.dir") + System.getProperty("file.separator") + jobID);

			// Upload the new archive to the Grid
			//

			System.out.println("\tUploading the new archive to the Grid");

			// Create exactly the same number of replicas as the original archive had
			int nreplicas = commander.c_api.getPFNsToRead(remoteArchiveLFN, null, null).size();
			File newArchive = new File(System.getProperty("user.dir") + System.getProperty("file.separator") + jobID
					+ System.getProperty("file.separator") + archiveName);
			commander.c_api.uploadFile(newArchive, remoteArchive + ".new", "-w", "-S", "disk:" + nreplicas);

			// Delete the members links of old archive
			//
			System.out.println("\tDeleting the members links of old archive");
			for (String otherMembers : listOfFiles) {
				commander.c_api.removeLFN(remotePath + System.getProperty("file.separator") + otherMembers);
			}

			commander.c_api.removeLFN(remoteFile);

			// Delete old remote archive
			//
			System.out.println("\tDeleting old remote archive");

			// Speed up things by calling xrootd delete directly
			try {
				List<PFN> remotePFN = Dispatcher.execute(new PFNforReadOrDel(commander.getUser(), commander.getSite(),
						AccessType.DELETE, remoteArchiveLFN, null, null)).getPFNs();

				Iterator<PFN> it = remotePFN.iterator();
				while (it.hasNext()) {
					PFN pfn = it.next();

					try {
						if (!Factory.xrootd.delete(pfn)) {
							System.err.println("\tCould not delete: " + pfn.pfn);
						}
					} catch (final IOException e) {
						e.printStackTrace();
					}
				}
			} catch (final ServerException e) {
				System.err.println("\tCould not get PFN for: " + remoteArchive);
				e.getCause().printStackTrace();
			}

			// Still need to call rm to clean up possible db leftovers
			commander.c_api.removeLFN(remoteArchive);

			// Rename uploaded archive
			//
			System.out.println("\tRenaming uploaded archive");
			commander.c_api.moveLFN(remoteArchive + ".new", remoteArchive);

			// Register files in the catalogue
			//
			System.out.println("\tRegistering files in the catalogue");
			CatalogueApiUtils.registerEntry(entry, remotePath + System.getProperty("file.separator"),
					commander.getUser());

			// Set jobID for the new archive LFN
			commander.c_api.getLFN(remoteArchive).jobid = jobID;

			System.out.println(remoteFile + " done\n");
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
	private static boolean unzip(final long jobID) throws IOException {

		File destDir = new File(System.getProperty("user.dir") + System.getProperty("file.separator") + jobID);
		if (!destDir.exists()) {
			destDir.mkdir();
		} else {
			// Clean up temp directory if there are files in it
			String[] destDirEntries = destDir.list();
			for (String s : destDirEntries) {
				File currentFile = new File(destDir.getPath(), s);
				currentFile.delete();
			}
		}

		// Start unpacking the archive
		try (ZipInputStream zipIn = new ZipInputStream(new FileInputStream(
				System.getProperty("user.dir") + System.getProperty("file.separator") + jobID + archiveName))) {
			ZipEntry entry = zipIn.getNextEntry();
			// iterates over entries in the zip file
			while (entry != null) {
				if (entry.getName().contains(memberName)) {
					// Skip this file
					zipIn.closeEntry();
					entry = zipIn.getNextEntry();
					continue;
				}

				String filePath = destDir.getAbsolutePath() + System.getProperty("file.separator") + entry.getName();
				if (!entry.isDirectory()) {
					// if the entry is a file, extracts it
					extractFile(zipIn, filePath);
				} else {
					// if the entry is a directory, make the directory
					File dir = new File(filePath);
					dir.mkdir();
				}
				zipIn.closeEntry();
				entry = zipIn.getNextEntry();
			}

			zipIn.close();
			return true;
		} catch (@SuppressWarnings("unused") FileNotFoundException e) {
			System.err.println("No such file: " + System.getProperty("user.dir") + System.getProperty("file.separator")
					+ jobID + archiveName);
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
		try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath))) {
			byte[] bytesIn = new byte[BUFFER_SIZE];
			int read = 0;
			while ((read = zipIn.read(bytesIn)) != -1) {
				bos.write(bytesIn, 0, read);
			}
			bos.close();
		}
	}
}
