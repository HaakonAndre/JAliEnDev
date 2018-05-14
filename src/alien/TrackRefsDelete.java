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
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.CatalogueApiUtils;
import alien.api.catalogue.PFNforReadOrDel;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.XmlCollection;
import alien.catalogue.access.AccessType;
import alien.config.ConfigUtils;
import alien.io.protocols.Factory;
import alien.shell.commands.JAliEnCOMMander;
import alien.site.OutputEntry;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author yuw
 *
 */
public class TrackRefsDelete {

	static transient final Logger logger = ConfigUtils.getLogger(TrackRefsDelete.class.getCanonicalName());

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
			parser.accepts("list").withRequiredArg();		// Like "collection.xml"
			parser.accepts("archive").withRequiredArg(); 	// Like "root_archive.zip";
			parser.accepts("member").withRequiredArg();  	// Like "TrackRefs.root";
			final OptionSet options = parser.parse(args);

			// Read directory names from file
			final String collectionName = (String) options.valueOf("list");
			archiveName = (String) options.valueOf("archive");
			memberName = (String) options.valueOf("member");
			
			XmlCollection xmlCollection = new XmlCollection(new File(collectionName));

			Iterator<LFN> directorys = xmlCollection.iterator();
			while (directorys.hasNext()) {
				deleteArchiveMember(directorys.next().getCanonicalName());
			}
		}
	}

	private static void deleteArchiveMember(final String directory) {
		// TODO delete archive with 1 member
		try {
			final String jobID = directory.substring(directory.lastIndexOf("/") + 1);
			// Use this for debugging
			// final ByteArrayOutputStream out = new ByteArrayOutputStream();
			// commander.setLine(new JSONPrintWriter(out), null);

			// Create file variables
			//
			final String remotePath = FileSystemUtils.getAbsolutePath(commander.getUser().getName(), commander.getCurrentDirName(), directory);
			final String remoteFile = remotePath + System.getProperty("file.separator") + archiveName;

			File localFile = new File(System.getProperty("user.dir") + System.getProperty("file.separator") + jobID + archiveName);
			if (localFile.exists()) {
				localFile.delete();
			}

			// Check if remote zip file exists
			//
			if (commander.c_api.find(remotePath, archiveName, 0).isEmpty()) {
				System.out.println("There is no " + archiveName + " in " + remotePath);
				return;
			}

			// Check if member file is registered in the catalog
			//
			if (commander.c_api.find(remotePath, memberName, 0).isEmpty()) {
				System.out.println("There is no " + memberName + " in " + remotePath);
				return;
			}

			// Download the archive from the Grid
			//
			commander.c_api.downloadFile(remoteFile, localFile, "");

			// Unpack to local directory and zip again without member file
			//
			if (!unzip(jobID)) {
				System.err.println("Failed to extract files from archive: " + System.getProperty("user.dir") + System.getProperty("file.separator") + jobID + archiveName);
				System.err.println("Aborting.");
				return;
			}

			File folder = new File(System.getProperty("user.dir") + System.getProperty("file.separator") + jobID);
			ArrayList<String> listOfFiles = new ArrayList<>();
			for (File file : folder.listFiles())
				listOfFiles.add(file.getName());

			OutputEntry entry = new OutputEntry(archiveName, listOfFiles, "", Long.valueOf(jobID));
			entry.createZip(System.getProperty("user.dir") + System.getProperty("file.separator") + jobID);

			// Upload new file to the Grid
			//

			// Create exactly the same number of replicas as the original archive had
			int nreplicas = commander.c_api.getPFNsToRead(commander.c_api.getLFN(remoteFile), null, null).size();
			File newArchive = new File(System.getProperty("user.dir") + System.getProperty("file.separator") + jobID + System.getProperty("file.separator") + archiveName);
			commander.c_api.uploadFile(newArchive, remoteFile + ".new", "-w", "-S", "disk:" + nreplicas);

			// Delete the members links of old archive
			//
			for (String member : listOfFiles) {
				commander.c_api.removeLFN(remotePath + System.getProperty("file.separator") + member);
			}

			commander.c_api.removeLFN(remotePath + System.getProperty("file.separator") + memberName);

			// Delete old remote file
			//

			// Speed up things by calling xrootd delete directly
			LFN oldRemoteLFN = commander.c_api.getLFN(remoteFile);

			if (oldRemoteLFN != null) {
				try {
					List<PFN> oldRemotePFN = Dispatcher.execute(new PFNforReadOrDel(commander.getUser(), commander.getSite(), AccessType.DELETE, oldRemoteLFN, null, null)).getPFNs();

					Iterator<PFN> it = oldRemotePFN.iterator();
					while (it.hasNext()) {
						PFN pfn = it.next();

						try {
							if (!Factory.xrootd.delete(pfn)) {
								System.err.println("Could not delete: " + pfn);
								logger.log(Level.WARNING, "Could not delete: " + pfn);
							}
						} catch (final IOException e) {
							e.printStackTrace();
						}
					}
				} catch (final ServerException e) {
					logger.log(Level.WARNING, "Could not get PFN for: " + oldRemoteLFN);
					e.getCause().printStackTrace();
				}
			}

			// Still need to call rm to clean up possible db leftovers
			commander.c_api.removeLFN(remoteFile);

			// Rename uploaded file
			//
			commander.c_api.moveLFN(remoteFile + ".new", remoteFile);

			// Register files in the catalogue
			//
			CatalogueApiUtils.registerEntry(entry, remotePath + System.getProperty("file.separator"), commander.getUser());

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
	private static boolean unzip(final String jobID) throws IOException {

		File destDir = new File(System.getProperty("user.dir") + System.getProperty("file.separator") + jobID);
		if (!destDir.exists()) {
			destDir.mkdir();
		}
		else {
			// Clean up temp directory if there are files in it
			String[] destDirEntries = destDir.list();
			for (String s : destDirEntries) {
				File currentFile = new File(destDir.getPath(), s);
				currentFile.delete();
			}
		}

		// Start unpacking the archive
		try (ZipInputStream zipIn = new ZipInputStream(new FileInputStream(System.getProperty("user.dir") + System.getProperty("file.separator") + jobID + archiveName))) {
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
				}
				else {
					// if the entry is a directory, make the directory
					File dir = new File(filePath);
					dir.mkdir();
				}
				zipIn.closeEntry();
				entry = zipIn.getNextEntry();
			}

			zipIn.close();
			return true;
		} catch (FileNotFoundException e) {
			System.err.println("No such file: " + System.getProperty("user.dir") + System.getProperty("file.separator") + jobID + archiveName);
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
