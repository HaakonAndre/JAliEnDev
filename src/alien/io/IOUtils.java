package alien.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

import lazyj.Utils;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.io.protocols.Protocol;
import alien.io.protocols.TempFileManager;
import alien.se.SEUtils;
import alien.shell.commands.JAliEnCOMMander;
import alien.shell.commands.JAliEnCommandcp;
import alien.shell.commands.PlainWriter;
import alien.shell.commands.UIPrintWriter;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;

/**
 * Helper functions for IO
 * 
 * @author costing
 */
public class IOUtils {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(IOUtils.class.getCanonicalName());

	/**
	 * @param f
	 * @return the MD5 checksum of the entire file
	 * @throws IOException
	 */
	public static String getMD5(final File f) throws IOException {
		if (f == null || !f.isFile() || !f.canRead())
			throw new IOException("Cannot read from this file: " + f);

		DigestInputStream dis = null;

		try {
			final MessageDigest md = MessageDigest.getInstance("MD5");
			dis = new DigestInputStream(new FileInputStream(f), md);

			final byte[] buff = new byte[10240];

			int cnt;

			do {
				cnt = dis.read(buff);
			} while (cnt == buff.length);

			final byte[] digest = md.digest();

			return String.format("%032x", new BigInteger(1, digest));
		}
		catch (final IOException ioe) {
			throw ioe;
		}
		catch (final Exception e) {
			// ignore
		}
		finally {
			if (dis != null) {
				try {
					dis.close();
				}
				catch (final IOException ioe) {
					// ignore
				}
			}
		}

		return null;
	}

	/**
	 * Download the file in a temporary location. The GUID should be filled with authorization tokens before calling this method.
	 * 
	 * @param guid
	 * @return the temporary file name. You should handle the deletion of this temporary file!
	 * @see TempFileManager#release(File)
	 * @see #get(GUID, File)
	 * @see AuthorizationFactory#fillAccess(GUID, AccessType)
	 */
	public static File get(final GUID guid) {
		return get(guid, null);
	}

	private static final ThreadPoolExecutor PARALLEL_DW_THREAD_POOL = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 2L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());

	/**
	 * Download the file in a specified location. The GUID should be filled with authorization tokens before calling this method.
	 * 
	 * @param guid
	 * @param localFile
	 *            path where the file should be downloaded. Can be <code>null</code> in which case a temporary location will be used, but then you should handle the temporary files.
	 * @return the downloaded file, or <code>null</code> if the file could not be retrieved
	 * @see TempFileManager#release(File)
	 * @see AuthorizationFactory#fillAccess(GUID, AccessType)
	 */
	public static File get(final GUID guid, final File localFile) {
		final File cachedContent = TempFileManager.getAny(guid);

		if (cachedContent != null) {
			if (localFile == null)
				return cachedContent;

			try {
				if (!Utils.copyFile(cachedContent.getAbsolutePath(), localFile.getAbsolutePath())) {
					if (logger.isLoggable(Level.WARNING))
						logger.log(Level.WARNING, "Cannot copy " + cachedContent.getAbsolutePath() + " to " + localFile.getAbsolutePath());

					return null;
				}
			}
			finally {
				TempFileManager.release(cachedContent);
			}

			TempFileManager.putPersistent(guid, localFile);

			return localFile;
		}

		final Set<PFN> pfns = guid.getPFNs();

		if (pfns == null || pfns.size() == 0)
			return null;

		final Set<PFN> realPFNsSet = new HashSet<PFN>();

		boolean zipArchive = false;

		for (final PFN pfn : pfns) {
			if (pfn.pfn.startsWith("guid:/") && pfn.pfn.indexOf("?ZIP=") >= 0)
				zipArchive = true;

			final Set<PFN> realPfnsTemp = pfn.getRealPFNs();

			if (realPfnsTemp == null || realPfnsTemp.size() == 0)
				continue;

			for (final PFN realPFN : realPfnsTemp)
				realPFNsSet.add(realPFN);
		}

		final String site = ConfigUtils.getConfig().gets("alice_close_site", "CERN").trim();

		File f = null;

		if (realPFNsSet.size() > 1 && guid.size < ConfigUtils.getConfig().getl("alien.io.IOUtils.parallel_downloads.size_limit", 10 * 1024 * 1024)
				&& PARALLEL_DW_THREAD_POOL.getActiveCount() < ConfigUtils.getConfig().geti("alien.io.IOUtils.parallel_downloads.threads", 100)) {
			f = parallelDownload(guid, realPFNsSet, zipArchive ? null : localFile);
		}
		else {
			final List<PFN> sortedRealPFNs = SEUtils.sortBySite(realPFNsSet, site, false, false);

			for (final PFN realPfn : sortedRealPFNs) {
				if (realPfn.ticket == null) {
					logger.log(Level.WARNING, "Missing ticket for " + realPfn.pfn);
					continue; // no access to this guy
				}

				final List<Protocol> protocols = Transfer.getAccessProtocols(realPfn);

				if (protocols == null || protocols.size() == 0)
					continue;

				for (final Protocol protocol : protocols) {
					try {
						f = protocol.get(realPfn, zipArchive ? null : localFile);

						if (f != null)
							break;
					}
					catch (final IOException e) {
						logger.log(Level.INFO, "Failed to fetch " + realPfn.pfn + " by " + protocol, e);
					}
				}

				if (f != null)
					break;
			}
		}

		if (f == null || !zipArchive)
			return f;

		try {
			for (final PFN p : pfns) {
				if (p.pfn.startsWith("guid:/") && p.pfn.indexOf("?ZIP=") >= 0) {
					// this was actually an archive

					final String archiveFileName = p.pfn.substring(p.pfn.lastIndexOf('=') + 1);

					try {
						final ZipInputStream zi = new ZipInputStream(new FileInputStream(f));

						ZipEntry zipentry = zi.getNextEntry();

						File target = null;

						while (zipentry != null) {
							if (zipentry.getName().equals(archiveFileName)) {
								if (localFile != null) {
									target = localFile;
								}
								else {
									target = File.createTempFile(guid.guid + "#" + archiveFileName + ".", null, getTemporaryDirectory());
								}

								final FileOutputStream fos = new FileOutputStream(target);

								final byte[] buf = new byte[8192];

								int n;

								while ((n = zi.read(buf, 0, buf.length)) > -1)
									fos.write(buf, 0, n);

								fos.close();
								zi.closeEntry();
								break;
							}

							zipentry = zi.getNextEntry();
						}

						zi.close();

						if (target != null) {
							if (localFile == null)
								TempFileManager.putTemp(guid, target);
							else
								TempFileManager.putPersistent(guid, localFile);
						}

						return target;
					}
					catch (final ZipException e) {
						logger.log(Level.WARNING, "ZipException parsing the content of " + f.getAbsolutePath(), e);
					}
					catch (final IOException e) {
						logger.log(Level.WARNING, "IOException extracting " + archiveFileName + " from " + f.getAbsolutePath() + " to parse as ZIP", e);
					}

					return null;
				}
			}
		}
		finally {
			TempFileManager.release(f);
		}

		return null;
	}

	private static final class DownloadWork implements Runnable {
		private final PFN realPfn;
		private final Object lock;
		private File f;

		public DownloadWork(final PFN realPfn, final Object lock) {
			this.realPfn = realPfn;
			this.lock = lock;
		}

		@Override
		public void run() {
			final List<Protocol> protocols = Transfer.getAccessProtocols(realPfn);

			if (protocols == null || protocols.size() == 0)
				return;

			for (final Protocol protocol : protocols) {
				try {
					f = protocol.get(realPfn, null);

					if (f != null)
						break;
				}
				catch (final IOException e) {
					logger.log(Level.FINE, "Failed to fetch " + realPfn.pfn + " by " + protocol, e);
				}
			}

			if (f != null)
				synchronized (lock) {
					lock.notifyAll();
				}
		}

		public File getLocalFile() {
			return f;
		}

		public PFN getPFN() {
			return realPfn;
		}
	}

	private static File parallelDownload(final GUID guid, final Set<PFN> realPFNsSet, final File localFile) {
		final List<Future<DownloadWork>> parallelDownloads = new ArrayList<Future<DownloadWork>>(realPFNsSet.size());

		final Object lock = new Object();

		File f = null;

		for (final PFN realPfn : realPFNsSet) {
			if (realPfn.ticket == null) {
				logger.log(Level.WARNING, "Missing ticket for " + realPfn.pfn);
				continue; // no access to this guy
			}

			final DownloadWork dw = new DownloadWork(realPfn, lock);

			final Future<DownloadWork> future = PARALLEL_DW_THREAD_POOL.submit(dw, dw);

			parallelDownloads.add(future);
		}

		while (f == null) {
			final Iterator<Future<DownloadWork>> it = parallelDownloads.iterator();

			while (it.hasNext()) {
				final Future<DownloadWork> future = it.next();

				if (future.isDone()) {
					try {
						final DownloadWork dw = future.get();

						f = dw.getLocalFile();

						if (logger.isLoggable(Level.FINER)) {
							if (f != null)
								logger.log(Level.FINER, "The first replica to reply was: " + dw.getPFN().pfn);
							else
								logger.log(Level.FINER, "This replica was not accessible: " + dw.getPFN().pfn);
						}
					}
					catch (final InterruptedException e) {
						e.printStackTrace();
					}
					catch (final ExecutionException e) {
						e.printStackTrace();
					}
					finally {
						it.remove();
					}

					if (f != null)
						break;
				}
			}

			if (f == null) {
				synchronized (lock) {
					try {
						lock.wait(100);
					}
					catch (final InterruptedException e) {
						break;
					}
				}
			}
		}

		for (final Future<DownloadWork> future : parallelDownloads) {
			future.cancel(true);
		}

		if (localFile != null) {
			if (lazyj.Utils.copyFile(f.getAbsolutePath(), localFile.getAbsolutePath())) {
				TempFileManager.putPersistent(guid, localFile);

				return localFile;
			}
		}

		return f;
	}

	/**
	 * @param guid
	 * @return the contents of the file, or <code>null</code> if there was a problem getting it
	 */
	public static String getContents(final GUID guid) {
		final String reason = AuthorizationFactory.fillAccess(guid, AccessType.READ);

		if (reason != null) {
			logger.log(Level.WARNING, "Access denied: " + reason);

			return null;
		}

		final File f = get(guid);

		if (f != null) {
			try {
				return Utils.readFile(f.getCanonicalPath());
			}
			catch (final IOException ioe) {
				// ignore, shouldn't be ...
			}
			finally {
				TempFileManager.release(f);
			}
		}

		return null;
	}

	/**
	 * @param lfn
	 * @return the contents of the file, or <code>null</code> if there was a problem getting it
	 */
	public static String getContents(final LFN lfn) {
		if (lfn == null)
			return null;

		final GUID g = GUIDUtils.getGUID(lfn);

		if (g == null)
			return null;

		return getContents(g);
	}

	/**
	 * @param lfn
	 * @return the contents of the file, or <code>null</code> if there was a problem getting it
	 */
	public static String getContents(final String lfn) {
		return getContents(LFNUtils.getLFN(lfn));
	}

	/**
	 * @param lfn
	 *            relative paths are allowed
	 * @param owner
	 * @return <code>true</code> if the indicated LFN doesn't exist (any more) in the catalogue and can be created again
	 */
	public static boolean backupFile(final String lfn, final AliEnPrincipal owner) {
		final String absolutePath = FileSystemUtils.getAbsolutePath(owner.getName(), null, lfn);

		final LFN l = LFNUtils.getLFN(absolutePath, true);

		if (!l.exists) {
			return true;
		}

		final LFN backupLFN = LFNUtils.getLFN(absolutePath + "~", true);

		if (backupLFN.exists && AuthorizationChecker.canWrite(backupLFN.getParentDir(), owner)) {
			if (!backupLFN.delete(true, false))
				return false;
		}

		return LFNUtils.mvLFN(owner, l, absolutePath + "~") != null;
	}

	/**
	 * Upload a local file to the Grid
	 * 
	 * @param localFile
	 * @param toLFN
	 * @param owner
	 * @throws IOException
	 */
	public static void upload(final File localFile, final String toLFN, final AliEnPrincipal owner) throws IOException {
		upload(localFile, toLFN, owner, 2, null, false);
	}

	/**
	 * Upload a local file to the Grid
	 * 
	 * @param localFile
	 * @param toLFN
	 * @param owner
	 * @param replicaCount
	 * @throws IOException
	 */
	public static void upload(final File localFile, final String toLFN, final AliEnPrincipal owner, final int replicaCount) throws IOException {
		upload(localFile, toLFN, owner, replicaCount, null, false);
	}
	
	/**
	 * Upload a local file to the Grid
	 * 
	 * @param localFile
	 * @param toLFN
	 * @param owner
	 * @param replicaCount
	 * @param progressReport
	 * @throws IOException
	 */
	public static void upload(final File localFile, final String toLFN, final AliEnPrincipal owner, final int replicaCount, final OutputStream progressReport) throws IOException {
		upload(localFile, toLFN, owner, replicaCount, progressReport, false);
	}

	/**
	 * Upload a local file to the Grid
	 * 
	 * @param localFile
	 * @param toLFN
	 * @param owner
	 * @param replicaCount
	 * @param progressReport
	 * @param deleteSourceAfterUpload if <code>true</code> then the local file (the source) is to be deleted after the operation completes 
	 * @throws IOException
	 */
	public static void upload(final File localFile, final String toLFN, final AliEnPrincipal owner, final int replicaCount, final OutputStream progressReport, final boolean deleteSourceAfterUpload) throws IOException {
		final String absolutePath = FileSystemUtils.getAbsolutePath(owner.getName(), null, toLFN);

		final LFN l = LFNUtils.getLFN(absolutePath, true);

		if (l.exists) {
			throw new IOException("LFN already exists: " + toLFN);
		}

		final ArrayList<String> cpArgs = new ArrayList<String>();
		cpArgs.add("file:" + localFile.getAbsolutePath());
		cpArgs.add(absolutePath);
		
		if (deleteSourceAfterUpload)
			cpArgs.add("-d");
		
		cpArgs.add("-S");
		cpArgs.add("disk:" + replicaCount);

		final UIPrintWriter out = progressReport != null ? new PlainWriter(progressReport) : null;

		final JAliEnCOMMander cmd = new JAliEnCOMMander(owner, owner.getName(), null, ConfigUtils.getConfig().gets("alice_close_site", "CERN").trim(), out);

		final JAliEnCommandcp cp = new JAliEnCommandcp(cmd, out, cpArgs);

		cp.copyLocalToGrid(localFile, absolutePath);
	}

	/**
	 * @return the temporary directory where downloaded files are put by default
	 */
	public static final File getTemporaryDirectory() {
		final String sDir = ConfigUtils.getConfig().gets("alien.io.IOUtils.tempDownloadDir");

		if (sDir == null || sDir.length() == 0)
			return null;

		final File f = new File(sDir);

		if (f.exists() && f.isDirectory() && f.canWrite())
			return f;

		return null;
	}
}
