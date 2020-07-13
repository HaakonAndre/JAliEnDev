package alien.shell.commands;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.PFNforReadOrDel;
import alien.api.catalogue.PFNforWrite;
import alien.catalogue.BookingTable.BOOKING_STATE;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessTicket;
import alien.catalogue.access.AccessType;
import alien.io.protocols.Factory;
import alien.io.protocols.SpaceInfo;
import alien.io.protocols.TempFileManager;
import alien.io.protocols.Xrootd;
import alien.se.SE;
import alien.se.SEUtils;
import alien.shell.ErrNo;
import alien.shell.ShellColor;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 *
 */
public class JAliEnCommandtestSE extends JAliEnBaseCommand {

	private boolean verbose = false;

	private boolean showCommand = false;

	private final List<SE> sesToTest = new ArrayList<>();

	private static File getReferenceFile() throws IOException {
		// try some small files usually found on any system
		for (final String existingFile : new String[] { "/etc/hostname", "/etc/hosts", "/etc/timezone" }) {
			final File f = new File(existingFile);

			if (f.exists() && f.canRead())
				return f;
		}

		final File f = File.createTempFile("testSE", ".tmp");

		try (PrintWriter pw = new PrintWriter(f)) {
			pw.println("Some not so random content");
		}

		return f;
	}

	private static final String expected = " (" + ShellColor.jobStateGreen() + "expected" + ShellColor.reset() + ")";

	private static final String notOK = " (" + ShellColor.jobStateRed() + "NOT OK" + ShellColor.reset() + ")";

	private void openReadTest(final PFN pTarget, final Xrootd xrootd) {
		final AccessTicket oldTicket = pTarget.ticket;

		pTarget.ticket = null;

		try {
			commander.printOut("  Open read test: ");

			File tempFile = null;
			try {
				tempFile = xrootd.get(pTarget, null);
				commander.printOutln("reading worked" + notOK + " please check authorization configuration");
			}
			catch (final IOException ioe) {
				commander.printOutln("read back failed" + expected);

				if (verbose)
					commander.printOutln("    " + ioe.getMessage());
			}
			finally {
				if (tempFile != null) {
					TempFileManager.release(tempFile);
					tempFile.delete();
				}
			}

			if (showCommand)
				commander.printOutln(xrootd.getFormattedLastCommand());
		}
		finally {
			pTarget.ticket = oldTicket;
		}
	}

	private boolean openDeleteTest(final PFN pTarget, final Xrootd xrootd) {
		final AccessTicket oldTicket = pTarget.ticket;

		pTarget.ticket = null;

		try {
			commander.printOut("  Open delete test: ");

			try {
				if (xrootd.delete(pTarget, false)) {
					commander.printOutln("delete worked" + notOK);
					return true;
				}

				commander.printOutln("delete failed" + expected);
			}
			catch (final IOException ioe) {
				commander.printOutln("delete failed" + expected);

				if (verbose)
					commander.printOutln("    " + ioe.getMessage());
			}

			if (showCommand)
				System.err.println(xrootd.getFormattedLastCommand());
		}
		finally {
			pTarget.ticket = oldTicket;
		}

		return false;
	}

	@Override
	public void run() {
		final File referenceFile;
		final GUID g;
		final LFN lfn;

		try {
			referenceFile = getReferenceFile();
			g = GUIDUtils.createGuid(referenceFile, commander.getUser());
			lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), "testSE_" + System.currentTimeMillis()), true);
		}
		catch (final IOException ioe) {
			commander.setReturnCode(ErrNo.EIO, "Cannot pick a file to test with: " + ioe.getMessage());
			return;
		}

		lfn.guid = g.guid;
		lfn.size = g.size;
		lfn.md5 = g.md5;
		lfn.ctime = g.ctime;
		lfn.type = 'f';
		g.lfnCache = new LinkedHashSet<>(1);
		g.lfnCache.add(lfn);

		boolean openReadTested = false;
		boolean openDeleteTested = false;

		try {
			for (final SE se : sesToTest) {
				commander.printOutln(se.getName());
				PFNforWrite pfnForWrite;
				PFN pTarget;

				try {
					pfnForWrite = Dispatcher
							.execute(new PFNforWrite(commander.getUser(), commander.getSite(), lfn, g, Arrays.asList(se.getName()), null, new HashMap<>()));

					pTarget = pfnForWrite.getPFNs().iterator().next();
				}
				catch (final ServerException ex) {
					System.err.println("Could not get a write envelope for this entry: " + ex.getMessage());

					continue;
				}

				commander.printOut("  Open write test: ");

				final Xrootd xrootd = (Xrootd) Factory.xrootd.clone();

				boolean wasAdded = false;

				final AccessTicket writeTicket = pTarget.ticket;
				pTarget.ticket = null;

				try {
					xrootd.put(pTarget, referenceFile, false);

					commander.printOutln("could write, " + ShellColor.jobStateRed() + "NOT OK" + ShellColor.reset() + ", please check authorization configuration");

					wasAdded = true;

					commander.c_api.registerEnvelopes(Arrays.asList(writeTicket.envelope.getEncryptedEnvelope()), BOOKING_STATE.COMMITED);
				}
				catch (final IOException ioe) {
					commander.printOutln("cannot write" + expected);

					if (verbose)
						commander.printOutln("    " + ioe.getMessage());
				}

				if (showCommand)
					commander.printOutln(xrootd.getFormattedLastCommand());

				if (wasAdded) {
					openReadTest(pTarget, xrootd);

					openReadTested = true;

					openDeleteTest(pTarget, xrootd);

					openDeleteTested = true;
				}

				// now let's try this with the proper access tokens

				pTarget.ticket = writeTicket;

				commander.printOut("  Authenticated write test: ");

				wasAdded = false;

				try {
					xrootd.put(pTarget, referenceFile);

					commander.printOutln("could write" + expected);

					wasAdded = true;

					commander.c_api.registerEnvelopes(Arrays.asList(writeTicket.envelope.getEncryptedEnvelope()), BOOKING_STATE.COMMITED);
				}
				catch (final IOException ioe) {
					commander.printOutln("cannot write, that's bad\n    " + ioe.getMessage());
				}

				if (showCommand)
					commander.printOutln(xrootd.getFormattedLastCommand());

				PFN infoPFN = pTarget;

				if (wasAdded) {
					// try to read back, first without a token (if it was not tested before), then with a token

					if (!openReadTested)
						openReadTest(pTarget, xrootd);

					commander.printOut("  Authenticated read: ");

					final List<PFN> readPFNs = commander.c_api.getPFNsToRead(lfn, Arrays.asList(se.getName()), null);

					if (readPFNs.size() > 0) {
						File tempFile = null;
						try {
							infoPFN = readPFNs.iterator().next();

							tempFile = xrootd.get(infoPFN, null);
							commander.printOutln("file read back ok" + expected);
						}
						catch (final IOException ioe) {
							commander.printOutln("cannot read:\n    " + ioe.getMessage());
						}
						finally {
							if (tempFile != null) {
								TempFileManager.release(tempFile);
								tempFile.delete();
							}
						}

						if (showCommand)
							commander.printOutln(xrootd.getFormattedLastCommand());
					}
				}

				if (wasAdded && !openDeleteTested) {
					// with an existing file, try (if not tested before) to delete it without a token

					if (openDeleteTest(pTarget, xrootd)) {
						wasAdded = false;

						commander.printOutln("  The file is gone, trying to add it back and then try the authenticated delete");

						try {
							xrootd.put(pTarget, referenceFile);

							wasAdded = true;
						}
						catch (final IOException ioe) {
							commander.printOutln("    add operation failed, cannot test authenticated delete");

							if (verbose)
								commander.printOutln("      " + ioe.getMessage());

							if (showCommand)
								commander.printOutln(xrootd.getFormattedLastCommand());
						}
					}
				}

				if (wasAdded) {
					// if the file is (still) on the SE, try to delete it with a proper token
					commander.printOut("  Authenticated delete: ");

					try {
						final PFNforReadOrDel del = Dispatcher.execute(new PFNforReadOrDel(commander.getUser(), null, AccessType.DELETE, lfn, Arrays.asList(se.getName()), null));

						final PFN delPFN = del.getPFNs().iterator().next();

						if (xrootd.delete(delPFN))
							commander.printOutln("delete worked ok" + expected);
					}
					catch (final IOException ioe) {
						commander.printOutln("couldn't delete:\n    " + ioe.getMessage());
					}
					catch (final ServerException e) {
						commander.printOutln("couldn't get a delete token:\n    " + e.getMessage());
					}

					if (showCommand)
						commander.printOutln(xrootd.getFormattedLastCommand());
				}

				commander.printOutln("  Space information:");

				try {
					infoPFN.pfn = se.generateProtocol();
					infoPFN.ticket = null;

					final SpaceInfo info = xrootd.getSpaceInfo(infoPFN);

					commander.printOutln(info.toString());
				}
				catch (final IOException e) {
					commander.printOutln("    Could not get the space information due to: " + e.getMessage());
				}

				commander.printOutln("  LDAP information:");

				commander.printOutln(se.toString());
			}
		}
		finally

		{
			commander.c_api.removeLFN(lfn.getCanonicalName());
		}
	}

	@Override
	public void printHelp() {
		commander.printOutln("Test the functional status of Grid storage elements");
		commander.printOutln("Usage: testSE [options] <some SE names or numbers>");
		commander.printOutln(helpOption("-v", "verbose error messages even when the operation is expected to fail"));
		commander.printOutln(helpOption("-c", "show full command line for each test"));
	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * @param commander
	 * @param alArguments
	 * @throws OptionException
	 */
	public JAliEnCommandtestSE(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		try {
			final OptionParser parser = new OptionParser();

			parser.accepts("v");
			parser.accepts("c");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			verbose = options.has("v");
			showCommand = options.has("c");

			for (final Object o : options.nonOptionArguments()) {
				final String s = o.toString();

				try {
					final int seNumber = Integer.parseInt(s);
					final SE se = SEUtils.getSE(seNumber);

					if (se != null) {
						sesToTest.add(se);
					}
					else {
						commander.printErrln("No such SE: " + seNumber);
						commander.setReturnCode(ErrNo.ENOENT);
						setArgumentsOk(false);
						return;
					}
				}
				catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
					final SE se = SEUtils.getSE(s);

					if (se != null) {
						sesToTest.add(se);
					}
					else {
						commander.printErrln("No such SE: " + s);
						commander.setReturnCode(ErrNo.ENOENT);
						setArgumentsOk(false);
						return;
					}
				}
			}
		}
		catch (final OptionException | IllegalArgumentException e) {
			printHelp();
			throw e;
		}

		if (sesToTest.isEmpty())
			setArgumentsOk(false);
	}
}
